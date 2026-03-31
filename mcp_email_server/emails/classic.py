import asyncio
import email.utils
import json
import mimetypes
import os
import re
import ssl
import time
from collections.abc import AsyncGenerator
from datetime import datetime, timezone, date
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from typing import Any

import aiofiles
import aioimaplib
import aiosmtplib

from mcp_email_server.config import EmailServer, EmailSettings
from mcp_email_server.emails import EmailHandler
from mcp_email_server.emails.models import (
    AttachmentDownloadResponse,
    EmailBodyResponse,
    EmailContentBatchResponse,
    EmailMetadata,
    EmailMetadataPageResponse,
    EmailCountResponse,
    EmailUIDResponse,
    UtilResponse,
)
from mcp_email_server.log import logger

# Maximum body length before truncation (characters)
MAX_BODY_LENGTH = 20000


def _quote_mailbox(mailbox: str) -> str:
    """Quote mailbox name for IMAP compatibility.

    Some IMAP servers (notably Proton Mail Bridge) require mailbox names
    to be quoted. This is valid per RFC 3501 and works with all IMAP servers.

    Per RFC 3501 Section 9 (Formal Syntax), quoted strings must escape
    backslashes and double-quote characters with a preceding backslash.

    See: https://github.com/ai-zerolab/mcp-email-server/issues/87
    See: https://www.rfc-editor.org/rfc/rfc3501#section-9
    """
    # Per RFC 3501, literal double-quote characters in a quoted string must
    # be escaped with a backslash. Backslashes themselves must also be escaped.
    escaped = mailbox.replace("\\", "\\\\").replace('"', r"\"")
    return f'"{escaped}"'


async def _send_imap_id(imap: aioimaplib.IMAP4 | aioimaplib.IMAP4_SSL) -> None:
    """Send IMAP ID command with fallback for strict servers like 163.com.

    aioimaplib's id() method sends ID command with spaces between parentheses
    and content (e.g., 'ID ( "name" "value" )'), which some strict IMAP servers
    like 163.com reject with 'BAD Parse command error'.

    This function first tries the standard id() method, and if it fails,
    falls back to sending a raw command with correct format.

    See: https://github.com/ai-zerolab/mcp-email-server/issues/85
    """
    try:
        response = await imap.id(name="mcp-email-server", version="1.0.0")
        if response.result != "OK":
            # Fallback for strict servers (e.g., 163.com)
            # Send raw command with correct parenthesis format
            await imap.protocol.execute(
                aioimaplib.Command(
                    "ID",
                    imap.protocol.new_tag(),
                    '("name" "mcp-email-server" "version" "1.0.0")',
                )
            )
    except Exception as e:
        logger.warning(f"IMAP ID command failed: {e!s}")


def _create_ssl_context(verify_ssl: bool) -> ssl.SSLContext | None:
    """Create SSL context for SMTP/IMAP connections.

    Returns None for default verification, or permissive context
    for self-signed certificates when verify_ssl=False.
    """
    if verify_ssl:
        return None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


# Backwards-compatible alias
_create_smtp_ssl_context = _create_ssl_context


class EmailClient:
    def __init__(self, email_server: EmailServer, sender: str | None = None):
        self.email_server = email_server
        self.sender = sender or email_server.user_name

        self.imap_class = aioimaplib.IMAP4_SSL if self.email_server.use_ssl else aioimaplib.IMAP4

        self.smtp_use_tls = self.email_server.use_ssl
        self.smtp_start_tls = self.email_server.start_ssl
        self.smtp_verify_ssl = self.email_server.verify_ssl

    def _imap_connect(self) -> aioimaplib.IMAP4_SSL | aioimaplib.IMAP4:
        """Create a new IMAP connection with the configured SSL context."""
        if self.email_server.use_ssl:
            imap_ssl_context = _create_ssl_context(self.email_server.verify_ssl)
            return self.imap_class(self.email_server.host, self.email_server.port, ssl_context=imap_ssl_context)
        return self.imap_class(self.email_server.host, self.email_server.port)

    def _get_smtp_ssl_context(self) -> ssl.SSLContext | None:
        """Get SSL context for SMTP connections based on verify_ssl setting."""
        return _create_ssl_context(self.smtp_verify_ssl)

    @staticmethod
    def _parse_recipients(email_message) -> list[str]:
        """Extract recipient addresses from To and Cc headers."""
        recipients = []
        to_header = email_message.get("To", "")
        if to_header:
            recipients = [addr.strip() for addr in to_header.split(",")]
        cc_header = email_message.get("Cc", "")
        if cc_header:
            recipients.extend([addr.strip() for addr in cc_header.split(",")])
        return recipients

    @staticmethod
    def _parse_date(date_str: str) -> datetime:
        """Parse email date string to datetime, with fallback to current time."""
        try:
            date_tuple = email.utils.parsedate_tz(date_str)
            if date_tuple:
                return datetime.fromtimestamp(email.utils.mktime_tz(date_tuple), tz=timezone.utc)
            return datetime.now(timezone.utc)
        except Exception:
            return datetime.now(timezone.utc)

    def _parse_email_data(
            self,
            raw_email: bytes,
            email_id: str | None = None,
            cache_attachments: bool = False,
            attachment_cache_dir: str | None = "attachments",
    ) -> dict[str, Any]:  # noqa: C901
        """Parse raw email data into a structured dictionary."""
        parser = BytesParser(policy=default)
        email_message = parser.parsebytes(raw_email)

        # Extract email parts
        subject = email_message.get("Subject", "")
        sender = email_message.get("From", "")
        date_str = email_message.get("Date", "")

        # Extract Message-ID for reply threading
        message_id = email_message.get("Message-ID")

        # Extract recipients and parse date
        to_addresses = self._parse_recipients(email_message)
        date = self._parse_date(date_str)

        # Get body content
        body = ""
        html_body = ""  # Fallback if no text/plain
        attachments = []

        def _strip_html(html: str) -> str:
            """Simple HTML to text conversion."""
            import re

            # Remove script and style elements
            text = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html, flags=re.DOTALL | re.IGNORECASE)
            # Convert common block elements to newlines
            text = re.sub(r"<(br|p|div|tr|li)[^>]*/?>", "\n", text, flags=re.IGNORECASE)
            # Remove all remaining HTML tags
            text = re.sub(r"<[^>]+>", "", text)
            # Decode common HTML entities
            text = text.replace("&nbsp;", " ").replace("&amp;", "&")
            text = text.replace("&lt;", "<").replace("&gt;", ">")
            text = text.replace("&quot;", '"').replace("&#39;", "'")
            # Collapse multiple newlines and whitespace
            text = re.sub(r"\n\s*\n", "\n\n", text)
            text = re.sub(r" +", " ", text)
            return text.strip()

        if email_message.is_multipart():
            for part in email_message.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition", ""))

                # Handle attachments
                if "attachment" in content_disposition:
                    filename = part.get_filename()
                    if filename:
                        if cache_attachments:
                            attachment_data = part.get_payload(decode=True)
                            cache_dir = Path(attachment_cache_dir) / email_id
                            cache_file = cache_dir / filename
                            cache_file.parent.mkdir(parents=True, exist_ok=True)
                            cache_file.write_bytes(attachment_data)
                        attachments.append(filename)
                # Handle text parts - prefer text/plain
                elif content_type == "text/plain":
                    body_part = part.get_payload(decode=True)
                    if body_part:
                        charset = part.get_content_charset("utf-8")
                        try:
                            body += body_part.decode(charset)
                        except UnicodeDecodeError:
                            body += body_part.decode("utf-8", errors="replace")
                # Collect HTML as fallback
                elif content_type == "text/html" and not body:
                    html_part = part.get_payload(decode=True)
                    if html_part:
                        charset = part.get_content_charset("utf-8")
                        try:
                            html_body += html_part.decode(charset)
                        except UnicodeDecodeError:
                            html_body += html_part.decode("utf-8", errors="replace")

            # Fall back to HTML if no plain text found
            if not body and html_body:
                body = _strip_html(html_body)
        else:
            # Handle single-part emails
            content_type = email_message.get_content_type()
            payload = email_message.get_payload(decode=True)
            if payload:
                charset = email_message.get_content_charset("utf-8")
                try:
                    text = payload.decode(charset)
                except UnicodeDecodeError:
                    text = payload.decode("utf-8", errors="replace")

                body = _strip_html(text) if content_type == "text/html" else text
        # TODO: Allow retrieving full email body
        if body and len(body) > MAX_BODY_LENGTH:
            body = body[:MAX_BODY_LENGTH] + "...[TRUNCATED]"
        return {
            "email_id": email_id or "",
            "message_id": message_id,
            "subject": subject,
            "from": sender,
            "to": to_addresses,
            "body": body,
            "date": date,
            "attachments": attachments,
        }

    @staticmethod
    def _sanitize_imap_value(value: str) -> str:
        """Sanitize a string value for IMAP search criteria.

        For multi-word values, strips embedded double quotes (invalid per RFC 3501
        Section 4.3) and wraps in double quotes. Single-word values pass through unchanged.
        """
        if " " not in value:
            return value
        sanitized = value.replace('"', "")
        return f'"{sanitized}"'

    @staticmethod
    def validate_pagination_params(page, page_size, total, order):
        """验证分页参数：要么全部提供，要么全部为None"""
        params = [page, page_size, total, order]

        # 检查是否全部为None
        all_none = all(param is None for param in params)

        # 检查是否全部不为None
        all_provided = all(param is not None for param in params)

        if not (all_none or all_provided):
            raise ValueError("分页参数必须全部提供或全部为None")

        if all_provided and (page <= 0 or page_size <= 0 or total <= 0):
            raise ValueError("分页参数必须大于0")

    @staticmethod
    def _build_search_criteria(
            before: datetime | None = None,
            since: datetime | None = None,
            subject: str | None = None,
            body: str | None = None,
            text: str | None = None,
            from_address: str | None = None,
            to_address: str | None = None,
            seen: bool | None = None,
            flagged: bool | None = None,
            answered: bool | None = None,
    ) -> list[str]:
        search_criteria = []
        if before:
            search_criteria.extend(["BEFORE", before.strftime("%d-%b-%Y").upper()])
        if since:
            search_criteria.extend(["SINCE", since.strftime("%d-%b-%Y").upper()])
        if subject:
            search_criteria.extend(["SUBJECT", EmailClient._sanitize_imap_value(subject)])
        if body:
            search_criteria.extend(["BODY", EmailClient._sanitize_imap_value(body)])
        if text:
            search_criteria.extend(["TEXT", EmailClient._sanitize_imap_value(text)])
        if from_address:
            search_criteria.extend(["FROM", EmailClient._sanitize_imap_value(from_address)])
        if to_address:
            search_criteria.extend(["TO", EmailClient._sanitize_imap_value(to_address)])

        # Flag-based criteria using mapping to reduce complexity
        flag_criteria = [
            (seen, {True: "SEEN", False: "UNSEEN"}),
            (flagged, {True: "FLAGGED", False: "UNFLAGGED"}),
            (answered, {True: "ANSWERED", False: "UNANSWERED"}),
        ]
        for flag_value, criteria_map in flag_criteria:
            if flag_value in criteria_map:
                search_criteria.append(criteria_map[flag_value])

        return search_criteria or ["ALL"]

    def _parse_headers(self, email_id: str, raw_headers: bytes) -> dict[str, Any] | None:
        """Parse raw email headers into metadata dictionary."""
        try:
            parser = BytesParser(policy=default)
            email_message = parser.parsebytes(raw_headers)

            subject = email_message.get("Subject", "")
            sender = email_message.get("From", "")
            date_str = email_message.get("Date", "")

            to_addresses = self._parse_recipients(email_message)
            date = self._parse_date(date_str)

            return {
                "email_id": email_id,
                "subject": subject,
                "from": sender,
                "to": to_addresses,
                "date": date,
                "attachments": [],
            }
        except Exception as e:
            logger.error(f"Error parsing email headers: {e!s}")
            return None

    async def _batch_fetch_headers(
            self,
            imap: aioimaplib.IMAP4_SSL | aioimaplib.IMAP4,
            email_ids: list[bytes] | list[str],
    ) -> dict[str, dict[str, Any]]:
        """Batch fetch headers for a list of UIDs."""
        if not email_ids:
            return {}

        # Normalize to list of strings
        str_ids = [uid.decode() if isinstance(uid, bytes) else uid for uid in email_ids]
        uid_list = ",".join(str_ids)
        _, data = await imap.uid("fetch", uid_list, "BODY.PEEK[HEADER]")

        results: dict[str, dict[str, Any]] = {}
        for i, item in enumerate(data):
            if not isinstance(item, bytes) or b"BODY[HEADER]" not in item:
                continue
            # First try to find UID in the same line (standard format)
            uid_match = re.search(rb"UID (\d+)", item)
            if uid_match and i + 1 < len(data) and isinstance(data[i + 1], bytearray):
                uid = uid_match.group(1).decode()
                raw_headers = bytes(data[i + 1])
                metadata = self._parse_headers(uid, raw_headers)
                if metadata:
                    results[uid] = metadata
            # Proton Bridge format: UID comes AFTER header data in a separate item
            # Format: [i]=b'N FETCH (BODY[HEADER] {size}', [i+1]=bytearray(headers), [i+2]=b' UID xxx)'
            elif i + 2 < len(data) and isinstance(data[i + 1], bytearray):
                uid_after_match = re.search(rb"UID (\d+)", data[i + 2]) if isinstance(data[i + 2], bytes) else None
                if uid_after_match:
                    uid = uid_after_match.group(1).decode()
                    raw_headers = bytes(data[i + 1])
                    metadata = self._parse_headers(uid, raw_headers)
                    if metadata:
                        results[uid] = metadata

        return results

    async def get_email_count(
            self,
            before: datetime | None = None,
            since: datetime | None = None,
            subject: str | None = None,
            from_address: str | None = None,
            to_address: str | None = None,
            mailbox: str = "INBOX",
            seen: bool | None = None,
            flagged: bool | None = None,
            answered: bool | None = None,
    ) -> int:
        imap = self._imap_connect()
        try:
            # Wait for the connection to be established
            await imap._client_task
            await imap.wait_hello_from_server()

            # Login and select inbox
            await imap.login(self.email_server.user_name, self.email_server.password)
            await _send_imap_id(imap)
            await imap.select(_quote_mailbox(mailbox))
            search_criteria = self._build_search_criteria(
                before,
                since,
                subject,
                from_address=from_address,
                to_address=to_address,
                seen=seen,
                flagged=flagged,
                answered=answered,
            )
            logger.info(f"Count: Search criteria: {search_criteria}")
            # Search for messages and count them - use UID SEARCH for consistency
            _, messages = await imap.uid_search(*search_criteria)
            return len(messages[0].split())
        finally:
            # Ensure we logout properly
            try:
                await imap.logout()
            except Exception as e:
                logger.info(f"Error during logout: {e}")

    async def get_email_uid(
            self,
            before: datetime | None = None,
            since: datetime | None = None,
            subject: str | None = None,
            from_address: str | None = None,
            to_address: str | None = None,
            mailbox: str = "INBOX",
            seen: bool | None = None,
            flagged: bool | None = None,
            answered: bool | None = None,
    ) -> list[str]:
        imap = self._imap_connect()
        try:
            # Wait for the connection to be established
            await imap._client_task
            await imap.wait_hello_from_server()

            # Login and select inbox
            await imap.login(self.email_server.user_name, self.email_server.password)
            await _send_imap_id(imap)
            await imap.select(_quote_mailbox(mailbox))
            search_criteria = self._build_search_criteria(
                before,
                since,
                subject,
                from_address=from_address,
                to_address=to_address,
                seen=seen,
                flagged=flagged,
                answered=answered,
            )
            logger.info(f"Count: Search criteria: {search_criteria}")
            # Search for messages and count them - use UID SEARCH for consistency
            _, messages = await imap.uid_search(*search_criteria)
            return [str(uid.decode()) for uid in messages[0].split()]
        finally:
            # Ensure we logout properly
            try:
                await imap.logout()
            except Exception as e:
                logger.info(f"Error during logout: {e}")

    async def get_emails_metadata_stream(
            self,
            page: int = 1,
            page_size: int = 10,
            total: int | None = None,
            before: datetime | None = None,
            since: datetime | None = None,
            subject: str | None = None,
            from_address: str | None = None,
            to_address: str | None = None,
            order: str = "desc",
            mailbox: str = "INBOX",
            seen: bool | None = None,
            flagged: bool | None = None,
            answered: bool | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        imap = self._imap_connect()
        try:
            # Wait for the connection to be established
            await imap._client_task
            await imap.wait_hello_from_server()

            # Login and select mailbox
            await imap.login(self.email_server.user_name, self.email_server.password)
            await _send_imap_id(imap)
            await imap.select(_quote_mailbox(mailbox))

            search_criteria = self._build_search_criteria(
                before,
                since,
                subject,
                from_address=from_address,
                to_address=to_address,
                seen=seen,
                flagged=flagged,
                answered=answered,
            )
            logger.info(f"Get metadata: Search criteria: {search_criteria}")

            # Search for messages - use UID SEARCH for better compatibility
            _, messages = await imap.uid_search(*search_criteria)

            # Handle empty or None responses
            if not messages or not messages[0]:
                logger.warning("No messages returned from search")
                return

            email_ids = messages[0].split()
            logger.info(f"Found {len(email_ids)} email IDs")

            sorted_uids = sorted([int(uid.decode()) for uid in email_ids], reverse=(order == "desc"))

            self.validate_pagination_params(page, page_size, total, order)
            min_uid = (page - 1) * page_size
            max_uid = min_uid + page_size
            page_uids = [str(x) for x in sorted_uids[min_uid:max_uid]]

            # Phase 2: Batch fetch headers for requested page only
            fetch_headers_start = time.perf_counter()
            metadata_by_uid = await self._batch_fetch_headers(imap, page_uids)
            await asyncio.sleep(0.2)
            fetch_headers_elapsed = time.perf_counter() - fetch_headers_start

            logger.info(
                f"{fetch_headers_elapsed:.2f}s headers ({len(page_uids)} UIDs)"
            )

            # Yield in sorted order
            for uid in page_uids:
                if uid in metadata_by_uid:
                    yield metadata_by_uid[uid]
        finally:
            try:
                # 尝试发送 NOOP，可能刷新未决响应
                await imap.noop()
                await asyncio.sleep(0.2)
                await imap.logout()
            except Exception as e:
                logger.info(f"Error during logout: {e}")
                # 如果仍有协议异常，直接关闭传输层
                if hasattr(imap, 'transport') and imap.transport and not imap.transport.is_closing():
                    imap.transport.close()

    def _check_email_content(self, data: list) -> bool:
        """Check if the fetched data contains actual email content."""
        for item in data:
            if isinstance(item, bytes) and b"FETCH (" in item and b"RFC822" not in item and b"BODY" not in item:
                # This is just metadata, not actual content
                continue
            elif isinstance(item, bytes | bytearray) and len(item) > 100:
                # This looks like email content
                return True
        return False

    def _extract_raw_email(self, data: list) -> bytes | None:
        """Extract raw email bytes from IMAP response data."""
        # The email content is typically at index 1 as a bytearray
        if len(data) > 1 and isinstance(data[1], bytearray):
            return bytes(data[1])

        # Search through all items for email content
        for item in data:
            if isinstance(item, bytes | bytearray) and len(item) > 100:
                # Skip IMAP protocol responses
                if isinstance(item, bytes) and b"FETCH" in item:
                    continue
                # This is likely the email content
                return bytes(item) if isinstance(item, bytearray) else item
        return None

    async def _fetch_email_with_formats(self, imap, email_id: str) -> list | None:
        """Try different fetch formats to get email data."""
        fetch_formats = ["RFC822", "BODY[]", "BODY.PEEK[]", "(BODY.PEEK[])"]

        for fetch_format in fetch_formats:
            try:
                _, data = await imap.uid("fetch", email_id, fetch_format)

                if data and len(data) > 0 and self._check_email_content(data):
                    return data

            except Exception as e:
                logger.debug(f"Fetch format {fetch_format} failed: {e}")

        return None

    async def get_emails_body_by_id(
            self,
            email_ids: list[str],
            mailbox: str = "INBOX",
            cache_attachments: bool = False,
            attachment_cache_dir: str | None = "attachments",
    ) -> dict[str, Any] | None:
        imap = self._imap_connect()
        try:
            # Wait for the connection to be established
            await imap._client_task
            await imap.wait_hello_from_server()

            # Login and select inbox
            await imap.login(self.email_server.user_name, self.email_server.password)
            await _send_imap_id(imap)
            await imap.select(_quote_mailbox(mailbox))

            emails_content = []
            failed_ids = []

            for email_id in email_ids:
                # Fetch the specific email by UID
                data = await self._fetch_email_with_formats(imap, email_id)
                if not data:
                    logger.error(f"Failed to fetch UID {email_id} with any format")
                    failed_ids.append(email_id)

                # Extract raw email data
                raw_email = self._extract_raw_email(data)
                if not raw_email:
                    logger.error(f"Could not find email data in response for email ID: {email_id}")
                    failed_ids.append(email_id)

                # Parse the email
                try:
                    email_data = self._parse_email_data(raw_email, email_id, cache_attachments, attachment_cache_dir)

                    emails_content.append(
                        EmailBodyResponse(
                            email_id=email_data["email_id"],
                            message_id=email_data.get("message_id"),
                            subject=email_data["subject"],
                            sender=email_data["from"],
                            recipients=email_data["to"],
                            date=email_data["date"],
                            body=email_data["body"],
                            attachments=email_data["attachments"],
                        )
                    )
                except Exception as e:
                    logger.error(f"Error parsing email: {e!s}")
                    failed_ids.append(email_id)

            return {
                "emails_content": emails_content,
                "failed_ids": failed_ids,
            }

        finally:
            # Ensure we logout properly
            try:
                await imap.logout()
            except Exception as e:
                logger.info(f"Error during logout: {e}")

    async def download_attachment(
            self,
            email_id: str,
            attachment_name: str,
            save_path: str,
            mailbox: str = "INBOX",
    ) -> dict[str, Any]:
        """Download a specific attachment from an email and save it to disk.

        Args:
            email_id: The UID of the email containing the attachment.
            attachment_name: The filename of the attachment to download.
            save_path: The local path where the attachment will be saved.
            mailbox: The mailbox to search in (default: "INBOX").

        Returns:
            A dictionary with download result information.
        """
        imap = self._imap_connect()
        try:
            await imap._client_task
            await imap.wait_hello_from_server()

            await imap.login(self.email_server.user_name, self.email_server.password)
            await _send_imap_id(imap)
            await imap.select(_quote_mailbox(mailbox))

            data = await self._fetch_email_with_formats(imap, email_id)
            if not data:
                msg = f"Failed to fetch email with UID {email_id}"
                logger.error(msg)
                raise ValueError(msg)

            raw_email = self._extract_raw_email(data)
            if not raw_email:
                msg = f"Could not find email data for email ID: {email_id}"
                logger.error(msg)
                raise ValueError(msg)

            parser = BytesParser(policy=default)
            email_message = parser.parsebytes(raw_email)

            # Find the attachment
            attachment_data = None
            mime_type = None

            if email_message.is_multipart():
                for part in email_message.walk():
                    content_disposition = str(part.get("Content-Disposition", ""))
                    if "attachment" in content_disposition:
                        filename = part.get_filename()
                        if filename == attachment_name:
                            attachment_data = part.get_payload(decode=True)
                            mime_type = part.get_content_type()
                            break

            if attachment_data is None:
                msg = f"Attachment '{attachment_name}' not found in email {email_id}"
                logger.error(msg)
                raise ValueError(msg)

            # Save to disk
            save_file = Path(save_path)
            save_file.parent.mkdir(parents=True, exist_ok=True)
            save_file.write_bytes(attachment_data)

            logger.info(f"Attachment '{attachment_name}' saved to {save_path}")

            return {
                "email_id": email_id,
                "attachment_name": attachment_name,
                "mime_type": mime_type or "application/octet-stream",
                "size": len(attachment_data),
                "saved_path": str(save_file.resolve()),
            }

        finally:
            try:
                await imap.logout()
            except Exception as e:
                logger.info(f"Error during logout: {e}")

    def _validate_attachment(self, file_path: str) -> Path:
        """Validate attachment file path."""
        path = Path(file_path)
        if not path.exists():
            msg = f"Attachment file not found: {file_path}"
            logger.error(msg)
            raise FileNotFoundError(msg)

        if not path.is_file():
            msg = f"Attachment path is not a file: {file_path}"
            logger.error(msg)
            raise ValueError(msg)

        return path

    def _create_attachment_part(self, path: Path) -> MIMEApplication:
        """Create MIME attachment part from file."""
        with open(path, "rb") as f:
            file_data = f.read()

        mime_type, _ = mimetypes.guess_type(str(path))
        if mime_type is None:
            mime_type = "application/octet-stream"

        attachment_part = MIMEApplication(file_data, _subtype=mime_type.split("/")[1])
        attachment_part.add_header(
            "Content-Disposition",
            "attachment",
            filename=path.name,
        )
        logger.info(f"Attached file: {path.name} ({mime_type})")
        return attachment_part

    def _create_message_with_attachments(self, body: str, html: bool, attachments: list[str]) -> MIMEMultipart:
        """Create multipart message with attachments."""
        msg = MIMEMultipart()
        content_type = "html" if html else "plain"
        text_part = MIMEText(body, content_type, "utf-8")
        msg.attach(text_part)

        for file_path in attachments:
            try:
                path = self._validate_attachment(file_path)
                attachment_part = self._create_attachment_part(path)
                msg.attach(attachment_part)
            except Exception as e:
                logger.error(f"Failed to attach file {file_path}: {e}")
                raise

        return msg

    async def send_email(
            self,
            recipients: list[str],
            subject: str,
            body: str,
            cc: list[str] | None = None,
            bcc: list[str] | None = None,
            html: bool = False,
            attachments: list[str] | None = None,
            in_reply_to: str | None = None,
            references: str | None = None,
    ):
        # Create message with or without attachments
        if attachments:
            msg = self._create_message_with_attachments(body, html, attachments)
        else:
            content_type = "html" if html else "plain"
            msg = MIMEText(body, content_type, "utf-8")

        # Handle subject with special characters
        if any(ord(c) > 127 for c in subject):
            msg["Subject"] = Header(subject, "utf-8")
        else:
            msg["Subject"] = subject

        # Handle sender name with special characters
        if any(ord(c) > 127 for c in self.sender):
            msg["From"] = Header(self.sender, "utf-8")
        else:
            msg["From"] = self.sender

        msg["To"] = ", ".join(recipients)

        # Add CC header if provided (visible to recipients)
        if cc:
            msg["Cc"] = ", ".join(cc)

        # Set threading headers for replies
        if in_reply_to:
            msg["In-Reply-To"] = in_reply_to
        if references:
            msg["References"] = references

        # Set Date and Message-Id headers so the same values appear in both
        # the SMTP-sent copy and the IMAP Sent folder copy
        msg["Date"] = email.utils.formatdate(localtime=True)
        sender_domain = self.sender.rsplit("@", 1)[-1].rstrip(">")
        msg["Message-Id"] = email.utils.make_msgid(domain=sender_domain)

        # Note: BCC recipients are not added to headers (they remain hidden)
        # but will be included in the actual recipients for SMTP delivery

        async with aiosmtplib.SMTP(
                hostname=self.email_server.host,
                port=self.email_server.port,
                start_tls=self.smtp_start_tls,
                use_tls=self.smtp_use_tls,
                tls_context=self._get_smtp_ssl_context(),
        ) as smtp:
            await smtp.login(self.email_server.user_name, self.email_server.password)

            # Create a combined list of all recipients for delivery
            all_recipients = recipients.copy()
            if cc:
                all_recipients.extend(cc)
            if bcc:
                all_recipients.extend(bcc)

            await smtp.send_message(msg, recipients=all_recipients)

        # Return the message for potential saving to Sent folder
        return msg

    async def _find_sent_folder_by_flag(self, imap) -> str | None:
        """Find the Sent folder by searching for the \\Sent IMAP flag.

        Args:
            imap: Connected IMAP client

        Returns:
            The folder name with the \\Sent flag, or None if not found
        """
        try:
            # List all folders - aioimaplib requires reference_name and mailbox_pattern
            _, folders = await imap.list('""', "*")

            # Search for folder with \Sent flag
            for folder in folders:
                folder_str = folder.decode("utf-8") if isinstance(folder, bytes) else str(folder)
                # IMAP LIST response format: (flags) "delimiter" "name"
                # Example: (\Sent \HasNoChildren) "/" "Gesendete Objekte"
                if r"\Sent" in folder_str or "\\Sent" in folder_str:
                    # Extract folder name from the response
                    # Split by quotes and get the last quoted part
                    parts = folder_str.split('"')
                    if len(parts) >= 3:
                        folder_name = parts[-2]  # The folder name is the second-to-last quoted part
                        logger.info(f"Found Sent folder by \\Sent flag: '{folder_name}'")
                        return folder_name
        except Exception as e:
            logger.debug(f"Error finding Sent folder by flag: {e}")

        return None

    async def append_to_sent(
            self,
            msg: MIMEText | MIMEMultipart,
            incoming_server: EmailServer,
            sent_folder_name: str | None = None,
    ) -> bool:
        """Append a sent message to the IMAP Sent folder.

        Args:
            msg: The email message that was sent
            incoming_server: IMAP server configuration for accessing Sent folder
            sent_folder_name: Override folder name, or None for auto-detection

        Returns:
            True if successfully saved, False otherwise
        """
        if incoming_server.use_ssl:
            imap_ssl_context = _create_ssl_context(incoming_server.verify_ssl)
            imap = aioimaplib.IMAP4_SSL(incoming_server.host, incoming_server.port, ssl_context=imap_ssl_context)
        else:
            imap = aioimaplib.IMAP4(incoming_server.host, incoming_server.port)

        # Common Sent folder names across different providers
        sent_folder_candidates = [
            sent_folder_name,  # User-specified override (if provided)
            "Sent",
            "INBOX.Sent",
            "Sent Items",
            "Sent Mail",
            "[Gmail]/Sent Mail",
            "INBOX/Sent",
        ]
        # Filter out None values
        sent_folder_candidates = [f for f in sent_folder_candidates if f]

        try:
            await imap._client_task
            await imap.wait_hello_from_server()
            await imap.login(incoming_server.user_name, incoming_server.password)
            await _send_imap_id(imap)

            # Try to find Sent folder by IMAP \Sent flag first
            flag_folder = await self._find_sent_folder_by_flag(imap)
            if flag_folder and flag_folder not in sent_folder_candidates:
                # Add it at the beginning (high priority)
                sent_folder_candidates.insert(0, flag_folder)

            # Try to find and use the Sent folder
            for folder in sent_folder_candidates:
                try:
                    logger.debug(f"Trying Sent folder: '{folder}'")
                    # Try to select the folder to verify it exists
                    result = await imap.select(_quote_mailbox(folder))
                    logger.debug(f"Select result for '{folder}': {result}")

                    # aioimaplib returns (status, data) where status is a string like 'OK' or 'NO'
                    status = result[0] if isinstance(result, tuple) else result
                    if str(status).upper() == "OK":
                        # Folder exists, append the message
                        msg_bytes = msg.as_bytes()
                        logger.debug(f"Appending message to '{folder}'")
                        # aioimaplib.append signature: (message_bytes, mailbox, flags, date)
                        append_result = await imap.append(
                            msg_bytes,
                            mailbox=_quote_mailbox(folder),
                            flags=r"(\Seen)",
                        )
                        logger.debug(f"Append result: {append_result}")
                        append_status = append_result[0] if isinstance(append_result, tuple) else append_result
                        if str(append_status).upper() == "OK":
                            logger.info(f"Saved sent email to '{folder}'")
                            return True
                        else:
                            logger.warning(f"Failed to append to '{folder}': {append_status}")
                    else:
                        logger.debug(f"Folder '{folder}' select returned: {status}")
                except Exception as e:
                    logger.debug(f"Folder '{folder}' not available: {e}")
                    continue

            logger.warning("Could not find a valid Sent folder to save the message")
            return False

        except Exception as e:
            logger.error(f"Error saving to Sent folder: {e}")
            return False
        finally:
            try:
                await imap.logout()
            except Exception as e:
                logger.debug(f"Error during logout: {e}")

    async def delete_emails(self, email_ids: list[str], mailbox: str = "INBOX") -> tuple[list[str], list[str]]:
        """Delete emails by their UIDs. Returns (deleted_ids, failed_ids)."""
        imap = self._imap_connect()
        deleted_ids = []
        failed_ids = []

        try:
            await imap._client_task
            await imap.wait_hello_from_server()
            await imap.login(self.email_server.user_name, self.email_server.password)
            await _send_imap_id(imap)
            await imap.select(_quote_mailbox(mailbox))

            for email_id in email_ids:
                try:
                    await imap.uid("store", email_id, "+FLAGS", r"(\Deleted)")
                    deleted_ids.append(email_id)
                except Exception as e:
                    logger.error(f"Failed to delete email {email_id}: {e}")
                    failed_ids.append(email_id)

            await imap.expunge()
        finally:
            try:
                await imap.logout()
            except Exception as e:
                logger.info(f"Error during logout: {e}")

        return deleted_ids, failed_ids


class ClassicEmailHandler(EmailHandler):
    def __init__(self, email_settings: EmailSettings):
        self.email_settings = email_settings
        self.incoming_client = EmailClient(email_settings.incoming)
        self.outgoing_client = EmailClient(
            email_settings.outgoing,
            sender=f"{email_settings.full_name} <{email_settings.email_address}>",
        )
        self.save_to_sent = email_settings.save_to_sent
        self.sent_folder_name = email_settings.sent_folder_name

    @staticmethod
    async def _record_failed_uids(uids, reason="", filename='failed_emails.json'):
        """
        记录失败的 UID 到文件，便于后续重试。
        :param uids: list of str or int 失败的 UID 列表
        :param reason: 失败原因
        :param filename: 记录文件
        """
        if not uids:
            return

        import json
        import aiofiles
        from datetime import datetime

        # 读取已有失败记录（如果有）
        existing_failed = {}
        if os.path.exists(filename):
            try:
                async with aiofiles.open(filename, 'r', encoding='utf-8') as f:
                    content = await f.read()
                    if content.strip():
                        existing_failed = json.loads(content)
            except (json.JSONDecodeError, IOError):
                existing_failed = {}

        # 记录新失败的 UID，避免重复
        timestamp = datetime.now().isoformat()
        for uid in uids:
            uid_str = str(uid)
            if uid_str not in existing_failed:
                existing_failed[uid_str] = {
                    "uid": uid_str,
                    "first_failed_at": timestamp,
                    "last_failed_at": timestamp,
                    "fail_count": 1,
                    "last_reason": reason
                }
            else:
                # 更新已有记录
                existing_failed[uid_str]["last_failed_at"] = timestamp
                existing_failed[uid_str]["fail_count"] += 1
                existing_failed[uid_str]["last_reason"] = reason

        # 写回文件
        async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(existing_failed, ensure_ascii=False, indent=2))

    async def get_emails_count(
            self,
            before: datetime | None = None,
            since: datetime | None = None,
            subject: str | None = None,
            from_address: str | None = None,
            to_address: str | None = None,
            mailbox: str = "INBOX",
            seen: bool | None = None,
            flagged: bool | None = None,
            answered: bool | None = None
    ) -> EmailCountResponse:
        total = await self.incoming_client.get_email_count(
            before,
            since,
            subject,
            from_address=from_address,
            to_address=to_address,
            mailbox=mailbox,
            seen=seen,
            flagged=flagged,
            answered=answered,
        )

        return EmailCountResponse(
            email_count=total
        )

    async def get_emails_uid(
            self,
            before: datetime | None = None,
            since: datetime | None = None,
            subject: str | None = None,
            from_address: str | None = None,
            to_address: str | None = None,
            mailbox: str = "INBOX",
            seen: bool | None = None,
            flagged: bool | None = None,
            answered: bool | None = None
    ) -> EmailUIDResponse:
        uid_list = await self.incoming_client.get_email_uid(
            before,
            since,
            subject,
            from_address=from_address,
            to_address=to_address,
            mailbox=mailbox,
            seen=seen,
            flagged=flagged,
            answered=answered,
        )

        return EmailUIDResponse(
            email_uid_list=uid_list
        )

    async def get_emails_metadata(
            self,
            page: int = 1,
            page_size: int = 10,
            before: datetime | None = None,
            since: datetime | None = None,
            subject: str | None = None,
            from_address: str | None = None,
            to_address: str | None = None,
            order: str = "desc",
            mailbox: str = "INBOX",
            seen: bool | None = None,
            flagged: bool | None = None,
            answered: bool | None = None,
    ) -> EmailMetadataPageResponse:
        emails = []

        total = await self.incoming_client.get_email_count(
            before,
            since,
            subject,
            from_address=from_address,
            to_address=to_address,
            mailbox=mailbox,
            seen=seen,
            flagged=flagged,
            answered=answered,
        )

        async for email_data in self.incoming_client.get_emails_metadata_stream(
                page,
                page_size,
                total,
                before,
                since,
                subject,
                from_address,
                to_address,
                order,
                mailbox,
                seen,
                flagged,
                answered,
        ):
            emails.append(EmailMetadata.from_email(email_data))

        return EmailMetadataPageResponse(
            page=page,
            page_size=page_size,
            before=before,
            since=since,
            subject=subject,
            emails=emails,
            total=total,
        )

    async def get_emails_content(
            self,
            email_ids: list[str],
            mailbox: str = "INBOX",
            use_cache: bool = True,
            update_cache: bool = True,
            cache_file: str = 'emails.json',
            cache_attachments: bool = False,
            attachment_cache_dir: str | None = "attachments",
    ) -> EmailContentBatchResponse:
        emails_list = []
        failed_ids = []
        missing_ids = []

        # 如果启用缓存，读取本地缓存文件
        if use_cache:
            existing_cache = {}
            if os.path.exists(cache_file):
                try:
                    async with aiofiles.open(cache_file, 'r', encoding='utf-8') as f:
                        content = await f.read()
                        if content.strip():
                            existing_cache = json.loads(content)
                except (json.JSONDecodeError, IOError) as e:
                    logger.warning(f"Failed to read cache file {cache_file}: {e}, ignoring cache for this request.")
                    existing_cache = {}

            # 分类：已缓存的直接恢复对象，缺失的标记
            for uid in email_ids:
                if uid in existing_cache:
                    try:
                        # 将缓存的字典还原为 EmailBodyResponse 对象
                        email_obj = EmailBodyResponse.model_validate(existing_cache[uid])
                        emails_list.append(email_obj)
                    except Exception as e:
                        logger.error(f"Failed to parse cached email {uid}: {e}, will fetch again")
                        missing_ids.append(uid)
                else:
                    missing_ids.append(uid)
        else:
            missing_ids = email_ids[:]

        # 处理缺失的 UID：从 IMAP 服务器获取
        new_emails = []
        if missing_ids:
            try:
                result = await self.incoming_client.get_emails_body_by_id(missing_ids, mailbox, cache_attachments)
                # 预期 result 结构：{'emails_content': [...], 'failed_ids': [...]}
                emails_content = result.get('emails_content', [])
                fetched_failed = result.get('failed_ids', [])

                # 成功获取的邮件
                for email_obj in emails_content:
                    emails_list.append(email_obj)
                    new_emails.append(email_obj)

                failed_ids.extend([str(uid) for uid in fetched_failed])  # 记录获取失败的 UID

            except Exception as e:
                logger.error(f"Failed to fetch emails from IMAP: {e}")
                # 整个缺失列表标记为失败
                failed_ids.extend([str(uid) for uid in missing_ids])

        # 如果启用了缓存更新且成功获取了新邮件，写入缓存
        if update_cache and new_emails:
            try:
                await self._save_emails_chunk(new_emails, filename=cache_file)
            except Exception as e:
                logger.error(f"Failed to update cache with new emails: {e}")

        return EmailContentBatchResponse(
            emails=emails_list,
            requested_count=len(email_ids),
            retrieved_count=len(emails_list),
            failed_ids=failed_ids
        )

    async def send_email(
            self,
            recipients: list[str],
            subject: str,
            body: str,
            cc: list[str] | None = None,
            bcc: list[str] | None = None,
            html: bool = False,
            attachments: list[str] | None = None,
            in_reply_to: str | None = None,
            references: str | None = None,
    ) -> None:
        msg = await self.outgoing_client.send_email(
            recipients, subject, body, cc, bcc, html, attachments, in_reply_to, references
        )

        # Save to Sent folder if enabled
        if self.save_to_sent and msg:
            try:
                await self.outgoing_client.append_to_sent(
                    msg,
                    self.email_settings.incoming,
                    self.sent_folder_name,
                )
            except Exception as e:
                logger.error(f"Failed to save email to Sent folder: {e}", exc_info=True)

    async def delete_emails(self, email_ids: list[str], mailbox: str = "INBOX") -> tuple[list[str], list[str]]:
        """Delete emails by their UIDs. Returns (deleted_ids, failed_ids)."""
        return await self.incoming_client.delete_emails(email_ids, mailbox)

    async def download_attachment(
            self,
            email_id: str,
            attachment_name: str,
            save_path: str,
            mailbox: str = "INBOX",
    ) -> AttachmentDownloadResponse:
        """Download an email attachment and save it to the specified path.

        Args:
            email_id: The UID of the email containing the attachment.
            attachment_name: The filename of the attachment to download.
            save_path: The local path where the attachment will be saved.
            mailbox: The mailbox to search in (default: "INBOX").

        Returns:
            AttachmentDownloadResponse with download result information.
        """
        result = await self.incoming_client.download_attachment(email_id, attachment_name, save_path, mailbox)
        return AttachmentDownloadResponse(
            email_id=result["email_id"],
            attachment_name=result["attachment_name"],
            mime_type=result["mime_type"],
            size=result["size"],
            saved_path=result["saved_path"],
        )

    async def cache_emails(
            self,
            mailbox: str = "INBOX",
            cache_attachments: bool = True,
            attachment_cache_dir: str | None = "attachments",
    ) -> UtilResponse:
        def chunk_list(lst, chunk_size):
            return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

        try:
            # 获取所有邮件 UID
            uids = await self.get_emails_uid()
            all_uid_list = uids.email_uid_list

            # 检查已有缓存文件，提取已缓存的 UID 集合
            cache_file = 'emails.json'
            existing_uids = set()
            if os.path.exists(cache_file):
                try:
                    async with aiofiles.open(cache_file, 'r', encoding='utf-8') as f:
                        content = await f.read()
                        if content.strip():
                            existing_data = json.loads(content)
                            # JSON 的键是字符串形式的 UID
                            existing_uids = set(existing_data.keys())
                except (json.JSONDecodeError, IOError) as e:
                    logger.warning(f"Failed to read existing cache file: {e}, will re-cache all emails.")
                    existing_uids = set()  # 读取失败则全量缓存

            # 过滤出未缓存的 UID
            new_uid_list = [uid for uid in all_uid_list if str(uid) not in existing_uids]
            if not new_uid_list:
                logger.info("All emails are already cached.")
                return UtilResponse(message="所有邮件均已缓存", success=True)

            # 分块处理未缓存的 UID
            email_ids_chunks = chunk_list(new_uid_list, 1000)
            total_chunks = len(email_ids_chunks)
            for index, email_ids_chunk in enumerate(email_ids_chunks):
                logger.info(f"Processing chunk {index + 1}/{total_chunks} ({len(email_ids_chunk)} emails)")
                try:
                    # 获取该批次的邮件内容
                    emails_response = await self.get_emails_content(
                        email_ids=email_ids_chunk,
                        mailbox=mailbox,
                        cache_attachments=cache_attachments,
                        attachment_cache_dir=attachment_cache_dir
                    )
                    if emails_response is None or not hasattr(emails_response, 'emails'):
                        logger.warning(f"Chunk {index + 1} returned invalid response, all emails in this chunk failed")
                        # 记录整批失败
                        await self._record_failed_uids(email_ids_chunk, "get_emails_content returned None/invalid")
                        continue

                    email_list = emails_response.emails
                    if not email_list:
                        logger.info(f"Chunk {index + 1} has no email data, all emails failed")
                        await self._record_failed_uids(email_ids_chunk, "no email data in response")
                        continue

                    # 成功获取的 UID（从邮件对象中提取）
                    success_uids = set()
                    for email_obj in email_list:
                        if hasattr(email_obj, 'email_id'):
                            success_uids.add(str(email_obj.email_id))
                        elif isinstance(email_obj, dict) and 'email_id' in email_obj:
                            success_uids.add(str(email_obj['email_id']))

                    # 计算失败的 UID
                    failed_uids = [str(uid) for uid in email_ids_chunk if str(uid) not in success_uids]

                    # 保存成功的邮件
                    if email_list:
                        await self._save_emails_chunk(email_list)

                    # 记录失败的 UID
                    if failed_uids:
                        await self._record_failed_uids(failed_uids, "email content missing in response")

                except Exception as chunk_error:
                    logger.error(f"Failed to process chunk {index + 1}: {chunk_error}", exc_info=True)
                    # 整批记录失败
                    await self._record_failed_uids(email_ids_chunk, f"exception: {str(chunk_error)}")
                    continue

        except Exception as e:
            logger.error(f"Failed to cache emails: {e}", exc_info=True)
            return UtilResponse(
                message=f"缓存失败，错误信息为：{str(e)}",
                success=False
            )

        return UtilResponse(
            message="缓存成功",
            success=True
        )

    @staticmethod
    async def _save_emails_chunk(emails_chunk, filename='emails.json'):
        """
        将一批邮件保存到 JSON 文件，保证 uid 不重复。
        :param emails_chunk: list[dict] 每个字典至少包含 'uid' 键
        :param filename: 保存的文件名
        """

        class DateTimeEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                if isinstance(obj, date):
                    return obj.isoformat()
                return super().default(obj)

        # 读取已有数据
        existing_data = {}
        if os.path.exists(filename):
            async with aiofiles.open(filename, 'r', encoding='utf-8') as f:
                content = await f.read()
                if content.strip():
                    try:
                        existing_data = json.loads(content)
                    except json.JSONDecodeError:
                        existing_data = {}

        # 合并新数据，只添加 uid 不存在的邮件
        for _email in emails_chunk:
            if hasattr(_email, 'model_dump'):  # Pydantic v2
                email_dict = _email.model_dump()
            elif hasattr(_email, 'dict'):  # Pydantic v1
                email_dict = _email.dict()
            else:
                # 如果是普通字典，直接使用
                email_dict = _email if isinstance(_email, dict) else vars(_email)

            uid = email_dict.get("email_id")
            if uid and uid not in existing_data:
                existing_data[uid] = email_dict

        # 写回文件
        async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(existing_data, ensure_ascii=False, indent=2, cls=DateTimeEncoder))



