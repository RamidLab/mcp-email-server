import asyncio
import base64
import email.utils
import json
import mimetypes
import os
import re
import socket
import ssl
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime, timezone, date
from email.header import Header, decode_header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from typing import Any, TypeVar, Callable

import aiofiles
import aioimaplib
import aiosmtplib
import aiosqlite
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

MAX_BODY_LENGTH = 20000


# ----------------------------------------------------------------------
# 通用辅助函数
# ----------------------------------------------------------------------

def _quote_mailbox(mailbox: str) -> str:
    """为 IMAP 邮箱名添加引号，兼容 Proton Mail Bridge 等严格服务器。"""
    escaped = mailbox.replace("\\", "\\\\").replace('"', r"\"")
    return f'"{escaped}"'


async def _send_imap_id(imap: aioimaplib.IMAP4 | aioimaplib.IMAP4_SSL) -> None:
    """发送 IMAP ID 命令，针对 163.com 等严格服务器提供降级方案。"""
    try:
        response = await imap.id(name="mcp-email-server", version="1.0.0")
        if response.result != "OK":
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
    """创建 SSL 上下文，verify_ssl=False 时接受自签名证书。"""
    if verify_ssl:
        ctx = ssl.create_default_context()
    else:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        # 设置 ALPN 协议（部分服务器需要）
    ctx.set_alpn_protocols(["imap"])
    return ctx


T = TypeVar('T')


# ----------------------------------------------------------------------
# EmailClient - 底层邮件协议客户端
# ----------------------------------------------------------------------

class EmailClient:
    def __init__(self, email_server: EmailServer, sender: str | None = None):
        self.email_server = email_server
        self.sender = sender or email_server.user_name
        self.imap_class = aioimaplib.IMAP4_SSL if self.email_server.use_ssl else aioimaplib.IMAP4
        self.smtp_use_tls = self.email_server.use_ssl
        self.smtp_start_tls = self.email_server.start_ssl
        self.smtp_verify_ssl = self.email_server.verify_ssl

    def _imap_connect(self) -> aioimaplib.IMAP4_SSL | aioimaplib.IMAP4:
        """创建原始 IMAP 连接对象（未登录）。"""
        if self.email_server.use_ssl:
            ctx = _create_ssl_context(self.email_server.verify_ssl)
            conn = self.imap_class(self.email_server.host, self.email_server.port, ssl_context=ctx)
        else:
            conn = self.imap_class(self.email_server.host, self.email_server.port)

        try:
            if hasattr(conn, 'protocol') and conn.protocol:
                transport = conn.protocol.transport
                if transport:
                    sock = transport.get_extra_info('socket')
                    if sock:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                        if hasattr(socket, 'TCP_KEEPIDLE'):
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        except Exception as e:
            logger.debug(f"Failed to set TCP keepalive: {e}")
        return conn

    @asynccontextmanager
    async def _with_imap(self, mailbox: str = "INBOX"):
        """
        IMAP 操作统一上下文管理器。
        自动处理：连接等待 → 登录 → 发送 ID → 选择邮箱 → 返回客户端 → 最后登出/关闭。
        """
        imap = self._imap_connect()
        try:
            await asyncio.wait_for(imap.wait_hello_from_server(), timeout=10.0)
            await asyncio.wait_for(imap.login(self.email_server.user_name, self.email_server.password), timeout=10.0)
            await _send_imap_id(imap)
            await asyncio.wait_for(imap.select(_quote_mailbox(mailbox)), timeout=10.0)
            yield imap
        finally:
            # 安全关闭：只尝试 logout，不强制操作底层 transport
            try:
                await asyncio.wait_for(imap.logout(), timeout=5.0)
            except Exception:
                # 如果 logout 失败，尝试直接关闭协议
                try:
                    if hasattr(imap, 'protocol') and imap.protocol:
                        imap.protocol.connection_lost(None)
                        if hasattr(imap.protocol, 'transport') and imap.protocol.transport:
                            imap.protocol.transport.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # 内部搜索与数据处理
    # ------------------------------------------------------------------

    @staticmethod
    def _sanitize_imap_value(value: str) -> str:
        """清洗搜索值，多词时加双引号并移除内部引号。"""
        if " " not in value:
            return value
        return f'"{value.replace(chr(34), "")}"'

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
        """根据条件构建 IMAP 搜索词列表。"""
        criteria = []
        if before:
            criteria.extend(["BEFORE", before.strftime("%d-%b-%Y").upper()])
        if since:
            criteria.extend(["SINCE", since.strftime("%d-%b-%Y").upper()])
        if subject:
            criteria.extend(["SUBJECT", EmailClient._sanitize_imap_value(subject)])
        if body:
            criteria.extend(["BODY", EmailClient._sanitize_imap_value(body)])
        if text:
            criteria.extend(["TEXT", EmailClient._sanitize_imap_value(text)])
        if from_address:
            criteria.extend(["FROM", EmailClient._sanitize_imap_value(from_address)])
        if to_address:
            criteria.extend(["TO", EmailClient._sanitize_imap_value(to_address)])

        # 标志搜索
        if seen:
            criteria.append("SEEN")
        elif seen is False:
            criteria.append("UNSEEN")
        if flagged:
            criteria.append("FLAGGED")
        elif flagged is False:
            criteria.append("UNFLAGGED")
        if answered:
            criteria.append("ANSWERED")
        elif answered is False:
            criteria.append("UNANSWERED")

        return criteria or ["ALL"]

    async def _search_uids(
            self,
            imap: aioimaplib.IMAP4_SSL | aioimaplib.IMAP4,
            **kwargs,
    ) -> list[str]:
        """执行 UID SEARCH 并返回 UID 字符串列表。"""
        criteria = self._build_search_criteria(**kwargs)
        _, messages = await imap.uid_search(*criteria)
        raw_uids = messages[0].split() if messages and messages[0] else []
        return [uid.decode() for uid in raw_uids]

    # ------------------------------------------------------------------
    # 邮件头批量获取
    # ------------------------------------------------------------------

    async def _fetch_headers_batch(
            self,
            imap: aioimaplib.IMAP4_SSL | aioimaplib.IMAP4,
            uids: list[str],
    ) -> dict[str, dict[str, Any]]:
        """批量获取邮件头部（BODY.PEEK[HEADER]），返回 uid -> 元数据字典。"""
        if not uids:
            return {}
        uid_list = ",".join(uids)
        _, data = await imap.uid("fetch", uid_list, "BODY.PEEK[HEADER]")

        results = {}
        for i, item in enumerate(data):
            if not isinstance(item, bytes) or b"BODY[HEADER]" not in item:
                continue

            # 标准格式：UID 在头部行内
            uid_match = re.search(rb"UID (\d+)", item)
            if uid_match and i + 1 < len(data) and isinstance(data[i + 1], bytearray):
                uid = uid_match.group(1).decode()
                raw_headers = bytes(data[i + 1])
                metadata = self._parse_headers(uid, raw_headers)
                if metadata:
                    results[uid] = metadata
            # Proton Bridge 格式：UID 在后方独立条目中
            elif i + 2 < len(data) and isinstance(data[i + 1], bytearray):
                uid_after_match = re.search(rb"UID (\d+)", data[i + 2]) if isinstance(data[i + 2], bytes) else None
                if uid_after_match:
                    uid = uid_after_match.group(1).decode()
                    raw_headers = bytes(data[i + 1])
                    metadata = self._parse_headers(uid, raw_headers)
                    if metadata:
                        results[uid] = metadata
        return results

    def _parse_headers(self, email_id: str, raw_headers: bytes) -> dict[str, Any] | None:
        """从原始头部数据解析出邮件元数据。"""
        try:
            parser = BytesParser(policy=default)
            msg = parser.parsebytes(raw_headers)
            return {
                "email_id": email_id,
                "subject": msg.get("Subject", ""),
                "from": msg.get("From", ""),
                "to": self._parse_recipients(msg),
                "date": self._parse_date(msg.get("Date", "")),
                "attachments": [],
            }
        except Exception as e:
            logger.error(f"Error parsing headers for {email_id}: {e}")
            return None

    # ------------------------------------------------------------------
    # 邮件内容解析
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_recipients(email_message) -> list[str]:
        """从 To 和 Cc 中提取收件人地址列表。"""
        recipients = []
        to_header = email_message.get("To", "")
        if to_header:
            recipients.extend([addr.strip() for addr in to_header.split(",")])
        cc_header = email_message.get("Cc", "")
        if cc_header:
            recipients.extend([addr.strip() for addr in cc_header.split(",")])
        return recipients

    @staticmethod
    def _parse_date(date_str: str) -> datetime:
        """解析邮件 Date 头部，失败时返回当前 UTC 时间。"""
        try:
            date_tuple = email.utils.parsedate_tz(date_str)
            if date_tuple:
                return datetime.fromtimestamp(email.utils.mktime_tz(date_tuple), tz=timezone.utc)
            return datetime.now(timezone.utc)
        except Exception as e:
            logger.error(f"Error parsing date for {date_str}: {e}")
            return datetime.now(timezone.utc)

    @staticmethod
    def _strip_html(html: str) -> str:
        """简单将 HTML 转换为纯文本。"""
        import re
        text = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<(br|p|div|tr|li)[^>]*/?>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "", text)
        text = text.replace("&nbsp;", " ").replace("&amp;", "&")
        text = text.replace("&lt;", "<").replace("&gt;", ">")
        text = text.replace("&quot;", '"').replace("&#39;", "'")
        text = re.sub(r"\n\s*\n", "\n\n", text)
        text = re.sub(r" +", " ", text)
        return text.strip()

    @staticmethod
    def check_field(infos, _default="null", charset=None):
        """邮件头字段解码，兼容 RFC2047 及多种编码。"""
        if not infos:
            return _default
        decoded_parts = decode_header(infos)
        result = []

        def _decode_bytes(_data: bytes, _enc: str | None) -> str:
            priority = []
            if charset:
                priority.append(charset)
            if _enc and _enc != "unknown-8bit":
                priority.append(_enc)
            priority.extend(["gb18030", "utf-8"])
            for enc_name in priority:
                try:
                    return _data.decode(enc_name.strip())
                except (UnicodeDecodeError, LookupError):
                    continue
            return data.decode("utf-8", errors="replace")

        for data, enc in decoded_parts:
            if isinstance(data, bytes):
                result.append(_decode_bytes(data, enc))
            else:
                result.append(str(data))
        final = "".join(result).strip()
        return final if final else _default

    @staticmethod
    def decode_filename(part) -> str | None:
        """从邮件部分提取附件名，并清理无效字符。"""
        filename = part.get_filename()
        content_type = part.get_content_type()
        content_disposition = str(part.get("Content-Disposition", "")).lower()

        is_attachment = bool(filename) or "attachment" in content_disposition or (
                content_type not in ["text/plain", "text/html", "image/jpeg", "image/png", "image/gif"]
                and not content_type.startswith("multipart/")
        )
        if not is_attachment:
            return None

        if not filename:
            ext = content_type.split("/")[-1] if "/" in content_type else "bin"
            filename = f"attachment.{ext}"
        else:
            try:
                decoded = decode_header(filename)
                parts = []
                for data, enc in decoded:
                    if isinstance(data, bytes):
                        for enc_name in ["utf-8", "gbk", "gb2312", "iso-8859-1"]:
                            try:
                                parts.append(data.decode(enc_name))
                                break
                            except UnicodeDecodeError:
                                continue
                    else:
                        parts.append(data)
                filename = "".join(parts)
            except Exception as e:
                logger.error(f"Error decoding filename: {e}")
                pass

        # 清理文件名非法字符
        cleaned = re.sub(r'[\r\n\\/:*?"<>|]', '_', filename).strip()
        return cleaned

    def _extract_body_from_message(self, msg, cache_attachments: bool, email_id: str | None, cache_dir: str | None):
        """从 email.message.Message 对象中提取正文和附件列表。"""
        body = ""
        html_body = ""
        attachments = []

        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                disp = str(part.get("Content-Disposition", "")).lower()

                # 处理附件
                if "attachment" in disp:
                    filename = self.decode_filename(part)
                    if filename:
                        if cache_attachments and email_id and cache_dir:
                            data = part.get_payload(decode=True)
                            cache_path = Path(cache_dir) / email_id / filename
                            cache_path.parent.mkdir(parents=True, exist_ok=True)
                            cache_path.write_bytes(data)
                        attachments.append(filename)
                # 文本内容
                elif content_type == "text/plain":
                    payload = part.get_payload(decode=True)
                    if payload:
                        charset = part.get_content_charset("utf-8")
                        try:
                            body += payload.decode(charset)
                        except UnicodeDecodeError:
                            body += payload.decode("utf-8", errors="replace")
                elif content_type == "text/html" and not body:
                    payload = part.get_payload(decode=True)
                    if payload:
                        charset = part.get_content_charset("utf-8")
                        try:
                            html_body += payload.decode(charset)
                        except UnicodeDecodeError:
                            html_body += payload.decode("utf-8", errors="replace")
            if not body and html_body:
                body = self._strip_html(html_body)
        else:
            # 单部分邮件
            payload = msg.get_payload(decode=True)
            if payload:
                charset = msg.get_content_charset("utf-8")
                try:
                    text = payload.decode(charset)
                except UnicodeDecodeError:
                    text = payload.decode("utf-8", errors="replace")
                if msg.get_content_type() == "text/html":
                    body = self._strip_html(text)
                else:
                    body = text

        if len(body) > MAX_BODY_LENGTH:
            body = body[:MAX_BODY_LENGTH] + "...[TRUNCATED]"

        if len(html_body) > MAX_BODY_LENGTH:
            html_body = html_body[:MAX_BODY_LENGTH] + "...[TRUNCATED]"
        return body, html_body, attachments

    def _parse_email_data(
            self,
            raw_email: bytes,
            email_id: str | None = None,
            cache_attachments: bool = False,
            attachment_cache_dir: str | None = "attachments",
    ) -> dict[str, Any]:
        """将原始邮件字节解析为结构化字典。"""
        email_parser = email.parser.BytesFeedParser()
        email_parser.feed(raw_email)
        msg = email_parser.close()

        subject = self.check_field(msg.get("Subject", ""))
        sender = self.check_field(msg.get("From", ""))
        to = self._parse_recipients(msg)
        _date = self._parse_date(msg.get("Date", ""))
        message_id = msg.get("Message-ID")

        body, html_body, attachments = self._extract_body_from_message(
            msg, cache_attachments, email_id, attachment_cache_dir
        )

        return {
            "email_id": email_id or "",
            "message_id": message_id,
            "subject": subject,
            "from": sender,
            "to": to,
            "body": body,
            "html_body": html_body,
            "date": _date,
            "attachments": attachments,
            "cache_attachments": cache_attachments and bool(attachments),
        }

    # ------------------------------------------------------------------
    # 公共 API - 计数 / UID 列表
    # ------------------------------------------------------------------

    async def get_email_count(self, mailbox: str = "INBOX", **kwargs) -> int:
        """获取符合条件的邮件数量。"""
        async with self._with_imap(mailbox) as imap:
            uids = await self._search_uids(imap, **kwargs)
            return len(uids)

    async def get_email_uid(self, mailbox: str = "INBOX", **kwargs) -> list[str]:
        """获取符合条件的邮件 UID 列表。"""
        async with self._with_imap(mailbox) as imap:
            return await self._search_uids(imap, **kwargs)

    # ------------------------------------------------------------------
    # 公共 API - 元数据流式分页
    # ------------------------------------------------------------------

    async def get_emails_metadata_stream(
            self,
            page: int = 1,
            page_size: int = 10,
            order: str = "desc",
            mailbox: str = "INBOX",
            **kwargs,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """流式返回分页后的邮件元数据。"""
        async with self._with_imap(mailbox) as imap:
            all_uids = await self._search_uids(imap, **kwargs)
            if not all_uids:
                return
            sorted_uids = sorted([int(uid) for uid in all_uids], reverse=(order == "desc"))
            # 分页切片
            start = (page - 1) * page_size
            end = start + page_size
            page_uids = [str(uid) for uid in sorted_uids[start:end]]
            if not page_uids:
                return

            metadata_map = await self._fetch_headers_batch(imap, page_uids)
            for uid in page_uids:
                if uid in metadata_map:
                    yield metadata_map[uid]

    # ------------------------------------------------------------------
    # 公共 API - 获取完整邮件内容
    # ------------------------------------------------------------------
    @staticmethod
    async def _fetch_raw_emails_batch(
            imap: aioimaplib.IMAP4_SSL | aioimaplib.IMAP4,
            email_ids: list[str],
    ) -> dict[str, bytes | None]:
        """
        批量获取多封邮件的原始字节数据（使用 RFC822）。
        返回 {uid: raw_bytes} 字典，获取失败的 uid 对应 None。
        这是批量获取的主力方法，一次 IMAP 命令获取所有邮件。
        """
        if not email_ids:
            return {}
        uid_list = ",".join(email_ids)
        try:
            _, data = await imap.uid("fetch", uid_list, "RFC822")
        except Exception as e:
            logger.error(f"Batch fetch RFC822 failed: {e}")
            # 批量失败时，返回全部为 None，让调用方决定是否降级
            return {uid: None for uid in email_ids}

        # 解析响应，将每个 UID 对应的邮件内容提取出来
        results = dict[str, bytes | None]({uid: None for uid in email_ids})
        # data 格式: [b'... FETCH (UID 123 RFC822 {...})', bytearray(...), ...]
        # 需要将 UID 和紧随其后的 bytearray 配对
        i = 0
        while i < len(data):
            item = data[i]
            if isinstance(item, bytes) and b"FETCH" in item:
                # 提取 UID
                uid_match = re.search(rb"UID (\d+)", item)
                if uid_match:
                    uid = uid_match.group(1).decode()
                    # 下一个元素应该是邮件内容 (bytearray)
                    if i + 1 < len(data) and isinstance(data[i + 1], (bytes, bytearray)):
                        raw = bytes(data[i + 1])
                        if raw:
                            results[uid] = raw
                    i += 2
                else:
                    i += 1
            else:
                i += 1
        return results

    @staticmethod
    async def _fetch_email_raw_fallback(imap, email_id: str) -> bytes | None:
        """
        单封邮件的备用获取方法（降级使用）。
        尝试多种 FETCH 格式，用于处理那些不支持标准 RFC822 批量获取的极端服务器。
        此方法仅在批量获取完全失败时作为后备。
        """
        for fmt in ["RFC822", "BODY[]", "BODY.PEEK[]", "(BODY.PEEK[])"]:
            try:
                _, data = await imap.uid("fetch", email_id, fmt)
                if data and len(data) > 1 and isinstance(data[1], bytearray):
                    return bytes(data[1])
                for item in data:
                    if isinstance(item, (bytes, bytearray)) and len(item) > 100:
                        if isinstance(item, bytes) and b"FETCH" in item:
                            continue
                        return bytes(item) if isinstance(item, bytearray) else item
            except Exception as e:
                _ = e
                continue
        return None

    async def get_emails_body_by_id(
            self,
            email_ids: list[str],
            mailbox: str = "INBOX",
            cache_attachments: bool = False,
            attachment_cache_dir: str | None = "attachments",
            use_fallback: bool = False,  # 当批量失败时是否降级到单封备用模式
    ) -> dict[str, Any]:
        """
        批量获取多封邮件内容。
        优先使用一次命令批量获取，如果批量获取完全失败且 use_fallback=True，
        则降级为循环单封获取（使用备用方法）。
        """
        async with self._with_imap(mailbox) as imap:
            # 发送 NOOP 重置服务器空闲计时器
            await imap.noop()
            # 尝试批量获取
            raw_map = await self._fetch_raw_emails_batch(imap, email_ids)
            # 检查是否批量完全失败（所有结果都是 None）
            all_failed = all(v is None for v in raw_map.values())

            # 降级：循环单封获取
            emails_content = []
            failed = []
            _is_error = all_failed and use_fallback
            if _is_error:
                logger.warning(
                    f"Batch fetch completely failed for {len(email_ids)} emails, falling back to single fetch")

            for uid in email_ids:
                raw = raw_map.get(uid) if not _is_error else await self._fetch_email_raw_fallback(imap, uid)
                if not raw:
                    failed.append(uid)
                    continue
                try:
                    data = self._parse_email_data(raw, uid, cache_attachments, attachment_cache_dir)
                    emails_content.append(
                        EmailBodyResponse(
                            email_id=data["email_id"],
                            message_id=data.get("message_id"),
                            subject=data["subject"],
                            sender=data["from"],
                            recipients=data["to"],
                            date=data["date"],
                            body=data["body"],
                            html_body=data["html_body"],
                            attachments=data["attachments"],
                            cache_attachments=data["cache_attachments"],
                        )
                    )
                except Exception as e:
                    logger.error(f"Failed to parse email {uid}: {e}")
                    failed.append(uid)
            return {"emails_content": emails_content, "failed_ids": failed}

    async def get_email_body_by_id(
            self,
            email_id: str,
            mailbox: str = "INBOX",
            cache_attachments: bool = False,
            attachment_cache_dir: str | None = "attachments",
            use_fallback: bool = True,
    ) -> dict[str, Any] | None:
        """
        获取单封邮件内容，完全复用批量方法。
        """
        result = await self.get_emails_body_by_id(
            [email_id], mailbox, cache_attachments, attachment_cache_dir, use_fallback
        )
        if result["emails_content"]:
            return {
                "status": "success",
                "email_content": result["emails_content"][0],
            }
        else:
            return {
                "status": "error",
                "message": f"Failed to fetch email {email_id}",
            }

    # ------------------------------------------------------------------
    # 公共 API - 附件下载
    # ------------------------------------------------------------------

    async def download_attachment(
            self,
            email_id: str,
            attachment_name: str,
            save_path: str,
            mailbox: str = "INBOX",
    ) -> dict[str, Any]:
        """下载邮件中的指定附件到本地文件。"""
        async with self._with_imap(mailbox) as imap:
            raw = await self._fetch_email_raw_fallback(imap, email_id)
            if not raw:
                raise ValueError(f"Email {email_id} not found")
            msg = BytesParser(policy=default).parsebytes(raw)
            if not msg.is_multipart():
                raise ValueError(f"Email {email_id} has no attachments")

            for part in msg.walk():
                if "attachment" in str(part.get("Content-Disposition", "")).lower():
                    filename = part.get_filename()
                    if filename == attachment_name:
                        data = part.get_payload(decode=True)
                        mime_type = part.get_content_type()
                        save_file = Path(save_path)
                        save_file.parent.mkdir(parents=True, exist_ok=True)
                        save_file.write_bytes(data)
                        return {
                            "email_id": email_id,
                            "attachment_name": attachment_name,
                            "mime_type": mime_type or "application/octet-stream",
                            "size": len(data),
                            "saved_path": str(save_file.resolve()),
                        }
            raise ValueError(f"Attachment '{attachment_name}' not found")

    # ------------------------------------------------------------------
    # 公共 API - 删除邮件
    # ------------------------------------------------------------------

    async def delete_emails(self, email_ids: list[str], mailbox: str = "INBOX") -> tuple[list[str], list[str]]:
        """删除邮件，返回 (成功删除的UID列表, 失败的UID列表)。"""
        async with self._with_imap(mailbox) as imap:
            deleted, failed = [], []
            for uid in email_ids:
                try:
                    await imap.uid("store", uid, "+FLAGS", r"(\Deleted)")
                    deleted.append(uid)
                except Exception as e:
                    _ = e
                    failed.append(uid)
            if deleted:
                await imap.expunge()
            return deleted, failed

    # ------------------------------------------------------------------
    # SMTP 发件相关
    # ------------------------------------------------------------------

    def _get_smtp_ssl_context(self) -> ssl.SSLContext | None:
        return _create_ssl_context(self.smtp_verify_ssl)

    @staticmethod
    def _validate_attachment(file_path: str) -> Path:
        path = Path(file_path)
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"Attachment not found: {file_path}")
        return path

    @staticmethod
    def _create_attachment_part(path: Path) -> MIMEApplication:
        with open(path, "rb") as f:
            data = f.read()
        mime_type, _ = mimetypes.guess_type(str(path))
        if not mime_type:
            mime_type = "application/octet-stream"
        subtype = mime_type.split("/")[1]
        part = MIMEApplication(data, _subtype=subtype)
        part.add_header("Content-Disposition", "attachment", filename=path.name)
        return part

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
        """发送邮件，返回构造好的 MIME 消息（用于后续保存到已发送）。"""
        if attachments:
            msg = MIMEMultipart()
            content_type = "html" if html else "plain"
            msg.attach(MIMEText(body, content_type, "utf-8"))
            for file_path in attachments:
                path = self._validate_attachment(file_path)
                msg.attach(self._create_attachment_part(path))
        else:
            content_type = "html" if html else "plain"
            msg = MIMEText(body, content_type, "utf-8")

        # 处理中文编码的头部
        def _encode_header(val: str) -> str:
            return Header(val, "utf-8").encode() if any(ord(c) > 127 for c in val) else val

        msg["Subject"] = _encode_header(subject)
        msg["From"] = _encode_header(self.sender)
        msg["To"] = ", ".join(recipients)
        if cc:
            msg["Cc"] = ", ".join(cc)
        if in_reply_to:
            msg["In-Reply-To"] = in_reply_to
        if references:
            msg["References"] = references
        msg["Date"] = email.utils.formatdate(localtime=True)
        sender_domain = self.sender.rsplit("@", 1)[-1].rstrip(">")
        msg["Message-Id"] = email.utils.make_msgid(domain=sender_domain)

        all_recipients = recipients[:]
        if cc:
            all_recipients.extend(cc)
        if bcc:
            all_recipients.extend(bcc)

        async with aiosmtplib.SMTP(
                hostname=self.email_server.host,
                port=self.email_server.port,
                start_tls=self.smtp_start_tls,
                use_tls=self.smtp_use_tls,
                tls_context=self._get_smtp_ssl_context(),
        ) as smtp:
            await smtp.login(self.email_server.user_name, self.email_server.password)
            await smtp.send_message(msg, recipients=all_recipients)

        return msg

    @staticmethod
    async def _find_sent_folder_by_flag(imap) -> str | None:
        """通过 \\Sent 标志查找已发送文件夹。"""
        try:
            _, folders = await imap.list('""', "*")
            for folder in folders:
                folder_str = folder.decode() if isinstance(folder, bytes) else str(folder)
                if r"\Sent" in folder_str or "\\Sent" in folder_str:
                    parts = folder_str.split('"')
                    if len(parts) >= 3:
                        return parts[-2]
        except Exception as e:
            logger.debug(f"Error finding Sent folder: {e}")
        return None

    async def append_to_sent(
            self,
            msg: MIMEText | MIMEMultipart,
            incoming_server: EmailServer,
            sent_folder_name: str | None = None,
    ) -> bool:
        """将已发送邮件追加到 IMAP 已发送文件夹。"""
        ctx = _create_ssl_context(incoming_server.verify_ssl) if incoming_server.use_ssl else None
        imap = aioimaplib.IMAP4_SSL(incoming_server.host, incoming_server.port,
                                    ssl_context=ctx) if incoming_server.use_ssl else aioimaplib.IMAP4(
            incoming_server.host, incoming_server.port)

        candidates = [f for f in
                      [sent_folder_name, "Sent", "INBOX.Sent", "Sent Items", "Sent Mail", "[Gmail]/Sent Mail",
                       "INBOX/Sent"] if f]
        try:
            await imap.wait_hello_from_server()
            await imap.login(incoming_server.user_name, incoming_server.password)
            await _send_imap_id(imap)

            flag_folder = await self._find_sent_folder_by_flag(imap)
            if flag_folder and flag_folder not in candidates:
                candidates.insert(0, flag_folder)

            for folder in candidates:
                try:
                    result = await imap.select(_quote_mailbox(folder))
                    if str(result[0] if isinstance(result, tuple) else result).upper() == "OK":
                        append_res = await imap.append(msg.as_bytes(), mailbox=_quote_mailbox(folder), flags=r"(\Seen)")
                        if str(append_res[0] if isinstance(append_res, tuple) else append_res).upper() == "OK":
                            logger.info(f"Saved sent email to '{folder}'")
                            return True
                except Exception as e:
                    logger.error(f"Error appending email to folder '{folder}': {e}")
                    continue
            return False
        finally:
            try:
                await imap.logout()
            except Exception as e:
                logger.error(f"Error logging out of IMAP server: {e}")
                pass


# ----------------------------------------------------------------------
# ClassicEmailHandler - 业务层（缓存、任务管理）
# ----------------------------------------------------------------------

class ClassicEmailHandler(EmailHandler):
    _cache_tasks: dict[str, dict[str, Any]] = {}

    def __init__(self, email_settings: EmailSettings):
        self.email_settings = email_settings
        self.incoming_client = EmailClient(email_settings.incoming)
        self.outgoing_client = EmailClient(
            email_settings.outgoing,
            sender=f"{email_settings.full_name} <{email_settings.email_address}>",
        )
        self.save_to_sent = email_settings.save_to_sent
        self.sent_folder_name = email_settings.sent_folder_name
        self._cache_lock = asyncio.Lock()

        # SQLite 替代 JSON 文件存储处理结果
        self.db_proc_results = 'emails_proc_results.db'
        self._db_initialized = False

    # ------------------------------------------------------------------
    # 内部缓存工具方法 (保留 JSON 相关工具，因为其他方法仍在使用)
    # ------------------------------------------------------------------

    class _DateTimeEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            return super().default(obj)

    @staticmethod
    async def _load_json_file(filename: str) -> dict:
        """异步读取 JSON 文件，若文件不存在或解析失败则返回空字典。"""
        if not os.path.exists(filename):
            return {}
        try:
            async with aiofiles.open(filename, 'r', encoding='utf-8') as f:
                content = await f.read()
                if content.strip():
                    return json.loads(content)
        except (json.JSONDecodeError, IOError) as e:
            raise RuntimeError(f"Cache file corrupted: {filename}") from e
        return {}

    @staticmethod
    async def _save_json_file(filename: str, data: dict) -> None:
        tmp = filename + '.tmp'
        async with aiofiles.open(tmp, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(data, ensure_ascii=False, indent=2))
        os.replace(tmp, filename)

    @staticmethod
    async def _update_json_file(filename: str, updater: Callable[[dict], dict]) -> dict:
        """
        原子化更新 JSON 文件：读取 -> 调用 updater 修改 -> 写回。
        返回更新后的完整数据。
        """
        data = await ClassicEmailHandler._load_json_file(filename)
        new_data = updater(data)
        await ClassicEmailHandler._save_json_file(filename, new_data)
        return new_data

    @staticmethod
    def _generate_task_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    async def _save_emails_chunk(emails_chunk, filename='emails.json'):
        """增量保存邮件到缓存，避免重复 UID。"""

        def updater(existing: dict) -> dict:
            for _email in emails_chunk:
                if hasattr(_email, 'model_dump'):
                    d = _email.model_dump()
                elif hasattr(_email, 'dict'):
                    d = _email.dict()
                else:
                    d = _email if isinstance(_email, dict) else vars(_email)
                uid = d.get("email_id")
                if uid and uid not in existing:
                    existing[uid] = d
            return existing

        await ClassicEmailHandler._update_json_file(filename, updater)

    @staticmethod
    async def _record_failed_uids(uids, reason="", filename='failed_emails.json'):
        if not uids:
            return
        now = datetime.now().isoformat()

        def updater(existing: dict) -> dict:
            for uid in map(str, uids):
                if uid not in existing:
                    existing[uid] = {
                        "uid": uid,
                        "first_failed_at": now,
                        "last_failed_at": now,
                        "fail_count": 1,
                        "last_reason": reason,
                    }
                else:
                    existing[uid]["last_failed_at"] = now
                    existing[uid]["fail_count"] += 1
                    existing[uid]["last_reason"] = reason
            return existing

        await ClassicEmailHandler._update_json_file(filename, updater)

    # ---------- SQLite 处理结果存储 ----------
    async def _ensure_proc_results_db(self):
        """确保 SQLite 数据库和表已初始化，并启用 WAL 模式。"""
        if self._db_initialized:
            return
        async with aiosqlite.connect(self.db_proc_results) as db:
            await db.execute('''
                             CREATE TABLE IF NOT EXISTS proc_results
                             (
                                 id
                                 INTEGER
                                 PRIMARY
                                 KEY
                                 AUTOINCREMENT,
                                 email_id
                                 INTEGER
                                 NOT
                                 NULL,
                                 status
                                 TEXT,
                                 message
                                 TEXT,
                                 created_at
                                 TEXT
                                 NOT
                                 NULL,
                                 raw_json
                                 TEXT
                                 NOT
                                 NULL
                             )
                             ''')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_email_id ON proc_results(email_id)')
            await db.execute('PRAGMA journal_mode=WAL')
            await db.commit()
        self._db_initialized = True

    async def save_proc_result(self, result: dict):
        """保存邮件处理结果到 SQLite，email_id 转为整数，若与上一条完全相同则跳过。"""
        await self._ensure_proc_results_db()

        email_id = result.pop("email_id")
        try:
            email_id_int = int(email_id)
        except (ValueError, TypeError):
            return UtilResponse(success=False, message=f"email_id 必须为可转为整数的值: {email_id}", data=None)

        result["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        raw_json = json.dumps(result, ensure_ascii=False)

        async with aiosqlite.connect(self.db_proc_results) as db:
            # 开启一个写事务，保证检查+插入的原子性
            await db.execute("BEGIN IMMEDIATE")
            try:
                # 查询该 email_id 最新一条记录的 raw_json
                cursor = await db.execute(
                    "SELECT raw_json FROM proc_results WHERE email_id = ? ORDER BY id DESC LIMIT 1",
                    (email_id_int,)
                )
                row = await cursor.fetchone()
                if row:
                    last_data = json.loads(row[0])
                    # 比较时排除自动添加的 created_at（因为时间肯定不同）
                    this_compare = {k: v for k, v in result.items() if k != "created_at"}
                    last_compare = {k: v for k, v in last_data.items() if k != "created_at"}
                    if this_compare == last_compare:
                        # 完全相同，跳过插入
                        await db.commit()
                        return UtilResponse(
                            success=True,
                            message=f"邮件 {email_id_int} 结果与上次相同，跳过保存",
                            data={"skipped": True}
                        )

                # 不同或首次记录，插入新行
                await db.execute(
                    "INSERT INTO proc_results (email_id, status, message, created_at, raw_json) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (email_id_int, result.get("status"), result.get("message"),
                     result["created_at"], raw_json)
                )
                await db.commit()
            except Exception:
                await db.rollback()
                raise

        return UtilResponse(
            success=True,
            message=f"成功保存邮件 {email_id_int} 的处理结果",
            data={"email_id": email_id_int}
        )

    async def get_proc_results(self, email_id: str) -> UtilResponse:
        """查询某个邮件的所有处理历史记录。"""
        await self._ensure_proc_results_db()
        async with aiosqlite.connect(self.db_proc_results) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT email_id, status, message, created_at, raw_json "
                "FROM proc_results WHERE email_id = ? ORDER BY id ASC",
                (email_id,)
            )
            rows = await cursor.fetchall()
            results = []
            for row in rows:
                data = json.loads(row["raw_json"])
                data["email_id"] = row["email_id"]
                data["created_at"] = row["created_at"]
                results.append(data)

            return UtilResponse(
                success=True,
                message=f"找到 {len(results)} 条记录",
                data={"history": results}
            )

    # ------------------------------------------------------------------
    # 公共 API - 数量/UID/元数据
    # ------------------------------------------------------------------

    async def get_emails_count(self, **kwargs) -> EmailCountResponse:
        total = await self.incoming_client.get_email_count(**kwargs)
        return EmailCountResponse(email_count=total)

    async def get_emails_uid(self, **kwargs) -> EmailUIDResponse:
        uids = await self.incoming_client.get_email_uid(**kwargs)
        return EmailUIDResponse(email_uid_list=uids)

    async def get_emails_metadata(
            self,
            page: int = 1,
            page_size: int = 10,
            order: str = "desc",
            mailbox: str = "INBOX",
            **kwargs,
    ) -> EmailMetadataPageResponse:
        total = await self.incoming_client.get_email_count(mailbox=mailbox, **kwargs)
        emails = []
        async for meta in self.incoming_client.get_emails_metadata_stream(
                page=page, page_size=page_size, order=order, mailbox=mailbox, **kwargs
        ):
            emails.append(EmailMetadata.from_email(meta))
        return EmailMetadataPageResponse(
            page=page,
            page_size=page_size,
            before=kwargs.get("before"),
            since=kwargs.get("since"),
            subject=kwargs.get("subject"),
            emails=emails,
            total=total,
        )

    # ------------------------------------------------------------------
    # 公共 API - 获取邮件内容（带缓存）
    # ------------------------------------------------------------------

    async def get_email_content(
            self,
            email_id: str,
            mailbox: str = "INBOX",
            use_cache: bool = True,
            update_cache: bool = True,
            cache_file: str = 'emails.json',
            cache_attachments: bool = False,
            attachment_cache_dir: str | None = "attachments",
    ) -> UtilResponse:
        """获取单封邮件内容，支持本地缓存。"""
        if use_cache:
            existing_cache = await self._load_json_file(cache_file)
            if email_id in existing_cache:
                return UtilResponse(success=True, data=existing_cache[email_id], message=f"{email_id} 缓存命中")

        result = await self.incoming_client.get_email_body_by_id(
            email_id, mailbox, cache_attachments, attachment_cache_dir
        )
        if result.get("status") != "success":
            return UtilResponse(success=False, message=result.get("message", "Unknown error"), data=None)

        email_obj = result["email_content"]
        if update_cache:
            await self._save_emails_chunk([email_obj], filename=cache_file)

        return UtilResponse(
            success=True,
            data=email_obj.model_dump(),
            message=f"{email_id} 查询成功",
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
        """批量获取邮件内容，优先从缓存读取。"""
        emails = []
        failed = []
        missing = []

        if use_cache:
            cache = await self._load_json_file(cache_file)
            for uid in email_ids:
                if uid in cache:
                    try:
                        emails.append(EmailBodyResponse.model_validate(cache[uid]))
                    except Exception as e:
                        logger.error(f"Failed to validate cache entry for {uid}: {e}")
                        missing.append(uid)
                else:
                    missing.append(uid)
        else:
            missing = email_ids[:]

        if missing:
            result = await self.incoming_client.get_emails_body_by_id(
                missing, mailbox, cache_attachments, attachment_cache_dir, use_fallback=False
            )
            for email_obj in result.get("emails_content", []):
                emails.append(email_obj)
            failed.extend(result.get("failed_ids", []))

            if update_cache and result.get("emails_content"):
                await self._save_emails_chunk(result["emails_content"], filename=cache_file)

        return EmailContentBatchResponse(
            emails=emails,
            requested_count=len(email_ids),
            retrieved_count=len(emails),
            failed_ids=failed,
        )

    # ------------------------------------------------------------------
    # 公共 API - 发送/删除/附件下载
    # ------------------------------------------------------------------

    async def send_email(self, **kwargs) -> None:
        msg = await self.outgoing_client.send_email(**kwargs)
        if self.save_to_sent and msg:
            try:
                await self.outgoing_client.append_to_sent(
                    msg, self.email_settings.incoming, self.sent_folder_name
                )
            except Exception as e:
                logger.error(f"Failed to save to Sent: {e}")

    async def delete_emails(self, email_ids: list[str], mailbox: str = "INBOX") -> tuple[list[str], list[str]]:
        return await self.incoming_client.delete_emails(email_ids, mailbox)

    async def download_attachment(
            self, email_id: str, attachment_name: str, save_path: str, mailbox: str = "INBOX"
    ) -> AttachmentDownloadResponse:
        res = await self.incoming_client.download_attachment(email_id, attachment_name, save_path, mailbox)
        return AttachmentDownloadResponse(**res)

    # ------------------------------------------------------------------
    # 公共 API - 缓存任务（后台异步）
    # ------------------------------------------------------------------

    async def cache_emails(
            self,
            mailbox: str = "INBOX",
            cache_attachments: bool = True,
            attachment_cache_dir: str | None = "attachments",
    ) -> UtilResponse:
        task_id = self._generate_task_id()
        self._cache_tasks[task_id] = {"status": "pending", "total": 0, "processed": 0, "message": "Task created"}
        asyncio.create_task(self._run_cache_task(task_id, mailbox, cache_attachments, attachment_cache_dir))
        return UtilResponse(message=f"Cache task started: {task_id}", success=True, data={"task_id": task_id})

    async def _run_cache_task(self, task_id: str, mailbox: str, cache_attachments: bool, cache_dir: str | None):
        """后台执行全量缓存。"""
        try:
            uids_resp = await self.get_emails_uid(mailbox=mailbox)
            all_uids = uids_resp.email_uid_list
            total_all = len(all_uids)

            cache_file = 'emails.json'
            async with self._cache_lock:
                cache_data = await self._load_json_file(cache_file)
                existing_uids = set(cache_data.keys())
            new_uids = [uid for uid in all_uids if uid not in existing_uids]
            total_new = len(new_uids)

            self._cache_tasks[task_id].update({
                "status": "running",
                "total": total_new,
                "processed": 0,
                "message": f"共 {total_all} 封邮件，{total_new} 封需要缓存",
            })
            if not new_uids:
                self._cache_tasks[task_id].update({"status": "completed", "message": "无新邮件需要缓存"})
                return

            fetched_emails = {}
            chunk_size = 50
            for i in range(0, len(new_uids), chunk_size):
                chunk = new_uids[i:i + chunk_size]
                resp = await self.get_emails_content(
                    email_ids=chunk,
                    mailbox=mailbox,
                    use_cache=False,
                    update_cache=True,
                    cache_file=cache_file,
                    cache_attachments=cache_attachments,
                    attachment_cache_dir=cache_dir,
                )
                if resp.success and resp.data:
                    fetched_emails.update(resp.data)
                if resp.failed_ids:
                    await self._record_failed_uids(resp.failed_ids, "缓存获取失败")
                self._cache_tasks[task_id]["processed"] = min(i + len(chunk), len(new_uids))
                self._cache_tasks[task_id][
                    "message"] = f"已缓存 {self._cache_tasks[task_id]['processed']} / {len(new_uids)}"

            if fetched_emails:
                async with self._cache_lock:
                    cache_data = await self._load_json_file(cache_file)
                    cache_data.update(fetched_emails)
                    try:
                        sorted_items = sorted(cache_data.items(), key=lambda x: int(x[0]))
                    except ValueError:
                        sorted_items = sorted(cache_data.items(), key=lambda x: x[0])
                    cache_data = dict(sorted_items)
                    await self._save_json_file(cache_file, cache_data)

            self._cache_tasks[task_id].update({"status": "completed", "message": "缓存完成"})
        except Exception as e:
            logger.error(f"Cache task {task_id} failed: {e}")
            self._cache_tasks[task_id].update({"status": "failed", "message": str(e), "error": str(e)})

    async def get_cache_status(self, task_id: str) -> UtilResponse:
        task = self._cache_tasks.get(task_id)
        if not task:
            return UtilResponse(success=False, message=f"Task {task_id} not found", data=None)
        return UtilResponse(
            success=task["status"] == "completed",
            message=task["message"],
            data={
                "task_id": task_id,
                "status": task["status"],
                "total": task.get("total", 0),
                "processed": task.get("processed", 0),
                "error": task.get("error"),
            },
        )

    # ------------------------------------------------------------------
    # 辅助工具 - Base64 附件读取
    # ------------------------------------------------------------------
    @staticmethod
    async def get_file_info(file_path: str) -> dict:
        """获取文件信息：base64, 文件名, 扩展名, MIME类型"""
        path = Path(file_path)
        file_name = path.name
        file_extension = path.suffix.lower()
        mime_type, _ = mimetypes.guess_type(file_path)
        if mime_type is None:
            # 默认二进制
            mime_type = 'application/octet-stream'
        file_base64 = base64.b64encode(open(file_path, 'rb').read()).decode('utf-8')
        return {
            "fileName": file_name,
            "fileExtension": file_extension,
            "mimeType": mime_type,
            "fileBase64": file_base64
        }

    async def get_attachment_by_base64(self, email_id: str) -> UtilResponse:
        """根据邮件 ID 读取本地缓存的附件，返回附件信息列表。"""
        folder = Path(f"attachments/{email_id}")
        if not folder.exists() or not folder.is_dir():
            return UtilResponse(success=False, message=f"附件目录不存在: {email_id}", data=None)
        attachments_info = []
        for file in folder.iterdir():
            if file.is_file():
                try:
                    info = await self.get_file_info(str(file))
                    attachments_info.append(info)
                except Exception as e:
                    logger.error(f"读取附件 {file} 失败: {e}")
                    return UtilResponse(success=False, message=f"读取失败: {e}", data=None)
        if not attachments_info:
            return UtilResponse(success=False, message=f"目录 {email_id} 下无附件", data=None)
        return UtilResponse(success=True, message=f"成功读取 {len(attachments_info)} 个附件",
                            data={"attachments": attachments_info})
