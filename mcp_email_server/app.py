from datetime import datetime
from typing import Annotated, Literal

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from mcp_email_server.config import (
    AccountAttributes,
    EmailSettings,
    ProviderSettings,
    get_settings,
)
from mcp_email_server.emails.dispatcher import dispatch_handler
from mcp_email_server.emails.models import (
    AttachmentDownloadResponse,
    EmailContentBatchResponse,
    EmailMetadataPageResponse,
    EmailCountResponse,
    EmailUIDResponse,
    UtilResponse,
)

mcp = FastMCP("邮件服务")


@mcp.resource("email://{account_name}")
async def get_account(account_name: str) -> EmailSettings | ProviderSettings | None:
    """获取指定账户的配置（已脱敏）"""
    settings = get_settings()
    return settings.get_account(account_name, masked=True)


@mcp.tool(description="列出所有已配置的邮件账户（凭证已脱敏）。")
async def list_available_accounts() -> list[AccountAttributes]:
    settings = get_settings()
    return [account.masked() for account in settings.get_accounts()]


@mcp.tool(description="添加一个新的邮件账户配置到设置中。")
async def add_email_account(email: EmailSettings) -> str:
    settings = get_settings()
    settings.add_email(email)
    settings.store()
    return f"成功添加邮件账户 '{email.account_name}'"


@mcp.tool(description="获取符合指定条件的邮件总数。")
async def get_emails_count(
        account_name: Annotated[str, Field(description="邮件账户名称。")],
        before: Annotated[
            datetime | None,
            Field(default=None, description="统计此时间（UTC）之前的邮件。"),
        ] = None,
        since: Annotated[
            datetime | None,
            Field(default=None, description="统计此时间（UTC）之后的邮件。"),
        ] = None,
        subject: Annotated[str | None, Field(default=None, description="按主题筛选邮件。")] = None,
        from_address: Annotated[str | None, Field(default=None, description="按发件人地址筛选。")] = None,
        to_address: Annotated[str | None, Field(default=None, description="按收件人地址筛选。")] = None,
        mailbox: Annotated[str, Field(default="INBOX", description="要统计的邮箱文件夹。")] = "INBOX",
        seen: Annotated[bool | None, Field(default=None, description="按已读状态筛选。")] = None,
        flagged: Annotated[bool | None, Field(default=None, description="按已标记状态筛选。")] = None,
        answered: Annotated[bool | None, Field(default=None, description="按已回复状态筛选。")] = None,
) -> EmailCountResponse:
    handler = dispatch_handler(account_name)
    return await handler.get_emails_count(
        before=before,
        since=since,
        subject=subject,
        from_address=from_address,
        to_address=to_address,
        mailbox=mailbox,
        seen=seen,
        flagged=flagged,
        answered=answered,
    )


@mcp.tool(description="获取符合给定过滤条件的邮件 UID 列表。")
async def get_emails_uid(
        account_name: Annotated[str, Field(description="邮件账户名称。")],
        before: Annotated[
            datetime | None,
            Field(default=None, description="此时间（UTC）之前的邮件。"),
        ] = None,
        since: Annotated[
            datetime | None,
            Field(default=None, description="此时间（UTC）之后的邮件。"),
        ] = None,
        subject: Annotated[str | None, Field(default=None, description="按主题筛选。")] = None,
        from_address: Annotated[str | None, Field(default=None, description="按发件人筛选。")] = None,
        to_address: Annotated[str | None, Field(default=None, description="按收件人筛选。")] = None,
        mailbox: Annotated[str, Field(default="INBOX", description="要搜索的邮箱文件夹。")] = "INBOX",
        seen: Annotated[bool | None, Field(default=None, description="按已读状态筛选。")] = None,
        flagged: Annotated[bool | None, Field(default=None, description="按已标记状态筛选。")] = None,
        answered: Annotated[bool | None, Field(default=None, description="按已回复状态筛选。")] = None,
) -> EmailUIDResponse:
    handler = dispatch_handler(account_name)
    return await handler.get_emails_uid(
        before=before,
        since=since,
        subject=subject,
        from_address=from_address,
        to_address=to_address,
        mailbox=mailbox,
        seen=seen,
        flagged=flagged,
        answered=answered,
    )


@mcp.tool(
    description="列出邮件元数据（邮件ID、主题、发件人、收件人、日期），不含正文。返回的 email_id 可用于 get_emails_content。"
)
async def list_emails_metadata(
        account_name: Annotated[str, Field(description="邮件账户名称。")],
        page: Annotated[
            int,
            Field(default=1, description="要检索的页码（从1开始）。"),
        ] = 1,
        page_size: Annotated[int, Field(default=10, description="每页返回的邮件数量。")] = 10,
        before: Annotated[
            datetime | None,
            Field(default=None, description="检索此时间（UTC）之前的邮件。"),
        ] = None,
        since: Annotated[
            datetime | None,
            Field(default=None, description="检索此时间（UTC）之后的邮件。"),
        ] = None,
        subject: Annotated[str | None, Field(default=None, description="按主题筛选邮件。")] = None,
        from_address: Annotated[str | None, Field(default=None, description="按发件人地址筛选。")] = None,
        to_address: Annotated[
            str | None,
            Field(default=None, description="按收件人地址筛选。"),
        ] = None,
        order: Annotated[
            Literal["asc", "desc"],
            Field(default=None, description="排序方式：`asc` 升序或 `desc` 降序。"),
        ] = "desc",
        mailbox: Annotated[str, Field(default="INBOX", description="要搜索的邮箱文件夹。")] = "INBOX",
        seen: Annotated[
            bool | None,
            Field(default=None, description="按已读状态筛选：True=已读，False=未读，None=全部。"),
        ] = None,
        flagged: Annotated[
            bool | None,
            Field(default=None, description="按已标记状态筛选：True=已标记，False=未标记，None=全部。"),
        ] = None,
        answered: Annotated[
            bool | None,
            Field(default=None, description="按已回复状态筛选：True=已回复，False=未回复，None=全部。"),
        ] = None,
) -> EmailMetadataPageResponse:
    handler = dispatch_handler(account_name)

    return await handler.get_emails_metadata(
        page=page,
        page_size=page_size,
        before=before,
        since=since,
        subject=subject,
        from_address=from_address,
        to_address=to_address,
        order=order,
        mailbox=mailbox,
        seen=seen,
        flagged=flagged,
        answered=answered,
    )


@mcp.tool(
    description="根据邮件 ID 获取一封或多封邮件的完整内容（含正文）。请先使用 list_emails_metadata 获取 email_id。"
)
async def get_emails_content(
        account_name: Annotated[str, Field(description="邮件账户名称。")],
        email_ids: Annotated[
            list[str],
            Field(
                description="要检索的 email_id 列表（从 list_emails_metadata 获得）。可以是一个或多个 email_id。"
            ),
        ],
        mailbox: Annotated[str, Field(default="INBOX", description="要检索邮件的邮箱文件夹。")] = "INBOX",
        use_cache: Annotated[bool, Field(default=True, description="是否使用本地缓存。")] = True,
        update_cache: Annotated[bool, Field(default=True, description="是否更新本地缓存。")] = True,
        cache_file: Annotated[str, Field(default='emails.json', description="本地缓存文件路径。")] = 'emails.json',
        cache_attachments: Annotated[bool, Field(default=False, description="是否将附件缓存到磁盘。")] = False,
        attachment_cache_dir: Annotated[
            str | None, Field(default="attachments", description="附件缓存目录。")] = "attachments",
) -> EmailContentBatchResponse:
    handler = dispatch_handler(account_name)
    return await handler.get_emails_content(
        email_ids, mailbox, use_cache, update_cache, cache_file, cache_attachments, attachment_cache_dir,
    )


@mcp.tool(
    description="根据邮件 ID 获取单封邮件的完整内容（含正文）。请先使用 list_emails_metadata 获取 email_id。"
)
async def get_email_content(
        account_name: Annotated[str, Field(description="邮件账户名称。")],
        email_id: Annotated[
            str,
            Field(
                description="要检索的 email_id（从 list_emails_metadata 获得）。单个邮件ID。"
            ),
        ],
        mailbox: Annotated[str, Field(default="INBOX", description="要检索邮件的邮箱文件夹。")] = "INBOX",
        use_cache: Annotated[bool, Field(default=True, description="是否使用本地缓存。")] = True,
        update_cache: Annotated[bool, Field(default=True, description="是否更新本地缓存。")] = True,
        cache_file: Annotated[str, Field(default='emails.json', description="本地缓存文件路径。")] = 'emails.json',
        cache_attachments: Annotated[bool, Field(default=False, description="是否将附件缓存到磁盘。")] = False,
        attachment_cache_dir: Annotated[
            str | None, Field(default="attachments", description="附件缓存目录。")] = "attachments",
) -> UtilResponse:
    handler = dispatch_handler(account_name)
    return await handler.get_email_content(
        email_id, mailbox, use_cache, update_cache, cache_file, cache_attachments, attachment_cache_dir,
    )


@mcp.tool(
    description="缓存指定账户中的所有邮件。"
)
async def cache_emails(
        account_name: Annotated[str, Field(description="邮件账户名称。")],
        mailbox: Annotated[str, Field(default="INBOX", description="要缓存的邮箱文件夹。")] = "INBOX",
        cache_attachments: Annotated[bool, Field(default=True, description="是否缓存附件。")] = True,
        attachment_cache_dir: Annotated[
            str | None, Field(default="attachments", description="附件缓存目录。")] = "attachments",
) -> UtilResponse:
    handler = dispatch_handler(account_name)
    return await handler.cache_emails(mailbox, cache_attachments, attachment_cache_dir)


@mcp.tool(
    description="使用指定账户发送邮件。支持通过 in_reply_to 参数正确回复邮件线程。",
)
async def send_email(
        account_name: Annotated[str, Field(description="发件账户名称。")],
        recipients: Annotated[list[str], Field(description="收件人邮箱地址列表。")],
        subject: Annotated[str, Field(description="邮件主题。")],
        body: Annotated[str, Field(description="邮件正文。")],
        cc: Annotated[
            list[str] | None,
            Field(default=None, description="抄送地址列表。"),
        ] = None,
        bcc: Annotated[
            list[str] | None,
            Field(default=None, description="密送地址列表。"),
        ] = None,
        html: Annotated[
            bool,
            Field(default=False, description="是否以 HTML 格式发送（True）还是纯文本（False）。"),
        ] = False,
        attachments: Annotated[
            list[str] | None,
            Field(
                default=None,
                description="要附加的本地文件绝对路径列表。支持常见文件类型（文档、图片、压缩包等）。",
            ),
        ] = None,
        in_reply_to: Annotated[
            str | None,
            Field(
                default=None,
                description="所回复邮件的 Message-ID。用于在邮件客户端中正确显示线程。",
            ),
        ] = None,
        references: Annotated[
            str | None,
            Field(
                default=None,
                description="线程链中的空格分隔的 Message-ID 列表。通常包含 in_reply_to 及其祖先。",
            ),
        ] = None,
) -> str:
    handler = dispatch_handler(account_name)
    await handler.send_email(
        recipients,
        subject,
        body,
        cc,
        bcc,
        html,
        attachments,
        in_reply_to,
        references,
    )
    recipient_str = ", ".join(recipients)
    attachment_info = f" 带 {len(attachments)} 个附件" if attachments else ""
    return f"邮件已成功发送至 {recipient_str}{attachment_info}"


@mcp.tool(
    description="根据邮件 ID 删除一封或多封邮件。请先使用 list_emails_metadata 获取 email_id。"
)
async def delete_emails(
        account_name: Annotated[str, Field(description="邮件账户名称。")],
        email_ids: Annotated[
            list[str],
            Field(description="要删除的 email_id 列表（从 list_emails_metadata 获得）。"),
        ],
        mailbox: Annotated[str, Field(default="INBOX", description="要删除邮件的邮箱文件夹。")] = "INBOX",
) -> str:
    handler = dispatch_handler(account_name)
    deleted_ids, failed_ids = await handler.delete_emails(email_ids, mailbox)

    result = f"成功删除 {len(deleted_ids)} 封邮件"
    if failed_ids:
        result += f"，删除失败 {len(failed_ids)} 封：{', '.join(failed_ids)}"
    return result


@mcp.tool(
    description="下载邮件附件并保存到指定路径。出于安全考虑，此功能需要在设置中显式启用（enable_attachment_download=true）。",
)
async def download_attachment(
        account_name: Annotated[str, Field(description="邮件账户名称。")],
        email_id: Annotated[
            str, Field(description="邮件 ID（从 list_emails_metadata 或 get_emails_content 获得）。")
        ],
        attachment_name: Annotated[
            str, Field(description="要下载的附件名称（如附件列表中显示）。")
        ],
        save_path: Annotated[str, Field(description="附件保存的绝对路径。")],
        mailbox: Annotated[str, Field(description="要搜索的邮箱文件夹（默认：INBOX）。")] = "INBOX",
) -> AttachmentDownloadResponse:
    settings = get_settings()
    if not settings.enable_attachment_download:
        msg = (
            "附件下载功能已禁用。请在设置中设置 'enable_attachment_download=true' 以启用此功能。"
        )
        raise PermissionError(msg)

    handler = dispatch_handler(account_name)
    return await handler.download_attachment(email_id, attachment_name, save_path, mailbox)


@mcp.tool(
    description="获取后台缓存操作的状态。"
)
async def get_cache_status(
        account_name: Annotated[str, Field(description="邮件账户名称。")],
        task_id: Annotated[str, Field(description="缓存操作的任务 ID。")],
) -> UtilResponse:
    handler = dispatch_handler(account_name)
    return await handler.get_cache_status(task_id)


@mcp.tool(
    description="获取邮件附件的 Base64 编码（不保存到磁盘）。"
)
async def get_attachment_by_base64(
        account_name: Annotated[str, Field(description="邮件账户名称。")],
        email_id: Annotated[str, Field(description="邮件 ID（从 list_emails_metadata 或 get_emails_content 获得）。")],
) -> UtilResponse:
    handler = dispatch_handler(account_name)
    return await handler.get_attachment_by_base64(email_id)
