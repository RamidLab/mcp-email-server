# Docker 部署指南

## 快速开始

### 前置条件

- Ubuntu 24.04（其他 Linux 发行版亦可）
- Docker 和 Docker Compose

```bash
# 安装 Docker
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
# 重新登录 SSH 使 docker 组生效
```

### 1. 克隆项目

```bash
git clone <repo-url> mcp-email-server
cd mcp-email-server
```

### 2. 配置邮箱

编辑 `config/config.toml`，填入实际邮箱信息：

```toml
db_location = "./data/db.sqlite3"
enable_attachment_download = true

[[emails]]
account_name = "work"
full_name = "Your Name"
email_address = "you@example.com"
save_to_sent = true

[emails.incoming]
user_name = "you@example.com"
password = "your_password"
host = "imap.example.com"
port = 993
use_ssl = true
verify_ssl = true

[emails.outgoing]
user_name = "you@example.com"
password = "your_password"
host = "smtp.example.com"
port = 465
use_ssl = true
verify_ssl = true
```

### 3. 启动服务

```bash
docker compose up -d --build
docker compose logs -f
```

服务启动后，MCP 端点地址为：`http://<服务器IP>:9557/mcp`

### 4. MCP 客户端连接

在 Dify、Claude Desktop 等 MCP 客户端中，传输方式选择 **Streamable HTTP**，填入：

```
http://<服务器IP>:9557/mcp
```

如果客户端也在 Docker 中运行，可使用：

```
http://host.docker.internal:9557/mcp
```

---

## 配置说明

### 配置文件

| 文件 | 用途 |
|---|---|
| `config/config.toml` | 本地开发配置 |
| `config/config.prod.toml` | 生产配置（db 指向 `./data/`） |

生产环境建议使用 `config.prod.toml`，在 `docker-compose.yml` 中通过环境变量指定：

```yaml
environment:
  - MCP_EMAIL_SERVER_CONFIG_PATH=/app/config/config.prod.toml
```

### 环境变量覆盖

可在 `docker-compose.yml` 中通过环境变量覆盖 config.toml 的邮箱配置（优先级更高）：

```yaml
environment:
  - MCP_EMAIL_SERVER_EMAIL_ADDRESS=you@example.com
  - MCP_EMAIL_SERVER_PASSWORD=your_password
  - MCP_EMAIL_SERVER_IMAP_HOST=imap.example.com
  - MCP_EMAIL_SERVER_SMTP_HOST=smtp.example.com
  - MCP_EMAIL_SERVER_ENABLE_ATTACHMENT_DOWNLOAD=true
```

### 卷挂载

| 容器路径 | 宿主机路径 | 用途 |
|---|---|---|
| `/app/config` | `./config` | 配置文件 |
| `/app/data` | `./data` | SQLite 数据库、邮件缓存等持久化数据 |

### 端口

默认映射 `9557`，可通过 `.env` 文件修改宿主机端口：

```bash
# .env
MCP_PORT=19557
```

---

## 常用运维命令

```bash
# 查看日志
docker compose logs -f

# 重启服务
docker compose restart

# 停止服务
docker compose down

# 代码更新后重建
git pull
docker compose build --no-cache
docker compose up -d

# 进入容器调试
docker compose exec mcp-email-server bash
```

---

## 常见问题

### 1. 镜像拉取超时

**现象**：`docker compose up -d --build` 时 `python:3.12-slim` 拉取超时报 `connection reset by peer`。

**原因**：Docker Hub 在国内被墙。

**解决**：配置 Docker daemon 镜像加速器，编辑 `/etc/docker/daemon.json`：

```json
{
  "registry-mirrors": [
    "https://mirror.ccs.tencentyun.com",
    "https://docker.m.daocloud.io"
  ]
}
```

然后重启 Docker：

```bash
sudo systemctl daemon-reload
sudo systemctl restart docker
```

### 2. aiosqlite 缺失

**现象**：容器启动即崩溃，日志报 `ModuleNotFoundError: No module named 'aiosqlite'`。

**原因**：代码中使用了 `aiosqlite` 但未在 `pyproject.toml` 中声明依赖。

**解决**：确认 `pyproject.toml` 的 dependencies 中包含 `"aiosqlite>=0.20.0"`，然后：

```bash
uv lock
docker compose build --no-cache
docker compose up -d
```

### 3. 421 Misdirected Request

**现象**：MCP 客户端连接时报 421，日志显示 `WARNING Invalid Host header: <IP>:9557`。

**原因**：MCP SDK 默认开启 DNS rebinding 保护，只允许 `localhost` 作为 Host header。局域网 IP 访问会被拒绝。

SDK 的 `allowed_hosts` 只支持精确匹配和 `hostname:*` 格式（如 `localhost:*`），不支持 IP 地址通配符。

**解决**：在 `cli.py` 的 `streamable_http` 命令中关闭 DNS rebinding 保护：

```python
if hasattr(mcp.settings, 'transport_security') and mcp.settings.transport_security:
    mcp.settings.transport_security.enable_dns_rebinding_protection = False
```

### 4. Docker 构建缓存导致代码未更新

**现象**：`git pull` 后 `docker compose up -d --build` 显示 `CACHED`，新代码未生效。

**解决**：使用 `--no-cache` 强制重建：

```bash
docker compose build --no-cache
docker compose up -d
```

### 5. Docker 权限不足

**现象**：`docker compose` 报 `permission denied while trying to connect to the docker API`。

**解决**：

```bash
# 永久方案：加入 docker 组（需重新登录 SSH）
sudo usermod -aG docker $USER

# 当前会话立即生效
newgrp docker

# 或直接用 sudo
sudo docker compose up -d
```

---

## 文件说明

```
.
├── Dockerfile                  # 多阶段构建，默认 streamable-http 模式
├── docker-compose.yml          # 一键编排
├── .dockerignore               # 精简镜像
├── .env.example                # 宿主机端口配置示例
└── config/
    ├── config.toml             # 本地开发配置（gitignore）
    └── config.prod.toml        # 生产配置
```
