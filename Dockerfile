# ---- Build stage ----
FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# 先只复制依赖定义，利用 Docker 层缓存
COPY uv.lock pyproject.toml ./
RUN uv sync --frozen --no-install-project

# 复制源码并安装项目本身
COPY . .
RUN uv sync --frozen

# ---- Runtime stage ----
FROM python:3.12-slim

# tini 作为 PID 1，正确处理系统信号
RUN apt-get update && \
    apt-get install -y --no-install-recommends tini && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 从构建阶段复制虚拟环境和项目代码
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/pyproject.toml /app/uv.lock /app/
COPY --from=builder /app/mcp_email_server /app/mcp_email_server

# 创建持久化目录
RUN mkdir -p /app/data /app/config

# 直接使用 venv 中的 Python，无需 uv run（启动更快）
ENV PATH="/app/.venv/bin:$PATH"

# 默认暴露 streamable-http 端口
EXPOSE 9557

# 入口点：默认以 streamable-http 模式运行
# 如需 stdio 模式：docker run ... mcp-email-server stdio
ENTRYPOINT ["tini", "--", "mcp-email-server"]
CMD ["streamable-http"]
