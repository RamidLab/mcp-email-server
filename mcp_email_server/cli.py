import os

import typer

from mcp_email_server.app import mcp
from mcp_email_server.config import delete_settings

app = typer.Typer()


@app.command()
def stdio():
    mcp.run(transport="stdio")


@app.command()
def sse(
    host: str = "localhost",
    port: int = 9557,
):
    mcp.settings.host = host
    mcp.settings.port = port
    mcp.run(transport="sse")


@app.command()
def streamable_http(
    host: str = os.environ.get("MCP_HOST", "localhost"),
    port: int = os.environ.get("MCP_PORT", 9557),
):
    mcp.settings.host = host
    mcp.settings.port = port
    if hasattr(mcp.settings, 'transport_security') and mcp.settings.transport_security:
        mcp.settings.transport_security.allowed_hosts.append('host.docker.internal:*')
    mcp.run(transport="streamable-http")


@app.command()
def ui():
    from mcp_email_server.ui import main as ui_main

    ui_main()


@app.command()
def reset():
    delete_settings()
    typer.echo("✅ Config reset")


if __name__ == "__main__":
    app(["stdio"])
