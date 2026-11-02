import asyncio
import base64
import contextlib
import os
import uuid

import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.lowlevel.helper_types import ReadResourceContents
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import AnyUrl
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.routing import Mount, WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect

RELAY_SECRET = os.environ.get("RELAY_SECRET", "")
ALLOWED_ORIGINS = [o.strip() for o in os.environ.get("MCP_ALLOWED_ORIGINS", "").split(",") if o.strip()]

server = Server(
    "mcp-relay",
    instructions=(
        "This server is a transparent relay to a single remote host (a real, persistent "
        "machine — a workstation or server owned by the user), reachable only through this "
        "server. The tools, resources, and prompts exposed here are not defined by this "
        "server: they are proxied verbatim from a third-party MCP server running on that "
        "remote host. Calls are forwarded to the host and their results returned unchanged. "
        "If the host is disconnected, tool/resource/prompt lists come back empty and calls "
        "fail with a 'relay offline' error (nothing ran on the host)."
    ),
)

agent: WebSocket | None = None
pending: dict[str, asyncio.Future] = {}


async def rpc(method: str, params: dict) -> dict:
    if agent is None:
        raise RuntimeError("relay offline: no local machine connected")
    req_id = uuid.uuid4().hex
    fut = asyncio.get_running_loop().create_future()
    pending[req_id] = fut
    try:
        await agent.send_json({"id": req_id, "method": method, "params": params})
        reply = await asyncio.wait_for(fut, timeout=120)
    finally:
        pending.pop(req_id, None)
    if "error" in reply:
        raise RuntimeError(reply["error"])
    return reply["result"]


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    if agent is None:
        return []
    return types.ListToolsResult.model_validate(await rpc("list_tools", {})).tools


@server.call_tool(validate_input=False)
async def call_tool(name: str, arguments: dict) -> types.CallToolResult:
    return types.CallToolResult.model_validate(await rpc("call_tool", {"name": name, "arguments": arguments}))


@server.list_resources()
async def list_resources() -> list[types.Resource]:
    if agent is None:
        return []
    return types.ListResourcesResult.model_validate(await rpc("list_resources", {})).resources


@server.list_resource_templates()
async def list_resource_templates() -> list[types.ResourceTemplate]:
    if agent is None:
        return []
    return types.ListResourceTemplatesResult.model_validate(await rpc("list_resource_templates", {})).resourceTemplates


@server.read_resource()
async def read_resource(uri: AnyUrl) -> list[ReadResourceContents]:
    result = types.ReadResourceResult.model_validate(await rpc("read_resource", {"uri": str(uri)}))
    out = []
    for c in result.contents:
        if isinstance(c, types.TextResourceContents):
            out.append(ReadResourceContents(content=c.text, mime_type=c.mimeType))
        else:
            out.append(ReadResourceContents(content=base64.b64decode(c.blob), mime_type=c.mimeType))
    return out


@server.list_prompts()
async def list_prompts() -> list[types.Prompt]:
    if agent is None:
        return []
    return types.ListPromptsResult.model_validate(await rpc("list_prompts", {})).prompts


@server.get_prompt()
async def get_prompt(name: str, arguments: dict | None) -> types.GetPromptResult:
    return types.GetPromptResult.model_validate(await rpc("get_prompt", {"name": name, "arguments": arguments}))


async def agent_endpoint(ws: WebSocket):
    global agent
    if RELAY_SECRET and ws.headers.get("x-relay-secret") != RELAY_SECRET:
        await ws.close(code=1008)
        return
    await ws.accept()
    agent = ws
    try:
        while True:
            msg = await ws.receive_json()
            if msg.get("type") == "ping":
                continue
            fut = pending.get(msg.get("id"))
            if fut and not fut.done():
                fut.set_result(msg)
    except WebSocketDisconnect:
        pass
    finally:
        if agent is ws:
            agent = None


session_manager = StreamableHTTPSessionManager(
    app=server,
    stateless=True,
    security_settings=TransportSecuritySettings(
        allowed_hosts=["127.0.0.1:*", "localhost:*", "[::1]:*"],
        allowed_origins=["http://127.0.0.1:*", "http://localhost:*", "http://[::1]:*", *ALLOWED_ORIGINS],
    ),
)


@contextlib.asynccontextmanager
async def lifespan(app):
    async with session_manager.run():
        yield


inner = Starlette(
    lifespan=lifespan,
    routes=[
        WebSocketRoute("/agent", agent_endpoint),
        Mount("/mcp", app=session_manager.handle_request),
    ],
    middleware=[
        Middleware(
            CORSMiddleware,
            allow_origins=ALLOWED_ORIGINS,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    ],
)


async def app(scope, receive, send):
    if scope["type"] == "http":
        if scope["path"] == "/mcp":  # serve bare /mcp instead of 307-ing to internal localhost:8000/mcp/
            scope = {**scope, "path": "/mcp/", "raw_path": b"/mcp/"}
        h = {k.decode(): v.decode() for k, v in scope["headers"]}
        print(f"REQ {scope['method']} {scope['path']} origin={h.get('origin')} host={h.get('host')} "
              f"referer={h.get('referer')} xff={h.get('x-forwarded-for')} ua={h.get('user-agent')}", flush=True)
    await inner(scope, receive, send)
