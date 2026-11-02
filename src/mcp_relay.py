import argparse
import asyncio
import json
import os
import pathlib
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
from contextlib import AsyncExitStack

import websockets
from databricks.sdk.core import Config
from dotenv import load_dotenv
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.types import (
    ListPromptsResult,
    ListResourceTemplatesResult,
    ListResourcesResult,
    ListToolsResult,
)
from pydantic import AnyUrl

SEP = "__"

HERE = pathlib.Path(__file__).resolve().parent
# repo checkout / editable install, else the copy bundled into the wheel
RESOURCES = next(p for p in (HERE.parent / "resources", HERE / "mcp_relay_resources") if p.is_dir())
CACHE = pathlib.Path(".relay_cache.json")


def parse_args():
    load_dotenv(pathlib.Path(".env"))  # existing env vars take precedence
    env = os.environ.get
    p = argparse.ArgumentParser(description="Relay local MCP servers to a Databricks workspace.")
    p.add_argument("--profile", default=env("DATABRICKS_PROFILE"),
                   help="Databricks CLI profile (env: DATABRICKS_PROFILE; default: CLI default auth)")
    p.add_argument("--app-name", default=env("APP_NAME", "mcp-relay"),
                   help="Databricks App name (env: APP_NAME; default: mcp-relay)")
    p.add_argument("--config", type=pathlib.Path, default=env("MCP_SERVERS_CONFIG", "mcp_servers.json"),
                   help="local MCP servers config (env: MCP_SERVERS_CONFIG; default: mcp_servers.json)")
    p.add_argument("--workspace-path", default=env("WORKSPACE_PATH"),
                   help="workspace folder for the app source (env: WORKSPACE_PATH; "
                        "default: /Workspace/Users/<me>/<app-name>)")
    p.add_argument("--allowed-origins", default=env("MCP_ALLOWED_ORIGINS"),
                   help="comma-separated CORS origins for the app (env: MCP_ALLOWED_ORIGINS; default: workspace host)")
    p.add_argument("--redeploy", action="store_true", help="sync and deploy the app even if it is already running")
    return p.parse_args()


def cli(args, *cmd, capture=True):
    profile = ["--profile", args.profile] if args.profile else []
    return subprocess.run(["databricks", *cmd, *profile], capture_output=capture, text=True)


def app_state(args):
    r = cli(args, "apps", "get", args.app_name, "-o", "json")
    if r.returncode != 0:
        return None
    return json.loads(r.stdout)


def is_running(state):
    return bool(state) and state.get("compute_status", {}).get("state") == "ACTIVE"


def load_cache(args):
    if not CACHE.exists():
        return {}
    cache = json.loads(CACHE.read_text())
    return cache if cache.get("app_name") == args.app_name else {}


def bootstrap(args):
    """Deploy the app if needed; return (app_url, relay_secret)."""
    cache = load_cache(args)
    secret = os.environ.get("RELAY_SECRET") or cache.get("relay_secret") or secrets.token_urlsafe(32)
    origins = args.allowed_origins or Config(profile=args.profile).host.rstrip("/")

    state = app_state(args)
    unchanged = cache.get("relay_secret") == secret and cache.get("allowed_origins") == origins
    if not args.redeploy and unchanged and is_running(state):
        return cache["app_url"], secret

    if state is None:
        c = cli(args, "apps", "create", args.app_name)
        if c.returncode != 0:
            sys.exit(f"app create failed: {c.stderr}")

    ws_path = args.workspace_path
    if not ws_path:
        me = json.loads(cli(args, "current-user", "me", "-o", "json").stdout)["userName"]
        ws_path = f"/Workspace/Users/{me}/{args.app_name}"

    with tempfile.TemporaryDirectory() as tmp:
        shutil.copytree(RESOURCES, tmp, dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__", ".databricks"))
        app_yaml = pathlib.Path(tmp, "app.yaml")
        app_yaml.write_text(
            app_yaml.read_text().replace("{{RELAY_SECRET}}", secret).replace("{{MCP_ALLOWED_ORIGINS}}", origins)
        )
        if cli(args, "sync", tmp, ws_path, capture=False).returncode != 0:
            sys.exit("sync failed")

    d = cli(args, "apps", "deploy", args.app_name, "--source-code-path", ws_path)
    if d.returncode != 0:
        sys.exit(f"deploy failed: {d.stderr}")

    for _ in range(60):
        state = app_state(args)
        if is_running(state):
            url = state["url"]
            CACHE.write_text(json.dumps({
                "app_name": args.app_name,
                "app_url": url,
                "relay_secret": secret,
                "allowed_origins": origins,
                "deployed_at": time.time(),
            }))
            return url, secret
        time.sleep(5)
    sys.exit("app did not reach a running state")


def server_params(config_path):
    servers = json.loads(config_path.read_text())["mcpServers"]
    print(f"local MCP servers: {', '.join(servers)}")
    return {
        name: StdioServerParameters(command=cfg["command"], args=cfg.get("args", []), env=cfg.get("env"))
        for name, cfg in servers.items()
    }


async def dispatch(sessions, method, params):
    if method == "list_tools":
        tools = []
        for name, session in sessions.items():
            for t in (await session.list_tools()).tools:
                t.name = f"{name}{SEP}{t.name}"
                tools.append(t)
        return ListToolsResult(tools=tools)
    if method == "call_tool":
        name, tool = params["name"].split(SEP, 1)
        return await sessions[name].call_tool(tool, params.get("arguments"))
    if method == "list_prompts":
        prompts = []
        for name, session in sessions.items():
            for p in (await session.list_prompts()).prompts:
                p.name = f"{name}{SEP}{p.name}"
                prompts.append(p)
        return ListPromptsResult(prompts=prompts)
    if method == "get_prompt":
        name, prompt = params["name"].split(SEP, 1)
        return await sessions[name].get_prompt(prompt, params.get("arguments"))
    if method == "list_resources":
        resources = []
        for session in sessions.values():
            resources += (await session.list_resources()).resources
        return ListResourcesResult(resources=resources)
    if method == "list_resource_templates":
        templates = []
        for session in sessions.values():
            templates += (await session.list_resource_templates()).resourceTemplates
        return ListResourceTemplatesResult(resourceTemplates=templates)
    if method == "read_resource":
        for session in sessions.values():
            try:
                return await session.read_resource(AnyUrl(params["uri"]))
            except Exception:
                continue
        raise ValueError(f"no server served resource: {params['uri']}")
    raise ValueError(f"unknown method: {method}")


def bearer(args):
    return Config(profile=args.profile).authenticate()["Authorization"]


async def serve(args, app_url, secret):
    ws_url = app_url.replace("https://", "wss://") + "/agent"
    all_params = server_params(args.config)
    while True:
        try:
            headers = {"Authorization": bearer(args), "X-Relay-Secret": secret}
            async with AsyncExitStack() as stack:
                sessions = {}
                for name, params in all_params.items():
                    r, w = await stack.enter_async_context(stdio_client(params))
                    session = await stack.enter_async_context(ClientSession(r, w))
                    await session.initialize()
                    sessions[name] = session
                ws = await stack.enter_async_context(
                    websockets.connect(ws_url, additional_headers=headers, ping_interval=20)
                )
                print(f"connected to {ws_url}")
                async for raw in ws:
                    msg = json.loads(raw)
                    try:
                        result = await dispatch(sessions, msg["method"], msg.get("params", {}))
                        reply = {"id": msg["id"], "result": result.model_dump(mode="json", by_alias=True)}
                    except Exception as e:
                        reply = {"id": msg["id"], "error": str(e)}
                    await ws.send(json.dumps(reply))
        except Exception:
            print("disconnected; retrying in 5s")
            traceback.print_exc()
            await asyncio.sleep(5)


def main():
    args = parse_args()
    if args.profile:
        for var in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"):
            os.environ.pop(var, None)
    url, secret = bootstrap(args)
    print(f"app ready at {url}")
    print(f"MCP endpoint: {url}/mcp")
    asyncio.run(serve(args, url, secret))


if __name__ == "__main__":
    main()
