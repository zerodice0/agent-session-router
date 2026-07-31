#!/usr/bin/env python3
"""Small local launcher for agent-session-router development commands."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import sys
from pathlib import Path


AGENT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
DEFAULT_ROUTER_URL = "ws://127.0.0.1:8787/ws"
REPO_ROOT = Path(__file__).resolve().parent.parent
CODEX_CLI_MCP_SERVER_ID = "agent_session_router_cli"
CODEX_CLI_MCP_ENV_VARS = [
    "ROUTER_URL",
    "GATEWAY_AGENT_ID",
    "GATEWAY_AGENT_ACTIVITY",
    "ROUTER_TOKEN",
]


def agent_id(value: str) -> str:
    resolved = value.strip()
    if ":" not in resolved:
        resolved = f"local:{resolved}"
    if not AGENT_ID_PATTERN.fullmatch(resolved):
        raise argparse.ArgumentTypeError("agent name must use letters, digits, '.', '_', ':', or '-'")
    return resolved


def agent_activity(value: str) -> str:
    resolved = value.strip()
    has_control_character = any(
        ord(character) < 32 or ord(character) == 127 for character in resolved
    )
    if not 1 <= len(resolved) <= 160 or has_control_character:
        raise argparse.ArgumentTypeError("activity must be 1-160 printable characters")
    return resolved


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        prog="asr",
        description="Run the local agent session router and interactive provider connectors.",
    )
    result.add_argument(
        "--dry-run",
        action="store_true",
        help="print the resolved command without starting it",
    )
    commands = result.add_subparsers(dest="command", required=True)

    commands.add_parser("router", help="start the loopback router")

    codex = commands.add_parser("codex", help="start a prompt-capable Codex connector")
    codex.add_argument("agent", nargs="?", default="codex", type=agent_id)
    codex.add_argument("--activity", type=agent_activity, help="public non-sensitive work summary")

    codex_cli = commands.add_parser("codex-cli", help="start stock Codex CLI with router MCP tools")
    codex_cli.add_argument("agent", nargs="?", default="codex-cli", type=agent_id)
    codex_cli.add_argument("--activity", type=agent_activity, help="public non-sensitive work summary")
    codex_cli.add_argument(
        "codex_args",
        nargs=argparse.REMAINDER,
        help="arguments passed to Codex after an optional '--' separator",
    )

    claude = commands.add_parser("claude", help="start interactive Claude with the configured Channel")
    claude.add_argument("agent", nargs="?", default="claude-channel", type=agent_id)
    claude.add_argument("--activity", type=agent_activity, help="public non-sensitive work summary")
    claude.add_argument(
        "--auto",
        action="store_true",
        help="start Claude in auto permission mode when the account supports it",
    )

    commands.add_parser(
        "setup-claude",
        help="register the repository-local Claude Channel MCP entry once",
    )
    commands.add_parser("test", help="run the Bun test suite")
    smoke = commands.add_parser("smoke", help="run a router-only round trip")
    smoke.add_argument("target", nargs="?", type=agent_id)
    smoke.add_argument("--timeout-ms", type=int, help=argparse.SUPPRESS)
    commands.add_parser("doctor", help="check required local commands")
    commands.add_parser("shell-init", help="print a shell function for the short 'asr' command")
    return result


def resolved_environment(agent: str | None = None, activity: str | None = None) -> dict[str, str]:
    environment = os.environ.copy()
    environment.setdefault("ROUTER_URL", DEFAULT_ROUTER_URL)
    if agent is not None:
        environment["GATEWAY_AGENT_ID"] = agent
    if activity is not None:
        environment["GATEWAY_AGENT_ACTIVITY"] = activity
    return environment


def execute(command: list[str], environment: dict[str, str], dry_run: bool) -> int:
    if dry_run:
        output: dict[str, object] = {
            "command": command,
            "routerUrl": environment.get("ROUTER_URL", DEFAULT_ROUTER_URL),
        }
        if "GATEWAY_AGENT_ID" in environment:
            output["agentId"] = environment["GATEWAY_AGENT_ID"]
        print(json.dumps(output, separators=(",", ":")))
        return 0

    os.chdir(REPO_ROOT)
    os.execvpe(command[0], command, environment)
    return 1


def codex_cli_command(workspace: str, extra_args: list[str]) -> list[str]:
    server_script = str(REPO_ROOT / "src" / "codex-cli-mcp-server.ts")
    prefix = f"mcp_servers.{CODEX_CLI_MCP_SERVER_ID}"
    forwarded_args = extra_args[1:] if extra_args[:1] == ["--"] else extra_args
    return [
        "codex",
        "-C",
        workspace,
        "-c",
        f"{prefix}.command={json.dumps('bun')}",
        "-c",
        f"{prefix}.args={json.dumps([server_script], separators=(',', ':'))}",
        "-c",
        f"{prefix}.env_vars={json.dumps(CODEX_CLI_MCP_ENV_VARS, separators=(',', ':'))}",
        "-c",
        f"{prefix}.required=true",
        "-c",
        f'{prefix}.enabled_tools=["agent_list","agent_send","agent_wait","agent_reply"]',
        "-c",
        f'{prefix}.default_tools_approval_mode="approve"',
        "-c",
        f"{prefix}.startup_timeout_sec=10",
        "-c",
        f"{prefix}.tool_timeout_sec=600",
        *forwarded_args,
    ]


def normalize_codex_cli_options(argv: list[str]) -> list[str]:
    """Keep launcher options before argparse's passthrough remainder."""
    try:
        command_index = argv.index("codex-cli")
    except ValueError:
        return argv

    separator_index = (
        argv.index("--", command_index + 1)
        if "--" in argv[command_index + 1 :]
        else len(argv)
    )
    for index in range(command_index + 1, separator_index):
        value = argv[index]
        if value == "--activity" and index + 1 < separator_index:
            option = argv[index : index + 2]
            remaining = argv[:index] + argv[index + 2 :]
            return remaining[: command_index + 1] + option + remaining[command_index + 1 :]
        if value.startswith("--activity="):
            remaining = argv[:index] + argv[index + 1 :]
            return remaining[: command_index + 1] + [value] + remaining[command_index + 1 :]
    return argv


def doctor() -> int:
    missing = [name for name in ("bun", "codex", "python3") if shutil.which(name) is None]
    if missing:
        print(f"Missing commands: {', '.join(missing)}", file=sys.stderr)
        return 1
    print("Ready: bun, codex, and python3 are available.")
    return 0


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    args = parser().parse_args(normalize_codex_cli_options(raw_argv))

    if args.command == "doctor":
        return doctor()
    if args.command == "shell-init":
        print(f'asr() {{ python3 {shlex.quote(str(Path(__file__).resolve()))} "$@"; }}')
        return 0

    if args.command == "router":
        return execute(["bun", "run", "start"], resolved_environment(), args.dry_run)
    if args.command == "codex":
        environment = resolved_environment(args.agent, args.activity)
        environment.setdefault("CODEX_CWD", os.getcwd())
        return execute(["bun", "run", "codex:interactive"], environment, args.dry_run)
    if args.command == "codex-cli":
        workspace = os.getcwd()
        return execute(
            codex_cli_command(workspace, args.codex_args),
            resolved_environment(args.agent, args.activity),
            args.dry_run,
        )
    if args.command == "claude":
        permission_mode = ["--permission-mode", "auto"] if args.auto else []
        return execute(
            [
                "claude",
                *permission_mode,
                "--dangerously-load-development-channels",
                "server:agent-session-router-channel",
            ],
            resolved_environment(args.agent, args.activity),
            args.dry_run,
        )
    if args.command == "setup-claude":
        return execute(
            [
                "claude",
                "mcp",
                "add",
                "--transport",
                "stdio",
                "--scope",
                "local",
                "agent-session-router-channel",
                "--",
                "bun",
                "run",
                "channel:claude",
            ],
            resolved_environment(),
            args.dry_run,
        )
    if args.command == "test":
        return execute(["bun", "test"], resolved_environment(), args.dry_run)
    if args.command == "smoke":
        environment = resolved_environment()
        if args.target is None:
            return execute(["bun", "run", "smoke"], environment, args.dry_run)
        environment["SMOKE_TARGET"] = args.target
        if args.timeout_ms is not None:
            environment["SMOKE_TIMEOUT_MS"] = str(args.timeout_ms)
        return execute(["bun", "run", "smoke:provider"], environment, args.dry_run)

    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
