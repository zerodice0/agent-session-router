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


def agent_id(value: str) -> str:
    resolved = value.strip()
    if ":" not in resolved:
        resolved = f"local:{resolved}"
    if not AGENT_ID_PATTERN.fullmatch(resolved):
        raise argparse.ArgumentTypeError("agent name must use letters, digits, '.', '_', ':', or '-'")
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

    claude = commands.add_parser("claude", help="start interactive Claude with the configured Channel")
    claude.add_argument("agent", nargs="?", default="claude-channel", type=agent_id)

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


def resolved_environment(agent: str | None = None) -> dict[str, str]:
    environment = os.environ.copy()
    environment.setdefault("ROUTER_URL", DEFAULT_ROUTER_URL)
    if agent is not None:
        environment["GATEWAY_AGENT_ID"] = agent
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


def doctor() -> int:
    missing = [name for name in ("bun", "codex", "python3") if shutil.which(name) is None]
    if missing:
        print(f"Missing commands: {', '.join(missing)}", file=sys.stderr)
        return 1
    print("Ready: bun, codex, and python3 are available.")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)

    if args.command == "doctor":
        return doctor()
    if args.command == "shell-init":
        print(f'asr() {{ python3 {shlex.quote(str(Path(__file__).resolve()))} "$@"; }}')
        return 0

    if args.command == "router":
        return execute(["bun", "run", "start"], resolved_environment(), args.dry_run)
    if args.command == "codex":
        environment = resolved_environment(args.agent)
        environment.setdefault("CODEX_CWD", os.getcwd())
        return execute(["bun", "run", "codex:interactive"], environment, args.dry_run)
    if args.command == "claude":
        return execute(
            [
                "claude",
                "--dangerously-load-development-channels",
                "server:agent-session-router-channel",
            ],
            resolved_environment(args.agent),
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
