#!/usr/bin/env python3
"""Small local launcher for agent-session-router development commands."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit


AGENT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
PROFILE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
DEFAULT_ROUTER_URL = "ws://127.0.0.1:8787/ws"
DEFAULT_ROUTER_PROFILE = "local"
CONFIG_VERSION = 1
REPO_ROOT = Path(__file__).resolve().parent.parent
CODEX_CLI_MCP_SERVER_ID = "agent_session_router_cli"
CODEX_CLI_MCP_ENV_VARS = [
    "ROUTER_URL",
    "GATEWAY_AGENT_ID",
    "GATEWAY_AGENT_ACTIVITY",
    "ROUTER_TOKEN",
]


class RouterProfileError(ValueError):
    """Raised when local router profile configuration is invalid."""


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


def profile_name(value: str) -> str:
    resolved = value.strip()
    if not PROFILE_NAME_PATTERN.fullmatch(resolved):
        raise RouterProfileError(
            "profile name must use letters, digits, '.', '_', or '-'"
        )
    return resolved


def router_url(value: str) -> str:
    resolved = value.strip()
    if not resolved or any(character.isspace() for character in resolved):
        raise RouterProfileError("router address must not be empty or contain whitespace")

    had_scheme = "://" in resolved
    candidate = resolved if had_scheme else f"ws://{resolved}"
    try:
        parsed = urlsplit(candidate)
        port = parsed.port
    except ValueError as error:
        raise RouterProfileError("router address has an invalid port") from error

    if parsed.scheme not in {"ws", "wss"}:
        raise RouterProfileError("router address must use ws:// or wss://")
    if not parsed.hostname:
        raise RouterProfileError("router address must include a host")
    if parsed.username is not None or parsed.password is not None:
        raise RouterProfileError("router address must not contain credentials")
    if parsed.query or parsed.fragment:
        raise RouterProfileError("router address must not contain a query or fragment")

    netloc = parsed.netloc
    if not had_scheme and port is None:
        host = parsed.hostname
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        netloc = f"{host}:8787"
    path = parsed.path if parsed.path not in {"", "/"} else "/ws"
    return urlunsplit((parsed.scheme, netloc, path, "", ""))


def router_config_path(
    environment: dict[str, str] | None = None,
) -> Path:
    values = os.environ if environment is None else environment
    explicit = values.get("ASR_CONFIG_PATH")
    if explicit:
        return Path(explicit).expanduser()
    xdg_config_home = values.get("XDG_CONFIG_HOME")
    base = Path(xdg_config_home).expanduser() if xdg_config_home else Path.home() / ".config"
    return base / "agent-session-router" / "config.json"


def read_router_profiles(path: Path | None = None) -> tuple[dict[str, str], str]:
    config_path = path or router_config_path()
    profiles = {DEFAULT_ROUTER_PROFILE: DEFAULT_ROUTER_URL}
    if not config_path.exists():
        return profiles, DEFAULT_ROUTER_PROFILE

    try:
        value = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RouterProfileError(f"cannot read router profiles: {config_path}") from error
    if not isinstance(value, dict) or value.get("version") != CONFIG_VERSION:
        raise RouterProfileError("router profile configuration has an unsupported version")

    stored_profiles = value.get("profiles", {})
    if not isinstance(stored_profiles, dict):
        raise RouterProfileError("router profile configuration is invalid")
    for raw_name, entry in stored_profiles.items():
        if not isinstance(raw_name, str) or not isinstance(entry, dict):
            raise RouterProfileError("router profile configuration is invalid")
        name = profile_name(raw_name)
        if name == DEFAULT_ROUTER_PROFILE:
            raise RouterProfileError("the built-in local profile cannot be overridden")
        raw_url = entry.get("routerUrl")
        if not isinstance(raw_url, str):
            raise RouterProfileError("router profile configuration is invalid")
        profiles[name] = router_url(raw_url)

    default_profile = value.get("defaultProfile", DEFAULT_ROUTER_PROFILE)
    if not isinstance(default_profile, str) or default_profile not in profiles:
        raise RouterProfileError("default router profile does not exist")
    return profiles, default_profile


def write_router_profiles(
    profiles: dict[str, str],
    default_profile: str,
    path: Path | None = None,
) -> None:
    config_path = path or router_config_path()
    if default_profile not in profiles:
        raise RouterProfileError("default router profile does not exist")
    custom_profiles = {
        name: {"routerUrl": url}
        for name, url in sorted(profiles.items())
        if name != DEFAULT_ROUTER_PROFILE
    }
    payload = {
        "version": CONFIG_VERSION,
        "defaultProfile": default_profile,
        "profiles": custom_profiles,
    }

    config_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{config_path.name}.", dir=config_path.parent
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as temporary:
            json.dump(payload, temporary, indent=2, sort_keys=True)
            temporary.write("\n")
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_name, config_path)
        os.chmod(config_path, 0o600)
    except Exception:
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            Path(temporary_name).unlink()
        except FileNotFoundError:
            pass
        raise


def save_router_profile(
    name: str,
    address: str,
    *,
    overwrite: bool = False,
    path: Path | None = None,
) -> str:
    resolved_name = profile_name(name)
    if resolved_name == DEFAULT_ROUTER_PROFILE:
        raise RouterProfileError("the built-in local profile cannot be changed")
    resolved_url = router_url(address)
    profiles, _ = read_router_profiles(path)
    if resolved_name in profiles and not overwrite:
        raise RouterProfileError(f"router profile already exists: {resolved_name}")
    profiles[resolved_name] = resolved_url
    write_router_profiles(profiles, resolved_name, path)
    return resolved_url


def set_default_router_profile(name: str, path: Path | None = None) -> None:
    resolved_name = profile_name(name)
    profiles, _ = read_router_profiles(path)
    if resolved_name not in profiles:
        raise RouterProfileError(f"router profile does not exist: {resolved_name}")
    write_router_profiles(profiles, resolved_name, path)


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
    commands = result.add_subparsers(dest="command")

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
    profiles = commands.add_parser("profile", help="manage local router address profiles")
    profile_commands = profiles.add_subparsers(dest="profile_command", required=True)
    profile_commands.add_parser("list", help="list saved router profiles")
    profile_add = profile_commands.add_parser("add", help="save a router address")
    profile_add.add_argument("name", type=profile_name)
    profile_add.add_argument("address", type=router_url)
    profile_add.add_argument("--force", action="store_true", help="replace an existing profile")
    profile_use = profile_commands.add_parser("use", help="select the default router profile")
    profile_use.add_argument("name", type=profile_name)
    return result


def resolved_environment(
    agent: str | None = None,
    activity: str | None = None,
    selected_router_url: str | None = None,
) -> dict[str, str]:
    environment = os.environ.copy()
    if selected_router_url is None:
        environment.setdefault("ROUTER_URL", DEFAULT_ROUTER_URL)
    else:
        environment["ROUTER_URL"] = router_url(selected_router_url)
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


def select_option(prompt: str, options: list[tuple[str, str]]) -> str | None:
    """Select a stable value with fzf, falling back to a numbered terminal menu."""
    if not options:
        return None
    labels = [label for _, label in options]
    fzf = shutil.which("fzf")
    if fzf:
        result = subprocess.run(
            [
                fzf,
                "--height=~40%",
                "--layout=reverse",
                "--border",
                "--prompt",
                f"{prompt}> ",
            ],
            input="\n".join(labels) + "\n",
            stdout=subprocess.PIPE,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            return None
        selected_label = result.stdout.rstrip("\n")
        return next((value for value, label in options if label == selected_label), None)

    print(f"\n{prompt}")
    for index, (_, label) in enumerate(options, start=1):
        print(f"  {index}. {label}")
    while True:
        try:
            selected = input("Select: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return None
        if selected.isdigit() and 1 <= int(selected) <= len(options):
            return options[int(selected) - 1][0]
        print(f"Enter a number between 1 and {len(options)}.", file=sys.stderr)


def prompt_value(
    label: str,
    default: str | None = None,
    *,
    optional: bool = False,
) -> str | None:
    suffix = f" [{default}]" if default else ""
    while True:
        try:
            value = input(f"{label}{suffix}: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return None
        if value:
            return value
        if default is not None:
            return default
        if optional:
            return ""
        print(f"{label} is required.", file=sys.stderr)


def add_router_profile_interactive(dry_run: bool = False) -> tuple[str, str] | None:
    raw_name = prompt_value("Profile name")
    if raw_name is None:
        return None
    raw_address = prompt_value("Router address (host, host:port, ws://, or wss://)")
    if raw_address is None:
        return None

    try:
        name = profile_name(raw_name)
        address = router_url(raw_address)
        profiles, _ = read_router_profiles()
        overwrite = False
        if name in profiles:
            if name == DEFAULT_ROUTER_PROFILE:
                raise RouterProfileError("the built-in local profile cannot be changed")
            decision = select_option(
                f"Replace profile '{name}'?",
                [("replace", "Replace existing profile"), ("cancel", "Cancel")],
            )
            if decision != "replace":
                return None
            overwrite = True
        if not dry_run:
            save_router_profile(name, address, overwrite=overwrite)
    except (OSError, RouterProfileError) as error:
        print(f"Router profile was not saved: {error}", file=sys.stderr)
        return None

    print(f"{'Would save' if dry_run else 'Saved'} router profile: {name}")
    return name, address


def select_router_profile(dry_run: bool = False) -> tuple[str, str] | None:
    try:
        profiles, default_profile = read_router_profiles()
    except RouterProfileError as error:
        print(f"Cannot load router profiles: {error}", file=sys.stderr)
        return None

    ordered_names = [default_profile, *sorted(name for name in profiles if name != default_profile)]
    options = [
        (
            name,
            f"{'*' if name == default_profile else ' '} {name:<16} {profiles[name]}",
        )
        for name in ordered_names
    ]
    options.append(("__add__", "+ Add router address"))
    selected = select_option("Router", options)
    if selected is None:
        return None
    if selected == "__add__":
        return add_router_profile_interactive(dry_run)

    if not dry_run:
        try:
            set_default_router_profile(selected)
        except (OSError, RouterProfileError) as error:
            print(f"Warning: default router profile was not saved: {error}", file=sys.stderr)
    return selected, profiles[selected]


def prompt_agent(default: str) -> str | None:
    while True:
        value = prompt_value("Agent ID", default)
        if value is None:
            return None
        try:
            return agent_id(value)
        except argparse.ArgumentTypeError as error:
            print(str(error), file=sys.stderr)


def prompt_activity() -> str | None:
    while True:
        value = prompt_value("Activity (optional)", optional=True)
        if value is None or value == "":
            return value
        try:
            return agent_activity(value)
        except argparse.ArgumentTypeError as error:
            print(str(error), file=sys.stderr)


def interactive_launcher(dry_run: bool) -> int:
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        print(
            "Interactive asr requires a terminal; use 'asr --help' for explicit commands.",
            file=sys.stderr,
        )
        return 2

    action = select_option(
        "Run",
        [
            ("claude", "Claude Code"),
            ("codex-cli", "Codex CLI"),
            ("codex", "Codex interactive connector"),
            ("router", "Start local router"),
            ("smoke", "Test router connection"),
            ("add-profile", "Add router address"),
            ("exit", "Exit"),
        ],
    )
    if action is None or action == "exit":
        return 0
    if action == "router":
        return execute(["bun", "run", "start"], resolved_environment(), dry_run)
    if action == "add-profile":
        return 0 if add_router_profile_interactive(dry_run) else 1

    selected_profile = select_router_profile(dry_run)
    if selected_profile is None:
        return 1
    _, selected_url = selected_profile
    if action == "smoke":
        return execute(
            ["bun", "run", "smoke"],
            resolved_environment(selected_router_url=selected_url),
            dry_run,
        )

    defaults = {
        "claude": "claude-channel",
        "codex-cli": "codex-cli",
        "codex": "codex",
    }
    selected_agent = prompt_agent(defaults[action])
    if selected_agent is None:
        return 1
    selected_activity = prompt_activity()
    if selected_activity is None:
        return 1
    environment = resolved_environment(
        selected_agent,
        selected_activity or None,
        selected_url,
    )

    if action == "claude":
        permission_mode = select_option(
            "Claude permission mode",
            [("default", "Default"), ("auto", "Auto")],
        )
        if permission_mode is None:
            return 1
        permission_args = ["--permission-mode", "auto"] if permission_mode == "auto" else []
        return execute(
            [
                "claude",
                *permission_args,
                "--dangerously-load-development-channels",
                "server:agent-session-router-channel",
            ],
            environment,
            dry_run,
        )
    if action == "codex-cli":
        return execute(codex_cli_command(os.getcwd(), []), environment, dry_run)
    return execute(["bun", "run", "codex:interactive"], environment, dry_run)


def handle_profile_command(args: argparse.Namespace) -> int:
    try:
        if args.profile_command == "list":
            profiles, default_profile = read_router_profiles()
            for name in [default_profile, *sorted(item for item in profiles if item != default_profile)]:
                marker = "*" if name == default_profile else " "
                print(f"{marker} {name}\t{profiles[name]}")
            return 0
        if args.profile_command == "add":
            if args.dry_run:
                print(json.dumps({"profile": args.name, "routerUrl": args.address}, separators=(",", ":")))
                return 0
            save_router_profile(args.name, args.address, overwrite=args.force)
            print(f"Saved router profile: {args.name}")
            return 0
        if args.profile_command == "use":
            if args.dry_run:
                print(json.dumps({"defaultProfile": args.name}, separators=(",", ":")))
                return 0
            set_default_router_profile(args.name)
            print(f"Selected router profile: {args.name}")
            return 0
    except (OSError, RouterProfileError) as error:
        print(f"Router profile error: {error}", file=sys.stderr)
        return 2
    raise AssertionError(f"unhandled profile command: {args.profile_command}")


def doctor() -> int:
    missing = [name for name in ("bun", "codex", "python3") if shutil.which(name) is None]
    if missing:
        print(f"Missing commands: {', '.join(missing)}", file=sys.stderr)
        return 1
    print("Ready: bun, codex, and python3 are available.")
    if shutil.which("fzf") is None:
        print("Optional: fzf is unavailable; interactive asr will use numbered menus.")
    else:
        print("Optional: fzf is available for interactive asr menus.")
    return 0


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    args = parser().parse_args(normalize_codex_cli_options(raw_argv))

    if args.command is None:
        return interactive_launcher(args.dry_run)
    if args.command == "profile":
        return handle_profile_command(args)
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
