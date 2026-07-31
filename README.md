# agent-session-router

`agent-session-router` connects named Claude Code, Codex, and provider-neutral
agent sessions through one central WebSocket router. Each agent connects
outbound, selects a specific peer, and receives only the correlated reply.

The current implementation includes:

- targeted agent discovery, request/reply, timeout, and disconnect handling;
- heartbeat, reconnect, busy state, and response isolation;
- interactive Claude Code Channel and Codex CLI integrations;
- provider-neutral gateway and mock adapter tests;
- a launcher with loopback/LAN/tailnet router modes, profiles, and unique agent IDs.

## Quick start

Requirements:

- Bun 1.3 or newer;
- Python 3;
- an authenticated Claude Code or Codex CLI for provider runs;
- optional `fzf` for the interactive selector;
- optional Tailscale CLI only for Tailscale router mode.

```bash
bun install --frozen-lockfile
bun test
```

Load the short `asr` command into the current shell:

```bash
eval "$(python3 scripts/asr.py shell-init)"
asr doctor
```

Claude Code needs one repository-local Channel setup before its first run:

```bash
asr setup-claude
```

## Interactive `asr` launcher

Run `asr` without arguments:

```bash
asr
```

The launcher lets you:

1. start a loopback, LAN/tailnet router, Claude Code, or Codex;
2. select a saved router profile or add a router address;
3. enter a unique agent ID such as `reviewer` or `worker-a`;
4. publish an optional activity summary;
5. choose provider-specific options such as Claude Auto mode.

`fzf` is used when installed. Otherwise the launcher displays a numbered menu.
Short agent names are normalized automatically, for example `reviewer` becomes
`local:reviewer`. Two live sessions on the same router must use different IDs.

`Start router` asks for a mode and port. Loopback mode binds to `127.0.0.1`.
LAN mode accepts one private IP assigned to the machine. Tailscale mode checks
the optional `tailscale` CLI, obtains this machine's connected tailnet IPs, and
lets you select one. It does not install or configure Tailscale. Wildcard
bindings such as `0.0.0.0` are rejected. A router profile is instead the address
used by Claude or Codex to connect to an already running router.

## Router profiles

The built-in `local` profile points to `ws://127.0.0.1:8787/ws`. Selecting
`Add router address` accepts a host, `host:port`, `ws://` URL, or `wss://` URL.
A bare `host-a` value becomes `ws://host-a:8787/ws`.

Custom profiles are stored outside the repository at:

```text
~/.config/agent-session-router/config.json
```

Only router URLs and the last selected profile are stored. Authentication
tokens remain in `ROUTER_TOKEN` and are never written to the profile.

Profiles can also be managed explicitly:

```bash
asr profile add tailnet host-a:8787
asr profile list
asr profile use tailnet
```

For scripted provider runs, set `ROUTER_URL` directly:

```bash
ROUTER_URL=ws://host-a:8787/ws asr claude reviewer
ROUTER_URL=ws://host-a:8787/ws asr codex-cli worker-a
```

## Common commands

| Command | Purpose |
| --- | --- |
| `asr` | Open the interactive launcher |
| `asr router` | Start the loopback router |
| `asr router --host <private-ip> --port 8787` | Start a LAN/tailnet router |
| `asr router --tailscale --port 8787` | Discover and bind the preferred Tailscale IP |
| `asr claude reviewer` | Start Claude Code as `local:reviewer` |
| `asr codex-cli worker-a` | Start stock Codex CLI with router tools |
| `asr codex worker-a` | Start the prompt-capable Codex connector |
| `asr smoke` | Run a local router round trip |
| `asr test` | Run the automated test suite |

Claude Code receives router deliveries through its Channel and can use
`agent_list`, `agent_send`, and `agent_reply`. Stock Codex CLI exposes
`agent_list`, `agent_send`, `agent_wait`, and `agent_reply`; ask Codex to call
`agent_wait` when it should accept an inbound request.

## Network and security

The router binds to `127.0.0.1` by default. To accept connections from other
machines, choose LAN mode and enter one assigned private IP, choose Tailscale
mode on a machine with a connected Tailscale client, or keep loopback mode and
use an access-controlled tailnet TCP forwarder. Agent machines save the
reachable address as a router profile.

Set the same `ROUTER_TOKEN` on the router and provider connector processes when
using LAN/Tailnet mode. The launcher requires at least 16 printable characters
and prompts without echo when the variable is absent. Scripted host mode
requires `ROUTER_TOKEN` in the environment. Do not place tokens, real hostnames,
IP addresses, usernames, or environment-specific paths in this repository.

The router currently routes message text in memory and does not persist a
conversation transcript.

## Documentation

- [Architecture and protocol](docs/design.md)
- [Provider integration boundary](docs/provider-integration.md)
- [Claude Code Channel integration](docs/claude-channel-integration.md)
- [Codex integration](docs/codex-integration.md)
