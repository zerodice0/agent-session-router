# Claude Code Channel integration

Claude Channel is an optional, explicitly enabled stock-Claude path. It is not
the managed Claude gateway and it is not a native AgentBridge adapter. The
current host command is:

```sh
asr setup-claude
asr --profile local --credential "$HOME/.config/agent-session-router/claude-code.json" \
  claude local:claude --workspace team-room
```

The first command installs the local stdio MCP registration. The second starts
the installed Claude Code host and passes the Channel development flag used by
the native launcher. Claude Code must be restarted after setup or any policy
change.

## Opt-in and policy

Claude Channels are a research-preview surface. Custom Channels require
explicit development opt-in, and organization policy may disable them. Obtain
approval for the development/organization Channel before using this adapter; ASR
does not attempt to bypass account policy.

The native launch uses
`--dangerously-load-development-channels server:agent-session-router-channel`.
This is the Claude Code development-Channel opt-in, not
`--dangerously-skip-permissions`, and it does not select
`bypassPermissions`. Use it only with the trusted repository-local MCP
registration installed by `setup-claude`.

Relevant upstream documentation:

- [Push events into a running session with Channels](https://code.claude.com/docs/en/channels)
- [Channels reference](https://code.claude.com/docs/en/channels-reference)
- [Claude Code MCP configuration](https://code.claude.com/docs/en/mcp)
- [Claude Code authentication](https://code.claude.com/docs/en/iam)
- [Claude Code permission modes](https://code.claude.com/docs/en/permission-modes)

## MCP negotiation and legacy clients

The native server advertises the experimental `claude/channel` capability only
for the `claude-channel` MCP role. Channel delivery uses the
`notifications/claude/channel` notification and correlates `request_id`, sender,
and timeout metadata with exactly one `agent_reply`. A reply is not a task
completion.

MCP initialization remains protocol-negotiated. An older official MCP client can
use the ordinary tool negotiation path, but Channel-specific notifications
require the `claude/channel` capability and current Channel behavior. There is
no legacy `ROUTER_TOKEN` authentication, Python launcher, or shared-token
negotiation fallback. `ROUTER_URL` remains supported as non-secret endpoint
configuration: URL precedence is explicit `--profile`, then nonempty
`ROUTER_URL`, then the saved default profile, then built-in `local`. Credentials
come from `--credential FILE` or `ASR_CREDENTIAL_FILE`, not from a shared token.
If an old Claude Code release does not advertise or accept the Channel
capability, upgrade it or use `asr gateway claude`; do not silently downgrade
security or inject task text into chat.

## Workspace and readiness

Channel initialization is not router membership. For router delivery, the
credential must identify the Claude Code client and grant the selected workspace,
and the provider must explicitly join it with `--workspace ROOM` at startup or
`workspace_join` after launch. Until joined, Claude may run a local provider
session but the MCP process receives no workspace delivery. The MCP process
registers only after initialization and readiness; closing it removes the agent
registration. An operator's separate `workspace join` does not join Claude.

```sh
asr --profile local credential issue \
  --agent local:claude --side claude --client claude-code \
  --workspace team-room --output "$HOME/.config/agent-session-router/claude-code.json"
asr --profile local workspace join team-room
```

Durable task events are delivered only after the joined-ready boundary. A
workspace chat message does not wake Claude, and the Channel adapter never starts
a second concurrent request while one targeted request is pending. Claude must
inspect task state, call `task_begin`, checkpoint progress, and explicitly pause
or complete.

The Channel MCP tool names are prefixed for Claude Code:

```text
mcp__agent_session_router__workspace_join
mcp__agent_session_router__task_list
mcp__agent_session_router__task_begin
mcp__agent_session_router__task_checkpoint
mcp__agent_session_router__task_pause
mcp__agent_session_router__task_complete
mcp__agent_session_router__agent_reply
```

`agent_reply` requires the current targeted request and can be accepted only once.
It does not mutate task state. Task state uses expected versions and operation IDs.

## Stop and resume handoff

`task interrupt` requests a stop but initially leaves stop evidence unknown.
Assignment to a new executor is allowed, but new `task_request` and `task_begin`
operations remain fenced until stop confirmation. Managed hosts can
automatically confirm exact terminal/reap evidence; interruption alone is not
proof of a stop. Read `task get` again; only if evidence remains unknown and the
operator has observed the actual execution stop should the operator run
`task confirm-stopped` with the exact interrupted attempt and freshly read task
version. The replacement Claude session reviews the checkpoint and must not
reuse an unconfirmed process.

Stock Claude may resume an explicit provider session:

```sh
asr --profile local --credential "$HOME/.config/agent-session-router/claude-code.json" \
  claude local:claude --workspace team-room --resume SESSION_ID
```

Review the durable checkpoint before using `--resume`. ASR does not reconstruct
provider context from chat history.

## OMP restart boundary

OMP is a separate optional host and does not share Claude Channel's MCP process.
If its integration is installed or changed, run:

```sh
asr setup-omp
```

Enable the linked `@agent-session-router/omp-integration` package and restart OMP.
Restarting only ASR does not reload OMP's plugin registry. This boundary is
intentional: Claude Channel setup and OMP setup are independent, and neither is
a legacy launcher alias.

## Security and trust

The built-in `local` profile uses loopback and cannot be added or replaced.
For remote use, select a WSS profile and scoped credential, with `ASR_CA_FILE`
for a private CA when needed. Remote operator commands require `--credential FILE`
or `ASR_CREDENTIAL_FILE`.

`router --share=lan` requires nonloopback `ASR_BIND` and all of `ROUTER_TLS_CERT`,
`ROUTER_TLS_KEY`, and `ROUTER_PUBLIC_URL` (WSS, `/ws`, matching bind port).
Explicit Tailscale sharing requires loopback and no ASR TLS. With TLS configured,
auto sharing selects local or LAN by bind; otherwise it selects valid Tailscale
or local on loopback, never plaintext LAN. See the
[README transport examples](../README.md#profiles-remote-routers-and-tls).
TLS verification is not disabled to make Channel startup succeed.

Claude provider secrets stay with Claude Code. Router credentials and private
integration token files are not placed in Channel notification content or child
environment variables.
