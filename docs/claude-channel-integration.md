# Claude Code Channel integration

Claude Channel is an optional, explicitly enabled stock-Claude path. It is not
the managed Claude SDK gateway and it is not a native AgentBridge adapter.
For new stock installations, use invitation provider `claude-code` and the
[copy-and-paste onboarding flow](provider-integration.md#invitation-onboarding).
The generated prompt installs ASR without requiring a preinstalled binary or a
manually copied long-lived credential. It installs the `asr@asr-local` user
plugin and one user-scoped stdio MCP entry named `agent-session-router-channel`;
the plugin itself does not register another MCP server.

After installation, follow the reported `nextAction`, using its actual absolute
ASR executable. The command form is:

```text
ABSOLUTE_ASR --profile office claude --workspace team-room
```

The profile binding supplies the enrolled identity, credential file, and
workspace. `configured` with `restart_required` records installation, not current
MCP or Channel activation. Restart Claude Code and confirm this session's actual
MCP membership with `/asr:workspace status`; skill availability alone is not proof
of Channel push or successful model execution.

For a distinct manually provisioned host, the retained lower-level commands are:

```sh
asr setup-claude
asr --profile local --credential "$HOME/.config/agent-session-router/claude-code.json" \
  claude local:claude --workspace team-room
```

`setup-claude` installs the local MCP registration for that manual path, not the
invitation plugin/marketplace. Do not run it as an additional onboarding step.
Claude Code must be restarted after setup or a policy change. See
[stock Claude installation](claude-integration.md#stock-claude-code-and-channel)
for the exact user-scoped marketplace, plugin, and MCP commands the installer owns.

## Opt-in and policy

Claude Channels are a research-preview surface. Custom Channels require
explicit development opt-in, and organization policy may disable them. Obtain
approval for the development/organization Channel before using this adapter; ASR
does not attempt to bypass account policy.

The native launch uses
`--dangerously-load-development-channels server:agent-session-router-channel`.
This is the Claude Code development-Channel opt-in, not
`--dangerously-skip-permissions`, and it does not select
`bypassPermissions`. Use it only with a trusted ASR MCP registration: the
invitation installer's user-scoped entry, or the separately provisioned local
registration installed by `setup-claude`. ASR does not opt into organization
policy or change provider authentication on your behalf.

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
`ROUTER_URL`, then the saved default profile, then built-in `local`. Manually
provisioned credentials come from `--credential FILE` or `ASR_CREDENTIAL_FILE`;
an onboarded host uses its explicit native profile binding, never a shared token.
If an old Claude Code release does not advertise or accept the Channel
capability, upgrade it or use `asr gateway claude`; do not silently downgrade
security or inject task text into chat.

## Workspace and readiness

Channel initialization is not router membership. The credential must identify
the Claude Code client and grant the room. An invitation binding selects the
initial workspace; manual hosts use `--workspace ROOM` at startup or the actual
MCP tool `workspace_join({"name":"ROOM"})` after launch. With no selected/joined
workspace, Claude can run local work but receives no workspace delivery. The MCP
process registers only after initialization and readiness; closing it removes
the agent registration. An operator's separate `workspace join` does not join
Claude.

`/asr:workspace list|find QUERY|join NAME|members|history|post TEXT|leave|status`
uses that same MCP connection. List/find traverse bounded accessible pages;
members reports connected identities and history uses bounded cursor pages.
Verify the provider's own exact identity, not another member, before reporting
participation. Confirm `workspace_leave({})` before joining a different room,
and preserve pending/running-work and unconfirmed-stop fences.

The explicit credential issuance example in
[manual Claude setup](claude-integration.md#manual-credentials-and-setup) remains
available for a manual host; it is not a step invitation recipients perform.

Durable task events are delivered only after the joined-ready boundary. A
workspace chat message does not wake Claude, and the Channel adapter never starts
a second concurrent request while one targeted request is pending. Claude must
inspect task state, call `task_begin`, checkpoint progress, and explicitly pause
or complete.

Task assignee and actual executor/session are separate. Assignment does not
request work; request acceptance does not establish `task_begin`, and chat,
readiness, or a reply does not complete a task.

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

## Host ownership and restart boundaries

The invitation installer refuses unknown existing marketplace, plugin/cache, and
MCP definitions. Exact executable/profile/scope/content plus its own recorded
intent are required to resume; a same-named or healthy entry is insufficient.
Use the reported `onboarding resume INVITE_ID --provider claude-code` command
after resolving a conflict deliberately. Do not register a second MCP server or
delete the private journal to retry. Status only checks recorded installation
state and tokenless transport reachability, not the running Claude conversation.

OMP is a separate invitation provider with its own linked package and private
startup descriptor. Restart OMP after its installation; restarting ASR or
reloading an OMP command does not reload the plugin registry. See
[stock OMP startup](provider-integration.md#stock-omp-startup-and-workspace-commands).

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

Invitation routes must already be configured and reachable; the prompt cannot
create a public endpoint, Tailscale membership, or TLS/ACL exceptions. Use only
the supplied candidates, and stop on trust or server-identity mismatches. A
local-only invitation is not usable on another machine. See the
[onboarding network contract](provider-integration.md#network-and-trust).

Claude provider secrets stay with Claude Code. Router credentials and private
integration token files are not placed in Channel notification content or child
environment variables.
