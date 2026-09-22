# Claude integration

ASR has two Claude paths:

- Stock Claude Code uses invitation provider `claude-code`. The installer
  configures a local plugin and the Claude Channel MCP boundary; the
  `asr claude` launcher supplies the development-Channel opt-in.
- `asr gateway claude` owns a managed Claude bridge using the retained Node
  Claude SDK package. This is a lower-level compatibility mode, not an
  invitation provider.

For the stock path, use the administrator-generated
[invitation onboarding prompt](provider-integration.md#invitation-onboarding).
It installs ASR even when ASR is absent, registers the selected provider, and
creates a private workspace-scoped credential locally. Do not manually copy a
long-lived credential or run a second setup flow after invitation installation.
It requires an existing Claude Code CLI supporting user-scoped local
marketplaces, plugin install/list and marketplace list JSON inspection, and
stdio MCP add/get. Provider login/model access remains Claude Code's own setup.

Both paths need explicit workspace membership for router collaboration and task
delivery. An invitation binding selects the initial workspace; manually
provisioned hosts can use `--workspace ROOM`, or stock Claude can use
`workspace_join({"name":"ROOM"})` through MCP. Without a selected/joined workspace,
local provider work receives no router transcript or task delivery. The
lower-level explicit-credential command forms are:

```sh
asr --profile local --credential "$HOME/.config/agent-session-router/claude-code.json" \
  claude local:claude --workspace team-room
asr --profile local --credential "$HOME/.config/agent-session-router/claude-sdk.json" \
  gateway claude local:claude --workspace team-room
```

## Stock Claude Code and Channel

Invitation installation adds the packaged local marketplace `asr-local` and the
user-scoped plugin `asr@asr-local`. The plugin supplies `/asr:workspace`, not a
second MCP server. Its package/skill assets come from the native bundle and its
version follows Cargo. The installer uses these native host operations, with
the actual installed absolute paths:

```text
claude plugin marketplace add INSTALLED_CLAUDE_PLUGIN_DIR --scope user
claude plugin install asr@asr-local --scope user
claude mcp add --transport stdio --scope user agent-session-router-channel -- ABSOLUTE_ASR --profile office mcp claude-channel
```

These describe installer-owned definitions, not extra setup commands to repeat.
The fixed MCP name selects one explicit profile for this provider. Its credential,
CA, and workspace come from that profile binding. Unknown existing marketplace,
plugin/cache, or MCP definitions are not overwritten, even if their names look
right; only the exact journal-owned installation can resume. See
[ownership and resume](provider-integration.md#private-state-ownership-and-resume).

`configured` / `restart_required` is not proof that this conversation has loaded
the plugin, MCP connection, Channel, or a working model. Follow the installer's
`nextAction`: restart using its absolute-ASR command, whose form is:

```text
ABSOLUTE_ASR --profile office claude --workspace team-room
```

This launcher passes
`--dangerously-load-development-channels server:agent-session-router-channel`.
Applicable account/organization policy must allow Channels; this is not a
permission-bypass flag. Ordinary MCP workspace tools and the plugin skill are
distinct from Channel push/autonomous execution. See
[Claude Channel integration](claude-channel-integration.md) for that boundary.

In the new Claude Code session:

```text
/asr:workspace list
/asr:workspace find team
/asr:workspace join team-room
/asr:workspace members
/asr:workspace history
/asr:workspace post Hello team
/asr:workspace status
/asr:workspace leave
```

The skill uses this session's actual MCP tools. List/find traverse accessible
bounded pages; history is bounded and cursor-based. Status must confirm the
provider's own exact identity in the invited room, not an operator's connection.
Leave must be confirmed before switching rooms, and active-work/stop fences
remain in force. Ordinary workspace chat does not wake a Claude turn. Claude
must inspect task state and explicitly call lifecycle tools.

For a separate manually provisioned installation, `asr setup-claude` remains the
local MCP setup command; it is not the invitation plugin installer. Obtain the
required Channel opt-in and restart the host after that setup as well.

Claude Code's provider resume option is explicit:

```sh
asr --profile local --credential "$HOME/.config/agent-session-router/claude-code.json" \
  claude local:claude --workspace team-room --resume SESSION_ID
```

Review the durable task checkpoint before resuming. If a prior attempt was
interrupted, do not reuse it until stop evidence is confirmed.

## Managed Claude gateway

The gateway owns the Claude child through the packaged Node bridge:

```sh
asr --profile local --credential "$HOME/.config/agent-session-router/claude-sdk.json" \
  gateway claude local:claude --workspace team-room
```

Node is required for this mode only. The release archive must contain
`share/agent-session-router/integrations/claude-sdk/bridge.js`, its manifest, and
its packaged Claude SDK dependencies. The native executable resolves this
installed asset layout; it does not search a source checkout.

The gateway filters provider environment variables, keeps router credentials out
of the child, and terminates the child when the host is interrupted. The current
gateway command has no implicit chat-resume behavior; start a new managed session
after reviewing the checkpoint and use an explicit task transition.

## Manual credentials and setup

The invitation flow provisions its own `claude-code` binding; the following is
only for manually provisioned stock hosts or the managed SDK route. Issue
separate credentials for those clients:

```sh
asr --profile local credential issue \
  --agent local:claude-code --side claude --client claude-code \
  --workspace team-room --output "$HOME/.config/agent-session-router/claude-code.json"
asr --profile local credential issue \
  --agent local:claude-sdk --side claude --client claude-sdk \
  --workspace team-room --output "$HOME/.config/agent-session-router/claude-sdk.json"
```

Credential files are private JSON. A `claude-code` credential is rejected for
`gateway claude`, and vice versa. Do not put bearer values in `ROUTER_TOKEN`,
`AGENT_ROUTER_TOKEN`, or provider prompts.

## Task lifecycle and handoff

The Claude MCP catalog exposes:

```text
workspace_join
task_list / task_get / task_history
task_request
task_begin
task_checkpoint
task_pause or task_complete
```

Assignment names the intended responsible agent; it does not start execution.
A request, the actual executor/session's `task_begin`, and completion are
separate transitions. The executor records summary, next steps, artifacts, and
risks in `task_checkpoint`, then explicitly pauses or completes. A Claude reply,
chat message, installation result, or readiness event is not a task transition.

`task interrupt` requests a stop but initially leaves stop evidence unknown.
Assignment to a replacement is allowed while evidence is unknown, but new
`task_request` and `task_begin` operations remain fenced. Managed hosts can
automatically confirm matching terminal/reap evidence. Read `task get` again;
only if evidence is still unknown and the operator has observed the real
execution stop should the operator call `task confirm-stopped` with the exact
interrupted attempt and freshly read task version. A replacement Claude session
reviews the checkpoint and uses an authorized identity; it never shares an
unconfirmed provider process.

## Optional external work tracking

GitHub and Linear access is configured by the router administrator per workspace
with private token files. Claude does not log into a personal backlog. Operators
explicitly choose any import, link, or publish operation and resolve an
unconfirmed external write before retrying.

## Trust and networking

Local Claude sessions use the built-in `local` loopback profile; do not add or
replace it. Remote operation uses WSS and scoped credentials, with `ASR_CA_FILE`
for a private CA when needed. Remote operator commands require `--credential FILE`
or `ASR_CREDENTIAL_FILE`.

`router --share=lan` requires nonloopback `ASR_BIND` and all of `ROUTER_TLS_CERT`,
`ROUTER_TLS_KEY`, and `ROUTER_PUBLIC_URL` (WSS, `/ws`, matching bind port).
Explicit Tailscale sharing requires loopback and no ASR TLS. With TLS configured,
auto sharing selects local or LAN by bind; without TLS it selects valid Tailscale
or local on loopback, never plaintext LAN. See the
[README transport examples](../README.md#profiles-remote-routers-and-tls).
Certificate verification is never disabled to make Claude connect.

Invitation candidates must already be reachable routes to the same server.
Only advertised/supplied Tailnet → LAN → public candidates are tried, and trust
or server-identity mismatches stop the flow. Local-only invitations are not
remote access. Installation does not create TLS certificates, public tunnels,
Tailscale membership, or firewall/ACL exceptions. See the
[invitation network contract](provider-integration.md#network-and-trust).
