# Codex integration

ASR supports stock Codex CLI and two lower-level App Server modes. Only stock
`codex-cli` is an invitation provider. For that path, use the administrator's
[copy-and-paste onboarding prompt](provider-integration.md#invitation-onboarding):
it installs ASR even if absent and generates a private scoped credential locally,
without asking the recipient to copy a long-lived credential.

The client must already have Codex CLI with stdio MCP add and JSON MCP get/list
support and skill discovery. Its own provider login/model configuration remains
separate. The native installer will not install system packages, change Codex
login, or bypass shell approval.

Router collaboration requires membership selected by the invitation binding,
`--workspace ROOM`, or an explicit MCP `workspace_join({"name":"ROOM"})`.
Without a selected/joined workspace, local work has no workspace transcript or
router delivery. The lower-level explicit-credential command forms are:

```sh
asr --profile local --credential "$HOME/.config/agent-session-router/codex.json" \
  codex-cli local:codex --workspace team-room
asr --profile local --credential "$HOME/.config/agent-session-router/codex.json" \
  codex local:codex --workspace team-room
asr --profile local --credential "$HOME/.config/agent-session-router/codex.json" \
  gateway codex local:codex --workspace team-room
```

The first command runs the installed stock Codex CLI. The second starts an
interactive Codex App Server session owned by ASR. The third starts a managed
Codex App Server provider subprocess. The managed modes use the caller's working
directory and close the provider child when the ASR host is interrupted.

## Manual credentials and workspace

Invitation installation already supplies a `codex-cli` credential/profile binding.
For manual or managed modes only, issue a credential whose claims match the mode:

```sh
asr --profile local credential issue \
  --agent local:codex --side codex --client codex-cli \
  --workspace team-room --output "$HOME/.config/agent-session-router/codex.json"
```

Use `codex-app-server` instead of `codex-cli` when the credential is for
`codex` or `gateway codex`. A credential mismatch is rejected before a provider
child is launched. `--workspace team-room` joins that room at startup; without
it or a configured startup binding, delivery waits for an explicit join to an
authorized workspace. Stock Codex can call `workspace_join({"name":"team-room"})`
through MCP, and interactive `asr codex` supports `/workspace join ROOM`.

Codex CLI mode uses an explicit MCP pull boundary. Router task events do not
become unsolicited CLI turns, and an ordinary workspace message does not wake a
busy provider. The provider must inspect workspace and task tools, request work,
and explicitly begin its assigned attempt.

## Stock Codex CLI

Invitation installation copies the packaged canonical skill to
`$HOME/.agents/skills/asr/SKILL.md` without replacing user-owned contents. It
registers exactly one stdio MCP entry named `agent_session_router` using this
command form with the actual installed absolute ASR path:

```text
codex mcp add agent_session_router -- ABSOLUTE_ASR --profile office mcp codex-cli
```

This describes installer-owned registration, not a second manual setup step.
The profile binding loads the credential, CA, and invited workspace. The native
definition is pinned to one explicit profile for this provider; neither a new
invitation nor another project may silently replace it. The stock wrapper uses
the same server name, not a duplicate MCP connection.

ASR inspects `codex mcp get agent_session_router --json` (and a structured list
when needed). Exact ownership includes the absolute command/profile arguments,
enabled stdio transport, and absence of unexpected environment, working-directory,
or tool overrides. A same-named or identical-looking entry without ASR's recorded
intent is a conflict, not permission to adopt or overwrite it. Use the reported
resume command for the same invitation after deliberately resolving any conflict:

```sh
asr onboarding resume INVITE_ID --provider codex-cli
asr --profile office onboarding status --provider codex-cli --json
```

Use the installer's absolute-ASR command if that executable is not on PATH.
Resume preserves the same pending credential and exact recorded steps. See
[installation ownership](provider-integration.md#private-state-ownership-and-resume).

Installation reports `configured` / `restart_required`; it does not demonstrate
that the current session loaded MCP, joined, or executed a model. Follow
`nextAction` and start a new normal `codex` session. There is no assumed live MCP
reload. Select the skill through `/skills`, or invoke it as:

```text
$asr workspace list
$asr workspace find team
$asr workspace join team-room
$asr workspace members
$asr workspace history
$asr workspace post Hello team
$asr workspace status
$asr workspace leave
```

`/asr` is not claimed as a native Codex command. These skill operations use the
provider's actual MCP connection. List/find traverse grant-filtered pages capped
at 100; find matches names by ASCII case-insensitive substring. History is bounded
and uses returned cursors for further pages. Status must confirm this session's
own identity with MCP list/members, not an operator CLI connection or installer
reachability check. Confirm leave before switching rooms, preserving pending-work
and unconfirmed-stop fences.

The lower-level `asr codex-cli` wrapper preserves installed CLI arguments after
`--`; ASR consumes only its own flags. Forward resume arguments only in the
syntax supported by the installed Codex CLI. Review the durable task checkpoint
first; chat history does not reconstruct a provider session.

## Owned interactive Codex

`asr codex` runs an interactive App Server thread and connects it to terminal
input. Ordinary input is local provider work; router delivery remains fenced by
workspace join, readiness, and task state. Its child receives a filtered
environment and never receives router bearer values through legacy token
variables.

The process returns a terminal status on EOF or interruption. Process exit does
not complete a task; checkpoint and pause/complete through the task lifecycle,
or use the interrupted-attempt recovery described below.

## Managed Codex gateway

`asr gateway codex` starts the App Server child under native supervision.

Neither this gateway nor interactive `asr codex` accepts `codex-cli` invitation
credentials as an App Server identity. Provision the `codex-app-server` client
separately; these modes are not additional invitation provider values:

```sh
asr --profile local --credential "$HOME/.config/agent-session-router/codex.json" \
  gateway codex local:codex --workspace team-room
```

The gateway uses the current working directory and passes only the App Server
arguments needed for the managed protocol. The managed child is terminated on
router interruption; the router does not assume a clean task completion from
process termination.

## Task lifecycle from Codex

The production MCP catalog exposes the native lifecycle:

```text
workspace_join
task_list / task_get / task_history
task_request
task_begin
task_checkpoint
task_pause or task_complete
```

`task_checkpoint` stores summary, next steps, artifacts, and risks. Expected task
versions and operation IDs fence concurrent updates. The assignee is the intended
responsible agent, not proof of an executing attempt. Assignment, request
acceptance, the actual executor/session's `task_begin`, and completion are distinct.
A chat message or response sent to another agent is not task execution or completion.

An interrupted attempt initially has unknown stop evidence. Assignment to a
replacement is allowed, but new `task_request` and `task_begin` operations remain
fenced until stop confirmation. Managed hosts can automatically confirm exact
terminal/reap evidence. Read `task get` again; only if evidence is still unknown
and the operator has observed the actual execution stop should the operator use
`task confirm-stopped` with the exact interrupted attempt and freshly read task
version. The replacement uses a new Codex session and reviews the checkpoint.

## Router profiles and network trust

Manual profiles store a router address. Invitation profiles additionally keep
server identity, verified routes/CA-file references, and provider bindings, never
bearer values. Built-in `local` already points to
`ws://127.0.0.1:8787/ws` and cannot be added or replaced:

```sh
asr --profile local workspace list --json
```

Loopback is the default. Remote routers require WSS and a scoped Codex credential;
configure `ASR_CA_FILE` when a private CA is needed. Remote operator commands also
require `--credential FILE` or `ASR_CREDENTIAL_FILE`.

`router --share=lan` requires nonloopback `ASR_BIND` plus `ROUTER_TLS_CERT`,
`ROUTER_TLS_KEY`, and `ROUTER_PUBLIC_URL` (WSS, `/ws`, matching bind port).
Explicit Tailscale sharing requires loopback and no ASR TLS. Auto sharing selects
local or LAN by bind when TLS is configured; otherwise it selects valid Tailscale
or local on loopback, never plaintext LAN. See the
[README transport examples](../README.md#profiles-remote-routers-and-tls).
ASR does not disable certificate verification or place router bearer values in
Codex provider environment variables.

The invitation's Tailnet → LAN → public candidates must already route to the
same server. The installer does not provision a public endpoint, certificates,
Tailscale membership, or firewall/ACL changes. Trust/identity failures stop the
flow rather than disabling verification; local-only invitations do not work
cross-device. See the [onboarding network contract](provider-integration.md#network-and-trust).
