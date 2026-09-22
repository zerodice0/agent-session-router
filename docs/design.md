# Native v2 design

Agent Session Router is a native Rust process whose authenticated WebSocket
protocol is the durable coordination boundary. The `asr` executable starts the
router, runs operator and agent clients, owns provider subprocesses in managed
modes, and exposes production MCP over stdio. A bounded HTTP bootstrap surface
supports enrollment and deployment assets on the same listener; it is not an
HTTP administration API. There is no TypeScript or Python router runtime or web
dashboard.

## Runtime boundaries

```text
asr router ── loopback/TLS WebSocket ── operator and provider clients
     │
     ├── SQLite state: credentials, invitations, workspaces, tasks, attempts, events
     ├── bootstrap HTTP: public info/files and one-use enrollment
     ├── full-screen operator client: authenticated WebSocket, not direct DB access
     ├── native MCP server: workspace/task/integration tools
     └── optional provider subprocess
           ├── stock Codex CLI or Claude Code (MCP boundary)
           ├── owned or managed Codex App Server
           ├── packaged Node Claude bridge
           └── external OMP host and linked package
```

The default router endpoint is `ws://127.0.0.1:8787/ws`. The `local` profile is
built in and cannot be added or replaced. Manual profiles contain a router
address; onboarded profiles additionally store persistent server identity,
verified route/CA-file metadata and per-provider workspace/credential-file
bindings. They never embed credential tokens.

Ordinary address selection prefers explicit `--profile`, then `ROUTER_URL`, then
the saved default profile, then built-in `local`. `ROUTER_URL` is a non-secret
address override. Authentication uses an operator credential or an agent
credential with the requested workspace grant. Remote operator clients require
`--credential PATH` or `ASR_CREDENTIAL_FILE`; automatic operator credential
discovery is limited to the owned local router. Explicit credential selection
takes precedence over the environment and is never upgraded to local admin
authority. Credential files are private JSON files; no shared `ROUTER_TOKEN`
environment contract exists.

The default data directory follows XDG conventions and can be overridden with
`ASR_DATA_DIR`; configuration can be overridden with `ASR_CONFIG_PATH`. The
router's local state is not a model prompt and is not an implicit chat transcript.

### Owned server and console lifecycle

With terminal stdin/stdout and a usable `TERM`, ordinary `asr router start`
starts/reuses the owned router and opens the full-screen console. The launcher
detaches its server child before entering the UI: server ownership and console
connection lifetime are distinct. `--background` starts/reuses and returns without
a UI. `--no-ui` preserves the foreground headless lifecycle and is mutually
exclusive with `--background`; Ctrl-C is forwarded to the owned child, which is
terminated/reaped before owned Serve/runtime cleanup and exit code 130. Non-TTY
startup never enters alternate-screen mode and stays foreground unless background
was requested. Reusing an existing router does not create a new foreground child.
The numbered menu routes start (1) and reattach (7) through these same paths.

`asr ui --workspace ROOM` only reattaches. With explicit `--profile` or
`ROUTER_URL`, it honors ordinary endpoint selection. Otherwise it reads the owned
runtime's control URL rather than following a saved default remote profile.
Credentials resolve as explicit `--credential`, then `ASR_CREDENTIAL_FILE`, then
the owned admin file. It requires operator credentials; provider bindings cannot
serve as console authority. A runtime record/health marker must match the exact
instance, and the authenticated registration must report admin authority before
owned-admin actions are enabled. File claims, a local URL, or a reused runtime
record alone are insufficient.

`q` in navigation or Ctrl-C detaches, not stops. The pending-request confirmation
requires `DETACH` and Ctrl-S; Esc preserves the connection. Disconnecting this
requester can trigger `RequesterDisconnected`, interrupt its task attempt, and
send `CancelWork`, without stopping the router or unrelated requesters' work.
Ctrl-X is a separate `STOP` confirmation restricted to a verified owned-admin
instance. After terminal restoration, the app calls the existing launcher's
instance-fenced stop path, including owned Tailscale Serve cleanup. A remote or
scoped console cannot use it. UI failure or a signal does not implicitly stop the
server.

## Copy/paste onboarding and deployment

`onboarding prompt --workspace ROOM --name PROFILE` operates through the owned
server's authenticated management connection, not an arbitrary saved profile or
`ROUTER_URL`. Only admin-authorized protocol operations issue/revoke invitations
or create workspaces; `--create-workspace` permits creation in the invitation
transaction. `--provider` optionally restricts the invitation to `claude-code`,
`codex-cli`, or `omp`. Routes, public CA material, assets, and rendered ticket size
are validated before issuing the invitation. F4 in an owned-admin console uses
the same issuer for its selected workspace; it does not issue on redraw.

The version-1 camelCase ticket carries `serverId`, `inviteId`, `inviteToken`,
`expiresAt`, `profileName`, `workspace`, optional `provider`, `routes`,
`manifestSha256`, and the available target artifacts with filenames, byte sizes,
and hashes. Unknown fields are rejected and ticket stdin is bounded at 128 KiB.
The persistent SQLite `serverId` survives restarts and is distinct from the
launcher's per-process `instanceId`. The server derives the provider subject and
single-workspace grant; enrollment cannot request its own role or extra grants.

Invitations expire ten minutes after server issuance and enroll one device/provider
identity. Before exchange, the client fsyncs its generated credential and
enrollment ID in a private journal. A serialized store transaction atomically
consumes the invitation and inserts only the credential hash. The same enrollment
ID, provider, credential ID and token hash can recover the same public claims
after a lost response, including after the consumed invitation expires; a
different consumer cannot. Revoked invitations/credentials cannot resume.
`onboarding resume INVITE_ID --provider PROVIDER` continues this journal rather
than generating new secrets. `onboarding revoke` does not revoke an already
enrolled credential; existing credential revocation is separate.

### Bounded bootstrap surface and route trust

The existing listener exposes only these onboarding HTTP paths:

- `GET /onboarding/info`: version, persistent server identity, manifest digest and
  available target IDs; no invitation or workspace listing.
- `GET /onboarding/files/{name}`: manifest and allowlisted raw binary/archive
  basenames from the verified bundle, not an arbitrary filesystem path.
- `POST /onboarding/enroll`: bounded JSON exchange through the router actor/store,
  not a second database owner. It rejects browser Origin requests, uses no-store
  responses, redacted errors, concurrency/rate limits, and bounded actor dispatch.

Assets default to `$ASR_DATA_DIR/bootstrap`, overridable with `ASR_BOOTSTRAP_DIR`.
Missing assets leave the router and info endpoint usable but prevent invitation
generation before issuance. Startup verifies ownership/file types, manifest
digest, file hashes and sizes. Validated file handles are retained; asset changes
stop serving rather than silently changing the pinned content. Clients verify
raw executable, manifest and archive before execution/enrollment and reject
unsafe archive paths or links.

The release workflow builds each native host target and aggregates
`asr-bootstrap-bundle` as an Actions artifact; it does not promise a public
download service. `scripts/package.ts` derives versions from `Cargo.toml` and
packages all provider skill variants from `integrations/skills/asr/SKILL.md`,
plus the skill-only Claude plugin, OMP package and managed Claude bridge.
`bun scripts/bootstrap-manifest.ts --dir DIRECTORY` verifies staged raw binaries,
archives and checksums, including raw/archive binary equality, and emits a
deterministic manifest. It does not build or cross-compile missing targets.
Supported IDs are `aarch64-apple-darwin`, `x86_64-apple-darwin`,
`aarch64-unknown-linux-gnu`, and `x86_64-unknown-linux-gnu`; only the subset actually
present can install. Host-native managed bridge assets must match their archive
architecture. Unsupported hosts or missing provider CLIs fail with prerequisites,
not automatic package installation or security-policy changes.

By default, the prompt derives candidates from the owned advertised/control URL.
Explicit repeated `--endpoint KIND=URL` replaces that set with aliases already
configured to reach this same server. It creates no listener, tunnel or ACL.
`--ca-file` takes precedence over `ASR_CA_FILE` and accepts public CA PEM, never
private keys. Clients probe supplied Tailnet → LAN → public routes and compare
server identity before sending enrollment secrets. Unreachable routes can fall
through; TLS, identity or digest mismatch fails closed. Local-only tickets must
remain loopback-only and cannot mix with remote routes. Tailnet plaintext WebSocket
is accepted only with actual Tailscale peer/Serve validation; LAN/public require
WSS and hostname/CA validation. Redirects, TLS bypass and guessed endpoints are
not fallback mechanisms.

The selected route and verified alternatives are saved. New provider MCP
connections reselect a valid route; live work is not migrated between sockets.
The invitation is the trust handoff and contains a redeemable short-lived secret,
so even public hashes/CA certificates do not make an untrusted prompt safe.
Tokens never belong in argv, environment, URLs, ordinary diagnostics or workspace
events. Only explicit prompt output/copy and the private pending journal retain
invitation material.

### Host definition ownership and activation

The installer checks provider availability, capabilities, permissions and
configuration conflicts before enrollment. Every effect is journaled with an exact
intent, inspected, then marked applied. Existing definitions are not adopted just
because their display name or even command happens to match: reuse requires this
journal's ownership and exact expected definition. Different paths, arguments,
scope, environment or unrelated files conflict rather than being overwritten.

- Claude uses the `asr-local` marketplace's skill-only `asr` plugin and one
  user-scoped `agent-session-router-channel` stdio definition pointing to the
  installed absolute binary plus `--profile PROFILE mcp claude-channel`. The
  plugin must not embed a competing MCP server.
- Codex installs the canonical skill at `~/.agents/skills/asr/SKILL.md` and one
  `agent_session_router` definition invoking the installed absolute binary plus
  `--profile PROFILE mcp codex-cli`.
- OMP links/enables `@agent-session-router/omp-integration` at user scope and stores
  a private host descriptor containing the exact executable, profile and workspace.
  The package owns its native MCP connection; no second generic MCP definition is
  registered. Unrelated link destinations are never removed to make room.

Profile/provider bindings reference a private credential file; definitions contain
no bearer. A configured bundle can support normal provider startup and the
existing wrappers without creating duplicate identity registrations.
`onboarding status --provider PROVIDER --json` inspects the binding's authoritative
journal, bundle/managed-file integrity and route reachability, but does not invoke
provider registry commands or start a health MCP child. Its allowlisted report
includes `stage`, optional `transport`, `activation` and `nextAction`.
`configured` / `restart_required` is an installation result, not activation;
incomplete installation reports `not_checked`. Actual provider MCP
`workspace_list`/`workspace_members` must confirm the host identity after the
normal restart. Model authentication, Channel opt-in, readiness and task execution
are separate capabilities; neither installation nor chat proves them.

## Protocol and identities

Protocol version 2 validates every registration and request at the boundary.
Agent identity includes an agent ID, side (`claude`, `codex`, or `generic`), and
client (`omp`, `claude-code`, `claude-sdk`, `codex-cli`, `codex-app-server`, or
`generic`). The router rejects an identity mismatch, duplicate registration,
missing workspace grant, stale expected version, or invalid operation ID.

The primary agent may create a scoped delegate credential for a nested worker.
Delegation is a capability, not a shared secret: the delegate is tied to its
owner and cannot register as another agent or claim another workspace.

## Workspace and delivery model

Joining a workspace is explicit. `workspace join` or MCP `workspace_join`
establishes a cursor and emits the join event. Provider `--workspace ROOM` is an
initial join convenience, not the only route to membership. A provider cannot
receive delivery until its join and readiness handshake succeed; one that has
not joined a workspace does not start delivery.

The router delivers durable task events to the joined ready provider session. It
uses one in-flight delivery per provider, acknowledgement and cursor fences,
timeouts, disconnect handling, and stale-session protection. It does not replay a
chat transcript or inject task text into an unsolicited provider turn. Stock
Codex CLI uses an explicit MCP pull boundary; stock Claude Channel similarly
requires the provider to be joined and ready.

Workspace events are durable records for coordination. `workspace list`,
`workspace members`, `workspace history`, and `workspace watch` expose state
without copying private content into provider environment variables or logs.

## Task state machine

Tasks use optimistic versions and idempotent operation IDs:

```text
todo ── request/begin ──> in_progress ── checkpoint ──> in_progress
  │                            │  │
  │                            │  ├── pause ──> paused or blocked
  │                            │  └── complete ──> done
  │                            └── interrupt ──> stop evidence pending
  ├── cancel ──> cancelled
  └── reopen <────────────── paused/blocked/cancelled
```

An executor must call `task_begin` for the assigned attempt. It records progress
with `task_checkpoint`, and must explicitly call `task_pause` or `task_complete`.
Each checkpoint contains a summary, next steps, artifacts, and risks. Assignment
alone does not wake or start an executor. Readiness, a chat reply, or a provider
process that merely starts does not begin or complete a task.

An interrupt requests a provider stop and initially records `unknown` stop
evidence. Assignment may change while that evidence is unknown, but new task
requests and `task_begin` remain fenced; the old provider session must not be
reused for execution. Managed hosts can automatically confirm the stop from
terminal or child-reap evidence tied to the exact attempt and provider session.
Read the task again after interruption: if evidence is already `confirmed`, no
manual confirmation is needed. Only if it remains `unknown` and the operator
actually observed execution stop should `task confirm-stopped` record that
evidence, using the exact interrupted attempt UUID and a freshly read task version.
If automatic confirmation or another mutation wins the race, read and reassess
instead of retrying stale confirmation. Once confirmed, use the current version
for handoff; the next executor reviews the prior checkpoint before `task_begin`.

## Full-screen operator client

The console uses authoritative `TaskSummary`, `TaskDetail` and
`TaskMutationResult`, not the CLI's shortened display rows. Chat is durable
coordination, not a model launcher. Task views distinguish assignee, actual/last
executor, attempt/provider session, version, checkpoint and stop evidence.
Assignment does not request execution; a request acknowledgment is not `Begun`,
and a send result is not task completion. Operator forms never call the executor's
begin/checkpoint/pause/complete operations on its behalf.

### Input and acknowledged writes

The minimum layout is 80×24, with an inline detail pane from 120 columns. Smaller
terminals keep the connection but disable mutations. F1/F2/F3 choose Chat/Tasks/
Members; F4 invites; Tab changes focus; Enter opens detail. Ctrl-N creates an
admin workspace and Ctrl-T creates a task. Task keys `a/r/i/f/e/m/c/o` map to
assign/request/interrupt/confirm-stopped/edit/note/cancel/reopen. `/` filters task
states and assignee. Ctrl-S submits a form; Enter is not an implicit submit.
Chat's composer accepts multiline Unicode/bracketed paste, with Ctrl-S posting
and no optimistic history insertion.

Opening a version-fenced task form captures its version/attempt. A fresh task
read before submission detects changes and keeps the draft; Ctrl-R explicitly
reconfirms the latest baseline before another Ctrl-S. Server conflict metadata is
preserved. Creation and notes do not fabricate expected versions. A mutation
freezes its complete payload and operation ID; a supported uncertain-result retry
uses exactly those bytes/identity. Changing payload/version is a new operation.
Task requests lack a mutation receipt and are never automatically resent after an
uncertain result; task snapshots/history and durable results resolve them.
Workspace creation likewise is inspected rather than blindly replayed.

Pending requests are tracked by request ID (bounded to 32), including accepted,
execution-observed and unresolved outcomes. They prevent unsafe workspace
switching/detachment without acknowledgment of requester ownership. An interrupted
attempt with unknown stop evidence blocks new execution. Manual confirmation is
offered only for the current interrupted/unknown attempt and requires an
observed-stop checkbox and note; the UI never infers evidence from silence,
assignment or a disconnected socket. Reconnection/resynchronization disables
writes.

### One client, bounded asynchronous reads

One `RouterClient` owns the console's subscription and requester lifetime. The
controller drains events separately from rendering and uses at most **three
background reads plus one action-lane read** for a fresh task/stop check, not a
client per pane or unbounded polls. Read kinds coalesce; only one display-history
read runs at a time. Five-second workspace/member polls skip duplicate work.
Responses are fenced by client generation, connected epoch, selected workspace
generation, and request/page/selection revisions. Both successes and failures
from superseded reads are discarded. A stale snapshot cannot replace a newer
task version already observed from an event.

Initial selection joins, loads up to 100 recent events, then subscribes from the
cursor until live. Subscription response pages are applied explicitly, not
assumed to arrive again on the event channel. Events are deduplicated by
workspace/sequence and acknowledged **only after application**. The intentionally
omitted older tail is not treated as a live sequence gap.

ACKs coalesce to one cumulative applied watermark per current workspace stamp.
If the client's bounded local command queue is full (for example, after a
100-event page), the controller retains that watermark and retries without
blocking event drain. It neither ACKs unapplied events nor invalidates live
synchronization merely because ACK enqueueing was delayed. Reconnect can replay
already-applied events until the watermark reaches the client; deduplication
makes this safe. Genuine sequence gaps reset/recover the subscription from the
last applied cursor and fence writes until live.

### History, paging and workspace transitions

Live Chat holds at most 2,000 events/8 MiB. Past mode keeps a separate logical
display page of at most 100 events/8 MiB and discards the live body cache, rather
than retaining both. Live events still update task state, pending results and ACK
cursors while incrementing the new-event count; the database retains their bodies.
End reloads the recent tail. Display-history cursors never move the live applied
cursor backward. A display-history failure leaves live synchronization, state and
ACK progress untouched and offers End to reload; it does not masquerade as a
transport failure.

Chat `[`/PgUp reads older history; `]`/PgDn pages forward in past mode. Backward
navigation uses the existing forward-history API and continues short byte-capped
pages until the desired boundary. Workspaces and tasks use `[`/`]` cursor paging;
task filtering resets its page, and `h` opens independently paged task history.
Large event/detail bodies use bounded previews and scrollable detail rather than
blocking terminal rendering.

A workspace switch requires no pending action/request or unsent chat draft, then
unsubscribe → confirmed leave → join/subscription. The old room is not replaced
optimistically. Successful unsubscribe clears the client's restored-subscription
state. A definite leave failure restores the old view/subscription;
`LeaveUnconfirmed` instead closes that operator client and reconnects to the old
room before allowing another switch. It never blindly retries leave or joins a
new room on uncertain membership. On a new connected epoch, the existing client
restores from its ACK cursor and the controller refreshes members, current task
page and selected detail rather than implementing a second reconnect loop.

### Detach dispatch and terminal safety

Accepting detach sets a shared dispatch fence before controller shutdown. RPC
dispatch is polled under the same short lock (never held across a pending await):
detach-first cannot enqueue a new action; request-first is tracked before the
input layer decides whether `DETACH` confirmation is required. Queued commands,
action continuations and unwinding cannot send new work after that fence.
Shutdown aborts remaining reads/actions and closes the requester client; it does
not synthesize successful writes or native stop evidence.

Terminal ownership is guarded across startup, normal exit, error, cancellation
and unwind. The panic hook restores terminal modes before delegating to the prior
hook; cleanup restores bracketed paste, alternate screen, cursor and raw mode.
SIGINT follows the pending-request detach check; termination signals detach
without stopping the server. Content is rendered as escaped data (including
terminal escape/control sequences), Unicode-aware editing preserves graphemes,
and `NO_COLOR` disables color.

Invitation text is kept outside ordinary view/event state. `y` invokes an
available native clipboard helper only on explicit request; no automatic OSC52
or helper installation occurs. `p` temporarily restores the normal screen and
prints the exact token-bearing prompt; Enter re-enters the UI. Closing the modal
drops its private prompt buffer, but cannot erase a deliberately printed prompt,
provider conversation or clipboard. Clipboard contents are not read or silently
cleared.

## MCP boundary

The production Rust MCP server uses the official MCP framing and client
interoperability path. Roles include `delegate`, `codex-cli`,
`claude-channel`, and `omp`. The catalog includes workspace operations, task
inspection and lifecycle operations, agent coordination, and explicitly selected
external integration operations.

MCP input is bounded and validated before dispatch. The server limits frame sizes
and concurrent calls, rejects unknown fields and identity spoofing, and returns
typed errors rather than embedding secrets in responses. Provider-facing
instructions treat task, peer, and external text as untrusted data.

## Optional external integrations

GitHub and Linear are optional server-private targets. Configuration associates a
workspace and provider target with a private token file and read or write access.
The router exposes only public metadata and explicit operations. `task import`,
`task link`, and `task publish` never read an agent's personal backlog or initiate
a provider login. External writes are serialized per target and use an operation
record with `running`, `applied`, `not_applied`, or `unconfirmed` resolution; an
operator must resolve an unconfirmed operation before retrying.

## Provider process policy

Stock modes preserve the provider's own process and MCP contract. Managed modes
own the child process, pass only an allowlisted environment, and terminate the
child on router shutdown or interruption. The Claude SDK bridge is a retained
Node package used only by `gateway claude`; the OMP and Claude Channel packages
are optional integration surfaces, not alternate router implementations.

Resume is always explicit. A resumed provider reviews durable task state and the
last checkpoint; a new provider session is never inferred from a chat message.

## Network and secret handling

Loopback is the safe default. `router start --share=tailscale` requires valid
Tailscale configuration, a loopback bind, and no ASR TLS settings. Tailscale Serve
forwards raw TCP and publishes `ws://<TAILSCALE_IP>:<PORT>/ws` over the encrypted
tailnet, not an HTTPS/WSS endpoint. With ASR TLS configured, `--share=auto`
selects local mode on loopback or LAN mode on a non-loopback bind. Without TLS,
auto mode requires loopback and selects valid Tailscale sharing when available,
otherwise local mode. There is no plaintext LAN fallback.

Direct `--share=lan` requires a non-loopback `ASR_BIND` and all of
`ROUTER_TLS_CERT`, `ROUTER_TLS_KEY`, and `ROUTER_PUBLIC_URL`. The public URL must
use WSS, the `/ws` path, and the same port as the bind address. Partial TLS
configuration and non-loopback binds without TLS fail closed. Remote clients use
private credentials and a trusted CA, with `ASR_CA_FILE` supplying private CA
trust when needed.

Router credentials, provider credentials, and integration tokens remain in
separate private files. The router stores authentication hashes, not bearer
escrow. Long-lived bearer values are never printed; the explicit onboarding
prompt is the deliberate exception for a short-lived invitation. Child provider
environments are filtered; integration configuration errors fail closed.
Content and secret values are not emitted to diagnostic logs.

## Verification invariants

Native integration tests exercise router ownership, workspace grants, optimistic
task transitions, provider child cleanup, the production MCP server with an
official client, and TLS trust/reuse/fail-closed behavior. The implementation
plan's concurrency requirement is normative: two workers' responses remain
isolated under concurrency. Release CI repeats focused black-box scenarios
against the compiled current-platform `asr` binary without live accounts or
Tailscale.
