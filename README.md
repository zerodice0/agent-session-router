# Agent Session Router

Agent Session Router (ASR) is a native Rust v2 router for explicit agent workspaces,
durable tasks, provider sessions, and operator-controlled handoff. The `asr` binary
owns the router and the protocol boundary; provider hosts join a named workspace
before they can receive work.

## Breaking cutover

This checkout no longer ships the old TypeScript router/provider launcher, the
Python launcher, or the shared-token environment contract. Commands named
`agent-session-router`, `bun run ...`, and the old `scripts/asr.py` entry point are
not part of v2. Use the compiled `asr` binary and per-agent or operator credential
files instead.

The Rust binary is the only core runtime. The retained `integrations/omp` package
is an optional package loaded by an external OMP host. The retained
`integrations/claude-sdk` package is used only by the managed Claude gateway and
therefore requires Node when that mode is selected. Bun is needed to build the
packaged integration assets, not to run the router, console, onboarding installer,
workspace, task, credential, or stock-provider commands.

## Build and install

Build the native executable with the repository's pinned Rust toolchain:

```sh
cargo build --locked --release
./target/release/asr --help
```

Release archives have this layout:

```text
bin/asr
share/agent-session-router/integrations/omp/
share/agent-session-router/integrations/claude-sdk/
share/agent-session-router/integrations/claude-plugin/
share/agent-session-router/integrations/codex/
```

Run `bin/asr install` from an archive to install the binary and its adjacent
integration assets into `$HOME/.local/bin` (or the configured destination). The
installer refuses conflicting or unsafe paths; it does not search a source
checkout for runtime assets.

On macOS, distribute a signed and notarized archive through the normal
organization policy. Do not bypass Gatekeeper or weaken system quarantine as an
installation step.

### Prepare the server's bootstrap bundle

Copy/paste onboarding needs deployment assets on the **server**, not just a locally
built `asr`. The release workflow produces an `asr-bootstrap-bundle` Actions
artifact; it does not publish a public npm package or GitHub Release download URL.
Extract that artifact into `$ASR_DATA_DIR/bootstrap`, or set `ASR_BOOTSTRAP_DIR` to
its absolute directory before starting the router. Keep it owned by the server
user and free of symlinks. The router validates the manifest and files at startup.

The supported target IDs are:

- `aarch64-apple-darwin`
- `x86_64-apple-darwin`
- `aarch64-unknown-linux-gnu`
- `x86_64-unknown-linux-gnu`

Only targets actually present in the bundle can install. A current-host build does
not create binaries for the other architectures; Linux targets require GNU/Linux,
and Windows is unsupported. The provider CLI must already be installed on the
client. Onboarding does not install system packages, bypass host approvals, or
build missing targets from source.

For a locally assembled bundle, follow the native archive staging in
[the release workflow](.github/workflows/release.yml). `bun run build:integrations`
runs `scripts/package.ts`, which packages the canonical ASR skill for all three
hosts, the Claude skill-only plugin, OMP integration, and managed Claude bridge,
using the version from `Cargo.toml`. For each available `TARGET`, stage
`asr-TARGET`, `agent-session-router-TARGET.tar.gz`, and the archive's `.sha256`
file together in a dedicated directory; a raw-binary `.sha256` is also supported.
The raw binary must match the archive's `bin/asr`. Then generate the manifest:

```sh
bun scripts/bootstrap-manifest.ts --dir dist/bootstrap
```

This script verifies existing artifacts; it does not cross-compile them. Place the
resulting directory on the server before startup. Without a bundle, the router and
already-installed clients still work, but invitation generation fails before
issuing an invitation.

## Native v2 quickstart

### 1. Start the owned server

The default endpoint is `ws://127.0.0.1:8787/ws`. In a terminal, start the router
and its full-screen operator console:

```sh
asr router start
```

With terminal stdin/stdout and `TERM` other than `dumb`, this starts or reuses the
owned router and opens the console. Use **Ctrl-N** to create `team-room`, then
**F4** to generate a client invitation for the selected room. The numbered `asr`
menu also offers start (1) and console reattach (7).

Choose the launch mode deliberately:

| Command / environment | Behavior |
| --- | --- |
| `asr router start` in a usable TTY | Owned router plus full-screen console; leaving the console does not stop the server. |
| `asr router start --background` | Start/reuse without a console and return. |
| `asr router start --no-ui` | Foreground headless lifecycle; Ctrl-C interrupts the owned child, waits for reaping and cleanup, then returns exit code 130. |
| Non-TTY stdin/stdout or `TERM=dumb` | No automatic console; foreground lifecycle unless `--background` is given. |
| `asr ui --workspace team-room` | Attach to an already-running router; never start one. |

`--background` and `--no-ui` are mutually exclusive. An already-running owned
router is reused rather than adopted as a new foreground child. To use CLI
commands while the console is open, use another terminal. Alternatively, detach
with `q` in navigation or Ctrl-C and later reattach with `asr ui`.

### 2. Generate and share a one-use invitation

For a CLI-only path, run this on the owned server:

```sh
asr onboarding prompt --workspace team-room --name office --create-workspace
```

Omit `--create-workspace` when the room must already exist. Workspace choice and
creation belong to the server administrator. Add
`--provider claude-code`, `--provider codex-cli`, or `--provider omp` to restrict the
invitation to that host; otherwise the receiving conversation chooses its own
host, not whichever executables happen to be installed.

The default invitation uses the owned router's advertised endpoint, or its control
endpoint if none is advertised. A loopback invitation works **only on the same
machine**. For another device, start with configured Tailscale sharing or TLS as
described in [Profiles, remote routers, and TLS](#profiles-remote-routers-and-tls).
Repeated `--endpoint KIND=URL` options replace the candidate list with already
configured aliases of this server (`tailnet`, `lan`, `public`, or local-only
`local`). They do not create listeners, proxies, tunnels, or firewall rules.
`--ca-file PATH` supplies a public CA certificate, not a private key.

Send the complete generated prompt through a trusted channel and paste it into
the intended Claude Code, Codex CLI, or OMP conversation. It contains a **10-minute,
one-use invitation secret**: someone with the prompt can redeem it first while it
is valid. It contains no administrator token, long-lived credential, or private
key. Do not publish it, log it, or put its ticket in command arguments, environment
variables, or URLs. Use a fresh invitation for every device/provider identity.

### 3. Install, restart, then confirm participation

The generated prompt includes an approved-shell bootstrap for clients without ASR.
It downloads the supplied host target, verifies its pinned size/hash, and passes
the invitation JSON through stdin to
`asr onboarding install --provider PROVIDER --stdin`. Run the generated block
unchanged through the provider's normal shell approval process; do not construct
a download URL or expose private credential/journal files to the conversation.

The installer verifies server identity and transport, tries supplied reachable
routes in Tailnet → LAN → public order, and fails closed on trust/hash mismatch.
It installs a versioned native bundle, stores a private client-generated credential,
and registers the profile and host integration without replacing unrelated
provider settings or an existing PATH executable.

`stage: configured` means installation/registration, **not current-session
activation**. Follow the returned `nextAction` exactly; `activation` remains
`restart_required` or `not_checked`, not proof of a connected model. Codex needs a
new session; OMP needs a process restart, not just command reload; Claude's Channel
launch additionally needs normal organization policy and development opt-in.

After the prescribed restart, use the host's ASR interface:

| Host | Workspace entry point |
| --- | --- |
| Claude Code | `/asr:workspace status` |
| Codex CLI | `$asr workspace status` (select `$asr` or use `/skills`) |
| OMP | `/asr workspace status`; the shared skill is `/skill:asr workspace` |

Confirm this host's actual MCP `workspace_list` and `workspace_members` show its
identity in the invited workspace. Joining (`workspace_join`) and readiness are
required for delivery; an operator CLI connection cannot stand in for provider
participation. Chat posts coordinate people and agents; they do not run a model.
Task execution is a separate request/begin/checkpoint/complete lifecycle below.
See the existing [provider guide](docs/provider-integration.md),
[Claude](docs/claude-integration.md), [Claude Channel](docs/claude-channel-integration.md),
[Codex](docs/codex-integration.md), and [OMP/AgentBridge](docs/agentbridge-integration.md)
guides for host-specific activation and execution boundaries.

Resume the same client's interrupted installation with its invitation ID, or
inspect its non-secret status:

```sh
asr onboarding resume INVITE_ID --provider codex-cli
asr --profile office onboarding status --provider codex-cli --json
```

Use the provider from the original installation. Resume uses the private journal
and the same enrollment/credential, not a second invitation consumption. Status
checks stored installation integrity and endpoint reachability; it does not start
a provider or assert that the current conversation is connected. On the server:

```sh
asr onboarding revoke INVITE_ID
asr credential list --json
asr credential revoke CREDENTIAL_ID
```

Invitation revocation prevents redemption/resume but does not revoke an already
issued credential. Revoke that credential separately when access must end.

The built-in `local` profile cannot be added or replaced. Manual profiles store
router addresses; onboarded profiles also retain verified route/server metadata
and provider bindings to private credential files, never bearer values. Explicit
`credential issue` and provider launch commands remain available for manually
managed identities; onboarding does not require copying an operator credential
to a provider machine.

### Full-screen console

The console requires at least **80×24**. At 120 columns it adds an inline detail
pane; narrower supported layouts use Enter for detail. Below the minimum it keeps
the connection but disables mutations until resized. It supports Unicode input,
bracketed paste, escaped untrusted terminal content, and `NO_COLOR`.

| Context | Keys |
| --- | --- |
| Navigation | F1 Chat, F2 Tasks, F3 Members, F4 Invite; Tab/Shift-Tab changes focus; arrows select; Enter opens detail; `?` opens help. |
| Workspace sidebar | Enter joins the selected room; `[` / `]` pages the list; Ctrl-N creates a workspace (admin only). |
| Chat | `i` focuses the composer; Enter adds a newline; Ctrl-S posts; `e` toggles all events; `[` / PgUp loads older history, `]` / PgDn pages forward in past mode, End reloads the live tail. |
| Tasks | Ctrl-T creates; `a` assigns, `r` requests execution, `i` interrupts, `f` confirms an observed stop when eligible, `e` edits, `m` adds a note, `c` cancels, `o` reopens. |
| Task browsing | `/` filters states/assignee; `[` / `]` pages results; `h` opens task history, also paged with `[` / `]`. |
| Forms | Tab/Shift-Tab moves fields; Ctrl-S submits; Esc cancels when no write is in flight; Ctrl-R explicitly reconfirms a changed task version. Assignment supports member selection with arrows, an explicit ID, or empty for unassigned. |
| Invitation result | `y` explicitly copies using an available native clipboard helper; `p` prints the exact prompt on the normal screen, then Enter returns. Esc discards the in-memory prompt. |
| Leave / stop | `q` in navigation or Ctrl-C detaches; Ctrl-X opens the separate owned-router stop confirmation. |

Chat drafts and forms remain pending until acknowledged; the console does not
optimistically invent a saved message or task transition. A stale task form keeps
your draft and requires explicit reconfirmation, rather than silently submitting
against a newer version. If a mutation result is unknown, only a supported
immutable retry resends the same operation/payload. An uncertain task request is
**not resent**: inspect task state/history first. Writes are disabled while
reconnecting or resynchronizing.

**Assignee is not executor.** Assignment changes responsibility without starting
work; request acceptance is not evidence that execution began. The console shows
authoritative task/attempt state, version, executor and stop evidence. It has no
operator button that impersonates an executor's begin/checkpoint/pause/complete.
Unknown stop evidence blocks new execution even if the assignee changes.
Manual stop confirmation requires the exact interrupted attempt, a note, and
confirmation that you actually observed execution stop.

The server survives detachment, but requests made by this console belong to its
connection. If requests are pending or unresolved, leaving requires typing
`DETACH` and pressing Ctrl-S; Esc returns without disconnecting. Confirming closes
the requester connection and can interrupt its work via `RequesterDisconnected`
and `CancelWork`. Workspace switching is also blocked while such work or a write
is pending. This does not cancel unrelated requests owned by other connections.
To stop the **server**, a verified owned-admin console requires Ctrl-X, `STOP`,
then Ctrl-S, warning that other workspaces are affected. Remote/scoped viewers
must ask the managing server operator to use `asr router stop`.

Reattach locally with `asr ui --workspace team-room`. Without an explicit profile
or `ROUTER_URL`, `ui` uses the owned runtime's control URL, not a saved default
remote profile. `--credential PATH` (before `ui`) takes precedence over
`ASR_CREDENTIAL_FILE`, then the owned admin credential; explicit credentials are
never silently replaced by admin credentials. For a remote router, use
`asr --profile NAME --credential PATH ui --workspace ROOM`. Admin actions depend
on authenticated server authority, not credential-file labels or loopback alone.

Leaving, handled signals, errors, and panic handling restore raw mode, cursor,
bracketed paste and the normal terminal screen. Console startup failure leaves the
router running and reports reattach guidance; it does not silently stop the server.


### Self-owned task flow

The simplest workflow is self-owned: create a task in the room, assign it to the
running provider identity, request it, and let that provider own the attempt.
Task creation is JSON on stdin:

```sh
printf '%s\n' '{"title":"Review the release manifest","description":"Check native archive paths and report risks."}' |
  asr --profile local task create team-room --stdin --json
asr --profile local task list team-room --json
asr --profile local task assign team-room TASK_ID \
  --agent local:worker-a --expected-version VERSION --json
asr --profile local task request team-room TASK_ID \
  --expected-version ASSIGNED_VERSION --json
```

`TASK_ID`, `VERSION`, and `ASSIGNED_VERSION` above are values returned by the
previous command; replace `local:worker-a` with the actual invited or manually
provisioned provider identity. Creation intentionally has no implicit assignee.
Assignment alone does not wake or start an executor. Version-fenced mutations
use the current expected version; idempotent mutations also use operation IDs
so an immutable retry cannot apply twice. Creation and notes do not invent an
expected-version requirement.

The assigned provider uses the production MCP tools
`workspace_join`, `task_begin`, `task_checkpoint`, `task_pause`, and
`task_complete`. A checkpoint records a summary, next steps, artifacts, and risks.
Pause is a durable state transition; completion is explicit and does not happen
merely because an agent replied. Inspect state and history from the operator CLI:

```sh
asr --profile local task show team-room TASK_ID --json
asr --profile local task history team-room TASK_ID --json
```

### Stop-unconfirmed handoff

Interrupting an active attempt requests a stop but does not prove that execution
has ended. Until stop evidence is confirmed, treat the attempt as
`stop-unconfirmed`; new task requests and `task_begin` remain fenced, and the old
provider session must not be reused for execution. Assignment can change while
stop evidence is unknown, but assigning alone does not wake or start an executor:

```sh
printf '%s\n' 'Stop before handing this task to worker-b.' |
  asr --profile local task interrupt team-room TASK_ID \
    --expected-version VERSION --stdin --json
```

Managed hosts can automatically confirm the stop from terminal or child-reap
evidence tied to the exact attempt and provider session. Read the task again
after the interrupt rather than reusing its returned version:

```sh
asr --profile local task show team-room TASK_ID --json
```

If stop evidence is already `confirmed`, skip manual confirmation. Only if it is
still `unknown` and the operator has actually observed execution stop, confirm
the exact interrupted attempt UUID using the freshly read task version:

```sh
printf '%s\n' 'Provider exit observed by the operator.' |
  asr --profile local task confirm-stopped team-room TASK_ID \
    --attempt ATTEMPT_UUID --expected-version CURRENT_VERSION --stdin --json
```

If confirmation races with automatic evidence or another mutation, read the task
again and reassess; do not blindly retry the old version or confirm an already
confirmed attempt. Once stop evidence is confirmed, read the current version and
hand off to a joined, ready worker:

```sh
asr --profile local task show team-room TASK_ID --json
asr --profile local task assign team-room TASK_ID \
  --agent local:worker-b --expected-version CURRENT_VERSION --json
asr --profile local task request team-room TASK_ID \
  --expected-version ASSIGNED_VERSION --json
```

Use `CURRENT_VERSION` from the latest `task show` and `ASSIGNED_VERSION` from the
assign response. The new executor must review the prior checkpoint before calling
`task_begin`.

## Provider modes

Provider commands accept an optional workspace. `--workspace ROOM` requests an
initial join; supported MCP clients can also join explicitly with `workspace_join`.
Router collaboration and task delivery require actual workspace membership and
readiness, not merely the launch flag. A native Codex command may run a local
prompt without a workspace, but that prompt has no workspace transcript or router
delivery. Choose stock provider behavior when the provider owns its normal CLI,
or a managed gateway when ASR owns the provider subprocess:

| Mode | Command | Runtime behavior |
| --- | --- | --- |
| Stock Codex CLI | `asr codex-cli AGENT --workspace ROOM` | Uses the installed Codex CLI and an explicit MCP pull boundary; it does not inject unsolicited turns. |
| Owned Codex app server | `asr codex AGENT --workspace ROOM` | ASR owns an interactive Codex App Server session and terminal input. |
| Managed Codex | `asr gateway codex AGENT --workspace ROOM` | ASR launches and supervises Codex App Server. |
| Stock Claude Code | `asr claude AGENT --workspace ROOM` | Uses Claude Channel MCP after `setup-claude` and organization/development opt-in. |
| Managed Claude | `asr gateway claude AGENT --workspace ROOM` | Uses the packaged Node Claude bridge; Node is required for this mode only. |
| External OMP | `asr omp AGENT --workspace ROOM` | Uses a linked, enabled OMP integration package; restart OMP after setup. |

Provider sessions do not resume from an implicit chat transcript. Use the
provider's explicit resume option where supported and review the durable task
checkpoint first. `asr claude` accepts `--resume`; managed provider supervision
also tears down its child process on interruption.

## Optional private GitHub and Linear integrations

External integrations are server-private configuration, not a personal backlog
connector and not an agent login flow. The router reads
`$ASR_DATA_DIR/integrations.json` by default, or the path in
`ASR_INTEGRATIONS_FILE`. A configuration has version `1` and absolute private
token-file paths:

```json
{
  "version": 1,
  "connections": [
    {
      "provider": "github",
      "workspace": "team-room",
      "repository": "org/repo",
      "access": "read",
      "tokenFile": "/private/path/github-token"
    },
    {
      "provider": "linear",
      "workspace": "team-room",
      "teamId": "TEAM_UUID",
      "projectId": "PROJECT_UUID",
      "access": "write",
      "tokenFile": "/private/path/linear-token"
    }
  ]
}
```

When an administrator has configured and reloaded a target, operators can check
it and explicitly import, link, or publish a task:

```sh
asr --profile local integration check team-room github --json
asr --profile local task import team-room github EXTERNAL_ID --json
asr --profile local task link team-room TASK_ID github EXTERNAL_ID \
  --expected-version VERSION --json
asr --profile local task publish team-room TASK_ID github \
  --kind issue --expected-version VERSION --json
```

No command reads a user's personal backlog, asks an agent to log in, or publishes
without an explicit workspace, provider, and target.

## Profiles, remote routers, and TLS

Use a profile for a configured remote router and a private operator credential
provided by its administrator:

```sh
asr profile add remote wss://router.example/ws
asr --profile remote --credential "$HOME/.config/agent-session-router/remote-operator.json" \
  workspace list --json
```

Remote operator commands require `--credential PATH` or `ASR_CREDENTIAL_FILE`;
profiles contain no bearer values, and provider bindings do not grant operator
authority. Router address selection is explicit
`--profile` first, then `ROUTER_URL`, then the saved default profile, then built-in
`local`. `ROUTER_URL` remains a supported non-secret address override, not the old
shared-token authentication contract.

`router start --share=tailscale` requires valid Tailscale configuration, a
loopback bind, and no ASR TLS settings. Tailscale Serve forwards the router port
over raw TCP and publishes `ws://<TAILSCALE_IP>:<PORT>/ws`; the tailnet encrypts
transport, but ASR does not terminate HTTPS/WSS in this mode. With ASR TLS
configured, `--share=auto` selects local mode for a loopback
bind or LAN mode for a non-loopback bind. Without ASR TLS, auto mode requires
loopback and selects valid Tailscale sharing when available, otherwise local
mode. It never falls back to plaintext LAN.

Direct `--share=lan` requires a non-loopback `ASR_BIND` and all three TLS settings:
`ROUTER_TLS_CERT`, `ROUTER_TLS_KEY`, and `ROUTER_PUBLIC_URL`. The public URL must
use `wss://`, the `/ws` path, and the same port as the bind address. For example,
on a router host with a certificate for `router.example`, a private key readable
only by its owner, and a trusted CA file:

```sh
ASR_BIND=0.0.0.0:8787 \
ROUTER_TLS_CERT=/private/path/router-cert.pem \
ROUTER_TLS_KEY=/private/path/router-key.pem \
ROUTER_PUBLIC_URL=wss://router.example:8787/ws \
ASR_CA_FILE=/private/path/ca.pem \
  asr router start --background --share=lan
```

On the client, provision the operator credential and CA file securely, then use
the matching endpoint:

```sh
asr profile add lan wss://router.example:8787/ws
ASR_CA_FILE=/private/path/ca.pem \
  asr --profile lan --credential "$HOME/.config/agent-session-router/remote-operator.json" \
    workspace list --json
```

`ASR_CA_FILE` supplies trust for a private CA; it can be omitted when the
certificate is already trusted by the client. Keep provider and integration
secrets in their private files, and use scoped per-agent credentials for providers.

The router enforces workspace grants, agent identity, single-flight delivery,
timeouts, disconnect fences, and durable acknowledgements. It does not log task
content or bearer values as a substitute for access control.

## Development and verification

Core changes use Rust:

```sh
cargo test --locked --all-targets
cargo build --locked --release
```

Optional integration package checks are separate:

```sh
bun run typecheck:integrations
bun run test:integrations
bun run build:integrations
```

The release workflow also runs focused native black-box tests against the compiled
current-platform executable, including router/task state, fake provider
subprocesses, the production MCP server with an official client, and the TLS
child-process boundary. It uses no live provider account or Tailscale dependency.

## Design notes

- [Design](docs/design.md)
- [Provider integration](docs/provider-integration.md)
- [Codex integration](docs/codex-integration.md)
- [Claude integration](docs/claude-integration.md)
- [Claude Channel integration](docs/claude-channel-integration.md)
- [AgentBridge integration](docs/agentbridge-integration.md)
