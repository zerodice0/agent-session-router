# Claude Agent SDK provider adapter

## Status and supported boundary

The repository contains a managed Claude provider adapter built on the official
TypeScript Agent SDK. It owns a resumable SDK session for one gateway identity;
it does not attach to or inject work into an arbitrary Claude UI session that
was started elsewhere.

The adapter implementation and fake-SDK lifecycle tests are complete. A live
development-machine check reached SDK initialization and router registration,
then stopped at the provider authentication boundary. An authenticated live
turn and the mixed Claude <-> Codex exchange remain integration gates.

Sources checked on 2026-07-30:

- [Claude Agent SDK TypeScript reference](https://code.claude.com/docs/en/agent-sdk/typescript)
- [Claude Agent SDK sessions](https://code.claude.com/docs/en/agent-sdk/sessions)
- [Claude Agent SDK MCP](https://code.claude.com/docs/en/agent-sdk/mcp)
- [Claude Agent SDK permissions](https://code.claude.com/docs/en/agent-sdk/permissions)
- [official TypeScript SDK repository](https://github.com/anthropics/claude-agent-sdk-typescript)

The implementation pins the current npm stable SDK and uses only its public
`startup()` and `query()` surfaces.

## Components

- `ClaudeAgentSdkAdapter` prewarms the SDK subprocess, accepts one router
  delivery at a time, captures the result `session_id`, and passes it as
  `resume` on the next delivery.
- `claude-agent-sdk-config` constructs a locked-down SDK option set and exposes
  the existing agent tool handlers through an in-process SDK MCP server.
- `claude-gateway` creates the delegation token, initializes the adapter, then
  registers the primary Claude identity with the router.
- `bun run gateway:claude` starts the complete provider connector.

## Request and response flow

```text
coordinator       router        Claude gateway       Agent SDK       SDK MCP
    | send(id)       |                |                  |               |
    |--------------->| deliver(id)    |                  |               |
    |                |--------------->| query(prompt)    |               |
    |                |                |----------------->|               |
    |                |                |                  | agent_send    |
    |                |                |                  |-------------->|
    |                |                |                  |   router call |
    |                |                |                  |<--------------|
    |                |                | result/session_id|               |
    |                |                |<-----------------|               |
    |                | reply(id)      |                  |               |
    |                |<---------------|                  |               |
    | result(id)     |                |                  |               |
    |<---------------|                |                  |               |
```

The router `requestId` remains the only central correlation identifier. The
provider `session_id` is local runtime state and never enters router messages.
Only an SDK message with `type=result`, a valid session ID, and a successful,
non-error terminal state becomes a successful router reply. SDK exception text
and provider error bodies are reduced to stable generic error codes.

## MCP tools and permissions

Claude receives the same tool contract used by Codex through the Agent SDK's
in-process `createSdkMcpServer()` boundary. The SDK configuration:

- exposes only `mcp__agent_session_router__agent_list` and
  `mcp__agent_session_router__agent_send`;
- sets the built-in tool list to empty;
- uses `permissionMode: "dontAsk"`, so anything not explicitly allowed is
  denied instead of prompting;
- uses `settingSources: []`, `skills: []`, and `plugins: []`;
- enables `strictMcpConfig` so user, project, plugin, and other on-disk MCP
  configurations are ignored;
- keeps an agent-scoped delegation token inside the gateway process and never
  gives the Agent SDK subprocess the router's primary registration credential
  or the delegation token.

The in-process tool service opens an outbound-only delegated router connection
when its first tool is called. The delegate is hidden from discovery, sends
with the owning Claude identity, cannot receive deliveries or reply, and is
revoked when the primary gateway disconnects. Its nested timeout cannot exceed
the owning request's remaining deadline.

## Busy, timeout, disconnect, and resume

| Condition | Result/action |
| --- | --- |
| another SDK query is active | `session_busy` |
| router delivery deadline expires | abort controller + query close, then `request_timeout` |
| SDK throws or returns an error terminal | generic `claude_*` error code |
| SDK stream ends without a terminal result | `claude_no_result` |
| adapter closes during a query | `provider_disconnected` |
| adapter is already closed | `provider_not_ready` |
| successful first turn | capture local `session_id` |
| later turn | start `query()` with `resume=<captured session_id>` |

There is no automatic router replay. A timed-out or disconnected delivery must
be retried explicitly by its caller with a new `requestId`.

## Authentication and privacy

Claude authentication is owned by the local Agent SDK/Claude Code runtime.
Provider credentials are never put in router messages. The Claude subprocess
inherits the local provider environment after the connector removes central
router credentials, router addressing, gateway identity, and provider-selection
variables. The in-process MCP implementation keeps router URL, owning agent ID,
and its scoped delegation token in gateway memory; none are serialized into the
Claude subprocess command line.

SDK stderr is discarded and debug logging is not enabled. Router, gateway, and
MCP logs do not emit prompts, responses, credentials, provider session IDs, or
runtime paths. The Agent SDK may persist its session transcript locally so the
gateway can resume the managed session; that transcript remains on the provider
machine and is never copied to the router.

## Authenticated development-machine validation

Use only a disposable workspace and neutral identifiers. Do not record machine
names, account details, paths, tokens, or provider session IDs in this
repository.

1. Authenticate the installed Claude runtime and verify its auth status without
   capturing account output.
2. Run `bun install --frozen-lockfile` and `bun test`.
3. Start the router on an available loopback port, optionally with a temporary
   `ROUTER_TOKEN`.
4. Start `local:claude-a` with `ROUTER_URL`, `GATEWAY_AGENT_ID`, and a disposable
   `CLAUDE_CWD` using `bun run gateway:claude`.
5. Send one harmless coordinator request and verify exactly one correlated
   result returns without content appearing in logs.
6. Send a second request while the first is active and verify `session_busy`.
7. Use a short deadline and verify `request_timeout`, SDK cancellation, and no
   late router result.
8. Stop the provider during an active request and verify
   `provider_disconnected` with no automatic replay.
9. Start a Codex gateway as `local:codex-a`. Direct Claude to call
   `agent_send(local:codex-a, ...)`, then direct Codex to call
   `agent_send(local:claude-a, ...)`. Verify authoritative `from`, correlation,
   and response isolation in both directions.
10. If persistence is required across gateway restart, capture the session ID
    privately and restart with `CLAUDE_SESSION_ID`; never commit that value.

An optional Claude Code Channel remains a separate future adapter only for an
already-running session that explicitly opts in. It is not required for the
managed-session baseline.
