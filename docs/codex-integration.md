# Codex App Server provider adapter

## Status and boundary

The repository contains a minimum Codex provider adapter for one
gateway-owned thread. It does not attach to or take over an arbitrary thread
already controlled by another Codex UI. The adapter can start a new thread or
resume one explicitly selected by runtime configuration.

The implementation uses Bun plus the official MCP server SDK and Zod schema
validation for the provider-facing tool process:

- `CodexStdioTransport` owns a local `codex app-server --listen stdio://`
  process and exchanges newline-delimited JSON;
- `CodexAppServerAdapter` implements the provider-neutral `SessionAdapter`;
- `GatewayClient` registers the resulting session and handles central router
  delivery;
- `agent-tools-mcp` exposes `agent_list` and `agent_send` over stdio using an
  outbound-only delegated router connection;
- `bun run gateway:codex` wires those components together for a manual run.

## Verified protocol surface

The implementation follows the stable public lifecycle:

```text
initialize -> initialized
thread/start or thread/resume -> thread.id
turn/start(threadId, text) -> turn.id
item/completed(threadId, turnId, agentMessage)
turn/completed(threadId, turn.status)
```

The official App Server documentation defines stdio as JSONL, requires the
initialization handshake, and defines `thread/start`, `thread/resume`,
`turn/start`, `turn/interrupt`, item events, and terminal turn status. The
current public source defines `TurnCompletedNotification` with `threadId` and a
`turn`, and `ItemCompletedNotification` with `threadId`, `turnId`, and an item.

Sources checked on 2026-07-30:

- [Codex App Server documentation](https://developers.openai.com/codex/app-server/)
- [official App Server README](https://github.com/openai/codex/blob/main/codex-rs/app-server/README.md)
- [thread protocol types](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/thread.rs)
- [turn protocol types](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/turn.rs)
- [item protocol types](https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/item.rs)
- [Codex MCP configuration](https://developers.openai.com/codex/mcp/)
- [MCP stdio transport](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports)

The adapter omits `capabilities.experimentalApi`; no experimental method or
field is part of this baseline.

## Agent tools and delegated identity

Each Codex gateway creates a unique delegation token before starting App
Server. Per-process `-c` overrides configure one required stdio MCP server with
only `agent_list` and `agent_send` enabled. App Server receives the token,
router URL, and agent ID through environment variables; secret values never
appear in command arguments and user-level Codex configuration is not modified.

The MCP child registers as a delegate of the already configured `agentId` only
when its first tool is called. The router verifies the token against the live
owning gateway. A delegate:

- may list agents, send requests, and ping;
- sends with the owning agent's authoritative `from` identity;
- cannot receive deliveries or issue replies;
- is not returned as a separate agent;
- is revoked when the owning gateway disconnects.

For a nested call, the router caps the delegated request timeout to the owning
gateway's remaining inbound deadline. The MCP server returns the correlated
result to Codex without writing request or response content to stdout or
stderr. Stdout remains exclusively reserved for MCP protocol frames.

## Correlation and completion

Only one provider turn is active at a time. The adapter keeps the router
`requestId`, selected `threadId`, returned `turnId`, deadline, and completed
agent messages in one active-turn record.

An item contributes output only if both its `threadId` and `turnId` match that
record. Completed `agentMessage` items with `phase: final_answer` are joined in
event order. If a compatible server emits no phase, the last unphased message
is used. Commentary, unrelated turns, malformed messages, and late events do
not become the router result. `turn/completed` is the only success boundary.

Events can arrive before the `turn/start` response is observed by the client.
The adapter temporarily buffers a bounded number of same-thread terminal/item
events and replays them after it learns `turnId`.

## Failure and approval behavior

| App Server condition | Adapter result/action |
| --- | --- |
| another provider turn is active | `session_busy` |
| request deadline expires | `request_timeout`, then best-effort `turn/interrupt` |
| turn status is `interrupted` | `codex_turn_interrupted` |
| turn status is `failed` | `codex_turn_failed` |
| completed turn has no final/unphased message | `codex_no_final_response` |
| malformed response or RPC timeout | `codex_protocol_error` |
| stdio process/transport exits | `provider_disconnected` |

Command and file-change approval requests receive `decision: decline`.
Permission requests receive an empty grant, and MCP elicitation is declined.
Unsupported server requests, including interactive user input in this
non-interactive baseline, receive a JSON-RPC method-not-supported error. The
adapter never sends an automatic accept response.

## Authentication and privacy

Codex authentication remains owned by the installed CLI and its local runtime
environment. No Codex credential enters router messages. Router authentication
uses `ROUTER_TOKEN` independently.

The stdio transport never logs protocol frames and discards child stderr.
Consequently prompts, responses, credentials, provider thread IDs, working
directories, and command output are not copied into gateway logs. Runtime
values are supplied only through environment configuration.

The delegation token is not the router's shared registration credential. It is
random, scoped to one live agent, limited to outbound operations, and removed
with the owner connection. A non-loopback deployment still requires WSS and a
per-agent central credential; the delegation token does not replace that
transport control.

## Live validation result

The two-Codex gate was completed on 2026-07-30 using a loopback router,
disposable workspaces, and neutral agent identifiers. The validation confirmed:

- each primary gateway appeared once in `agent_list`, while delegated MCP
  connections remained hidden;
- Codex A called `agent_send`, Codex B produced the nested result, and the
  correlated completion returned through Codex A to the original requester;
- simultaneous direct requests to Codex A and Codex B returned only their own
  sentinels, demonstrating response isolation at the live provider boundary;
- a cleanly restarted provider recovered without replaying an earlier timed-out
  request.

The repository retains only generic pass/fail results. Router, gateway, and MCP
application logs did not emit prompt bodies, provider responses, credentials,
thread identifiers, machine details, or runtime paths.

## Separate development-machine validation

Use a disposable test workspace and neutral agent identifiers. Do not record
the machine name, account name, paths, tokens, or provider thread IDs in this
repository.

1. Install and authenticate a stable Codex CLI outside this repository.
2. Record `codex --version` only in the private test notes. Generate the
   version-matched App Server schema into a temporary directory with
   `codex app-server generate-ts`; do not commit it.
3. Run `bun test` in this repository before the live check.
4. Start the router on an available loopback port with a temporary local token.
5. Set `ROUTER_URL`, `ROUTER_TOKEN`, `GATEWAY_AGENT_ID=local:worker-a`, and a
   disposable `CODEX_CWD`; run `bun run gateway:codex`.
6. Start two gateways with distinct neutral IDs and disposable working
   directories. Confirm both appear exactly once in `agent_list`; delegated MCP
   connections must not appear as additional agents.
7. Send one harmless request from the coordinator to `local:worker-a` directing
   it to call `agent_send` for `local:worker-b`. Confirm worker B sees
   `from=local:worker-a` and the final nested response returns through worker A
   to the original coordinator request.
8. Send a second request while the first is active and confirm `session_busy`.
9. Exercise a short timeout and confirm the caller receives `request_timeout`,
   App Server receives `turn/interrupt`, and any late completion is ignored.
10. Stop the Codex process during an active request and confirm
    `provider_disconnected`; restart manually and confirm no request is replayed.
11. Trigger an operation requiring approval in the disposable workspace and
    confirm the adapter declines it without executing the protected action.
12. If inactive-thread continuation is required, repeat with an explicitly
    selected `CODEX_THREAD_ID`. Do not test simultaneous control by another UI;
    that behavior is outside the supported boundary.

Afterward, inspect only generic pass/fail metadata. Delete temporary schemas and
runtime secrets, and keep the repository free of environment-specific values.
