# Provider-native integration and central routing

## 1. Decision

Each agent participates through a connector running on the same system as the
agent session. The connector initiates an outbound WebSocket connection to the
central router and registers one neutral `agentId`.

`agentId` means "the session currently reachable through this connector." It
does not mean any arbitrary Claude or Codex UI session found on the machine.
Sessions must be launched by the connector or explicitly opt into the connector
through a supported provider mechanism.

AgentBridge is not in the runtime path. Provider-specific behavior is isolated
behind `SessionAdapter` and a local agent-tool boundary.

## 2. Topology

```text
System A                                      Central system
+------------------------+                   +-----------------------+
| Agent A                |                   | Agent Session Router  |
|  ^ inbound result      |                   | - authenticated       |
|  |                     |                   |   agent registry      |
|  v agent_list/send     |                   | - pending requests    |
| Provider connector A   |==== outbound ====>| - timeout/disconnect  |
+------------------------+     WebSocket/WSS  +-----------+-----------+
                                                        ^
System B                                                |
+------------------------+                              |
| Agent B                |                              |
|  ^ inbound delivery    |                              |
|  |                     |                              |
|  v agent_list/send     |                              |
| Provider connector B   |========== outbound ==========+
+------------------------+
```

The central router never opens a connection to an agent system. It only accepts
outbound connector connections and relays messages between them. Agents do not
learn hostnames, ports, provider session IDs, working directories, or provider
credentials belonging to other agents.

## 3. Connector responsibilities

One logical connector owns one `agentId` and three boundaries:

1. Central transport
   - connect, authenticate, register, heartbeat, and reconnect;
   - correlate outbound results and receive inbound deliveries;
   - never replay an uncertain request after reconnect.
2. Inbound provider adapter
   - accept one router delivery at a time;
   - start or notify the selected provider session;
   - return only the final result for that delivery's `requestId`;
   - translate provider busy, timeout, and process exit into stable errors.
3. Outbound agent tools
   - expose `agent_list()` and `agent_send(target, prompt, timeoutMs?)` to the
     local agent;
   - generate a new opaque request ID for each outbound tool call;
   - wait for the correlated router result and return it to the calling agent.

The current `GatewayClient` implements central registration and inbound
delivery. It must become duplex before provider work is considered complete:
an agent handling an inbound request must also be able to call another agent and
await that child result.

## 4. Connection lifecycle

```text
connector              router                    provider session
    | verify local provider readiness                    |
    |--------------------------------------------------->|
    | outbound connect                                    |
    |-------------------->|                               |
    | authenticate/register(agentId, side)                |
    |-------------------->|                               |
    | registered          |                               |
    |<--------------------|                               |
    | now visible in agent_list                           |
```

Registration means both the central transport and the provider adapter are
ready. A connector must not advertise an agent while its provider process,
thread, or channel cannot receive work.

For local development the router remains on `127.0.0.1` and may use the existing
shared test token. Before any central multi-system deployment:

- use `wss://` with authenticated TLS;
- replace the shared token with a credential scoped to an allowed `agentId` and
  operations;
- keep deployment configuration and credentials outside the repository;
- add heartbeat and reconnect with bounded exponential backoff and jitter;
- remove the registration immediately when the connector disconnects;
- do not retry or replay requests whose delivery state is uncertain.

The central router is a trust boundary: it routes message plaintext in memory.
TLS protects transport, but it does not hide messages from the router process.
End-to-end content encryption would be a separate protocol and is not part of
the current plan.

## 5. Agent-to-agent request flow

Every coordinator and worker uses the same flow. "Coordinator" is a behavioral
role, not a privileged transport role.

```text
Agent A       Connector A       Router       Connector B       Agent B
   | agent_send  |                 |              |               |
   |------------>| send(id,to)     |              |               |
   |             |---------------->| deliver(id)  |               |
   |             |                 |------------->| provider input|
   |             |                 |              |-------------->|
   |             |                 |              | final result  |
   |             |                 | reply(id)    |<---------------|
   |             | result(id)      |<-------------|               |
   | tool result |<----------------|              |               |
   |<------------|                 |              |               |
```

Rules:

- The origin connector creates `requestId`; every layer preserves it unchanged.
- The router derives `from` from the authenticated registration rather than
  trusting model-supplied identity.
- `accepted` only confirms socket delivery. Only `result` completes
  `agent_send`.
- One connector rejects a second inbound delivery with `session_busy` instead
  of queueing, steering, or interrupting the current provider turn.
- An agent may make an outbound child request while processing an inbound
  request. Inbound busy state and outbound pending state are separate.
- When a child request is associated with a parent request, its timeout is
  capped to the parent's remaining deadline.
- A late result is discarded after timeout. Disconnect fails pending calls and
  reconnect does not replay them.
- There is no broadcast, durable queue, scheduler, or implicit recipient
  selection.

The practical delivery guarantee is best-effort, at-most-once during one live
router connection. A durable exactly-once guarantee would require persistence
and provider-side idempotency and is outside the current scope.

## 6. Claude integration

### 6.1 Managed session: baseline

The baseline Claude connector owns a long-lived programmatic session through the
official Claude Agent SDK or its CLI streaming mode. Streaming input supports a
persistent interactive process, sequential messages, interrupts, and final
result messages. Because the connector owns the process, it can align provider
busy state, timeouts, and process disconnects with router semantics.

Official references:

- [Streaming input](https://code.claude.com/docs/en/agent-sdk/streaming-vs-single-mode)
- [Session resume](https://code.claude.com/docs/en/agent-sdk/sessions)
- [CLI programmatic mode](https://code.claude.com/docs/en/headless)

The first development-machine spike must choose between the SDK package and the
installed CLI transport. A dependency is added only if the SDK materially
improves lifecycle, approval, or typed message handling over the CLI.

### 6.2 Explicit live-session channel: optional

Claude Code Channels can push an event into a running session through an MCP
server and can expose a reply tool. A channel connector maps router delivery to:

```text
notifications/claude/channel
  content = prompt
  meta.request_id = router requestId
```

The local reply tool accepts `request_id` and `text`, verifies that the ID is
currently pending, and completes only that request.

Official references:

- [Push events with Channels](https://code.claude.com/docs/en/channels)
- [Channel notification and reply contract](https://code.claude.com/docs/en/channels-reference)

Channels remain optional because they are a research preview, require explicit
session and possibly organization opt-in, do not acknowledge model processing,
may silently drop events when disabled, and queue events while Claude is busy.
The connector therefore treats the reply tool as the only completion ACK and
uses timeout for non-delivery. A session not launched with the channel is
offline rather than being discovered or attached implicitly.

The official channel contract requires `@modelcontextprotocol/sdk`; that
dependency is justified only when this optional adapter is implemented.

## 7. Codex integration

The Codex connector owns an official App Server process and one selected thread.
Its minimum lifecycle is:

```text
initialize -> initialized
thread/start or thread/resume
turn/start(request content) -> turnId
item/* for the same threadId and turnId
turn/completed -> final adapter result
```

The connector maintains `requestId -> threadId/turnId`, accepts output only for
that turn, and finishes on `turn/completed`. It fails closed on unresolved
approval or user-input requests; it does not auto-approve provider actions.

Official reference:

- [Codex App Server](https://developers.openai.com/codex/app-server/)

The baseline target is a gateway-owned thread. Resuming a stored, inactive
thread can be tested separately. The design does not promise safe simultaneous
control of an arbitrary thread already open in another TUI or desktop process.

Codex also needs local outbound `agent_list` and `agent_send` tools. The stable
choice is a standard MCP server configured for the gateway-owned thread. When
the App Server host and MCP tool server are different processes, they communicate
with the connector over private loopback or local IPC using an ephemeral local
credential. Only the connector owns the central `agentId` registration.

## 8. Authentication and trust boundaries

| Boundary | Required behavior |
| --- | --- |
| Agent session -> local connector | Provider-supported tool/channel/SDK boundary; local IPC only when needed |
| Connector -> central router | Outbound WSS, per-agent credential, claimed `agentId` authorization |
| Router registry | One active inbound owner per `agentId`; no silent replacement |
| Router request | Authoritative `from`, recipient-bound reply, live duplicate rejection |
| Provider credentials | Remain on the agent system and never enter router messages or logs |
| Message content | Routed in memory; never logged; central router is trusted with plaintext |

Examples and tests use only neutral identifiers such as `local:reviewer`,
`local:worker-a`, and `host-a`. Runtime hostnames, addresses, usernames, paths,
tokens, and company identifiers must not be committed.

## 9. Implementation gates

Implementation proceeds in this order:

1. Prove the duplex gateway with mock sessions, including a nested
   coordinator -> worker A -> worker B -> worker A -> coordinator flow.
2. Implement and fake-test the Codex App Server transport and correlation.
3. Validate the Codex adapter against an installed CLI on a separate development
   machine.
4. Validate a managed Claude session; add the optional Channel adapter only if
   live-session injection is still required.
5. Add agent-facing MCP tools and local IPC where the provider requires a
   separate tool process.
6. Add WSS, per-agent authorization, heartbeat, and reconnect before enabling a
   non-loopback central deployment.

Provider adapters are not allowed to weaken router isolation, log content, copy
provider credentials to the central system, or introduce automatic replay.
