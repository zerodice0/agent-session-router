# Agent Session Router design

## 1. Purpose

Agent Session Router connects explicitly opted-in Claude, Codex, and future
provider sessions through one provider-neutral routing layer. An agent sends a
message to an `agentId`; the router resolves the currently connected gateway,
forwards the request, and returns only the correlated result.

AgentBridge informed the initial investigation but is not a runtime dependency.
This project uses provider-native integration boundaries and does not modify or
fork AgentBridge.

## 2. Goals

- Keep one active agent session per registered `agentId`.
- Route a request to one explicit recipient.
- Correlate the eventual result through a caller-provided `requestId`.
- Report offline, duplicate, unauthorized, and timeout states clearly.
- Bind to loopback by default for safe local integration testing.
- Avoid storing message content or environment-specific connection details.
- Let any connected agent use the same targeted send/list interface, not only a
  privileged coordinator.

## 3. Non-goals for the first version

- Group chat or broadcast delivery.
- Durable offline queues.
- Automatic task scheduling or load balancing.
- Shared state between router processes.
- Public internet exposure.
- Attaching to arbitrary provider sessions that were not launched by or
  explicitly configured for a gateway.
- Hiding provider approval or permission decisions behind automatic approval.

These features should only be added after a concrete test demonstrates that the
targeted request/reply flow is insufficient.

## 4. Architecture

```text
System A                                      Central router
+-------------------+                         +--------------------+
| Agent session A   |                         | registry + pending |
+---------+---------+                         | requests           |
          | local delivery/tools              |                    |
+---------v---------+ outbound WebSocket/WSS  |                    |
| Connector A       |------------------------>|                    |
+-------------------+                         |                    |
                                              |                    |
System B                                      |                    |
+-------------------+                         |                    |
| Agent session B   |                         |                    |
+---------+---------+                         |                    |
          | local delivery/tools              |                    |
+---------v---------+ outbound WebSocket/WSS  |                    |
| Connector B       |------------------------>|                    |
+-------------------+                         +--------------------+
```

There are no direct agent-to-agent sockets. Each system initiates one outbound
connection, and the router is the only component that resolves an `agentId` to
an active connection.

### 4.1 Router

The router owns two in-memory maps:

- `agentId -> active WebSocket`
- `requestId -> requester, recipient, timeout`

It never chooses a recipient implicitly. Every `send` message names exactly one
`agentId`.

### 4.2 Agent gateway

The agent gateway owns one logical session registration and has two local
directions:

- inbound: translate router `deliver` messages through one `SessionAdapter`;
- outbound: expose router discovery and targeted send operations as tools the
  local agent can call.

The included `GatewayClient` and mock adapter validate both directions. The
client waits for outbound `agents`, `result`, and `error` messages without
mixing them with inbound deliveries. `accepted` remains informational and does
not complete an outbound call.

The gateway registers one neutral identifier such as `local:reviewer`. Provider
session IDs, working directories, provider credentials, and local IPC details
remain in runtime configuration outside the repository and are never forwarded
to the router.

### 4.3 Agent-facing tool adapter

Every agent that needs to contact another agent receives the same minimal tool
surface:

```text
agent_list()
agent_send(target, prompt, timeoutMs?)
```

`agent_send` waits for the correlated final result. Asynchronous status polling
is not added unless a real provider workflow proves that a single bounded tool
call is insufficient. A coordinator is therefore an ordinary connected agent
with these tools, not a special router role.

Provider integration details are in
[provider-integration.md](provider-integration.md).

## 5. Protocol

Protocol messages are JSON objects with a string `type`. Registration includes a
numeric `protocolVersion`, currently `1`.

### 5.1 Client to router

- `register`: claim one `agentId` for the current connection.
- `list`: request the active agent list.
- `send`: deliver content to one active recipient.
- `reply`: complete one request previously delivered to this connection.
- `ping`: verify that the router is responsive.

### 5.2 Router to client

- `registered`: registration succeeded.
- `agents`: active agent descriptors.
- `accepted`: the request was delivered to the recipient socket.
- `deliver`: a recipient should process the request.
- `result`: successful or failed request completion.
- `error`: protocol or routing failure.
- `pong`: response to `ping`.

### 5.3 Request lifecycle

```text
requester            router                recipient
    | send              |                       |
    |------------------>|                       |
    |                   | deliver               |
    |                   |---------------------->|
    | accepted          |                       |
    |<------------------|                       |
    |                   | reply                 |
    |                   |<----------------------|
    | result            |                       |
    |<------------------|                       |
```

The router rejects duplicate live `requestId` values. A request is removed when
it receives one valid reply, times out, or either relevant socket disconnects.

An agent may send a child request while it is handling an inbound request. The
child uses a new `requestId`; the gateway caps its timeout to the remaining
parent budget when it can identify the parent request. The router still treats
the two requests independently.

## 6. Identity and addressing

An `agentId` is a routing identifier, not a hostname or process address. It must
match this pattern:

```text
^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$
```

Recommended neutral structure:

```text
<scope>:<role>
```

Examples:

- `local:coordinator`
- `local:reviewer`
- `lab:tester`

Duplicate live registrations are rejected. This avoids silent replacement and
makes session ownership visible.

## 7. Busy and failure behavior

- Unknown recipient: return `target_offline` immediately.
- Duplicate `agentId`: return `agent_conflict`.
- Duplicate `requestId`: return `request_conflict`.
- Recipient disconnect: return `target_disconnected` to the requester.
- Requester disconnect: cancel the pending request without retrying.
- Timeout: return `request_timeout` and discard a later reply.

The first version does not queue or retry. Delivery is best-effort and
at-most-once within one live request. Reconnect never replays an uncertain
delivery.

## 8. Security and privacy

- Default listener: `127.0.0.1` only.
- Optional shared token for local integration tests.
- Never log prompts, responses, tokens, credentials, provider session IDs, or
  environment-specific paths.
- Never place real infrastructure identifiers in source, tests, or examples.
- Reject a reply unless it comes from the connection that received the request.
- Use TLS and per-gateway credentials before enabling any non-loopback listener.

A multi-system deployment uses outbound authenticated gateway connections over
WSS. A credential must be scoped to the `agentId` and allowed operations it may
register; the current shared test token is not sufficient for that deployment.
Provider credentials stay on the agent system. The central router is trusted
with message plaintext unless a future end-to-end encryption layer is added.

## 9. Implementation phases

### Phase 1: local routing foundation

- WebSocket router
- in-memory registry
- targeted send/reply flow
- unit tests

### Phase 2: duplex agent gateway

- provider-neutral gateway client and mock session adapter
- outbound list/send request waiters
- nested agent-to-agent round-trip coverage
- router-to-session delivery, correlated completion, and busy translation

Implemented locally. Final test execution is required before release.

### Phase 3: provider-native adapters

- Codex App Server adapter for a gateway-owned thread (implemented with an
  injected transport; live CLI validation pending)
- Claude managed-session adapter
- optional Claude Channel adapter for explicitly opted-in live sessions
- provider approval and process-disconnect handling that fails closed

### Phase 4: agent-facing MCP tools

- expose `agent_list` and `agent_send` through each provider's supported local
  tool boundary
- use local IPC when a provider tool process and session host are separate
- keep provider credentials and central credentials out of model-visible input

### Phase 5: secure multi-system transport

- authenticated outbound gateway connections
- WSS and per-agent authorization
- heartbeat, readiness, and reconnect without automatic replay
- deployment configuration stored outside the repository

## 10. Validation plan

The first end-to-end test should run entirely on one development machine:

1. Start the router on loopback.
2. Register a coordinator and two mock workers.
3. Send requests to each worker by `agentId`.
4. Verify replies return only to the matching requester and `requestId`.
5. Verify duplicate registration, offline target, timeout, and disconnect paths.
6. Let worker A call worker B while A is processing an inbound request and
   verify the nested result returns only through A's original request.
7. Replace one mock worker with a provider-native adapter on a development
   machine.

Multi-host deployment is not required to validate the routing model.
