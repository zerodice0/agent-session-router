import {
  normalizeTimeoutMs,
  parseClientMessage,
  type RouterErrorCode,
  type ServerMessage,
} from "./protocol";
import {
  AgentRegistry,
  sendMessage,
  type AgentConnection,
  type RegisteredConnection,
} from "./registry";

interface SocketData {
  connectedAt: number;
}

interface PendingRequest {
  requestId: string;
  requester: AgentConnection;
  requesterId: string;
  recipient: AgentConnection;
  recipientId: string;
  deadlineAt: number;
  timer: ReturnType<typeof setTimeout>;
}

export interface RouterOptions {
  hostname?: string;
  port?: number;
  /** null explicitly disables an inherited ROUTER_TOKEN for in-process tests. */
  token?: string | null;
  logEvents?: boolean;
}

function parsePort(value: string | undefined): number {
  if (value === undefined) return 8787;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 65_535) {
    throw new Error("ROUTER_PORT must be an integer between 1 and 65535");
  }
  return parsed;
}

export function startRouter(options: RouterOptions = {}) {
  const registry = new AgentRegistry();
  const pendingRequests = new Map<string, PendingRequest>();
  const hostname = options.hostname?.trim() || "127.0.0.1";
  const port = options.port ?? parsePort(process.env.ROUTER_PORT);
  const expectedToken = options.token === null ? undefined : options.token ?? process.env.ROUTER_TOKEN;
  const logEvents = options.logEvents ?? false;

  function sendError(
    connection: AgentConnection,
    code: RouterErrorCode,
    message: string,
    requestId?: string,
  ): void {
    sendMessage(connection, { type: "error", code, message, requestId });
  }

  function requireRegistered(
    connection: AgentConnection,
    requestId?: string,
  ): RegisteredConnection | null {
    const registered = registry.getByConnection(connection);
    if (registered) return registered;
    sendError(connection, "not_registered", "Register before using the router", requestId);
    return null;
  }

  function finishRequest(requestId: string, message: ServerMessage): void {
    const pending = pendingRequests.get(requestId);
    if (!pending) return;
    clearTimeout(pending.timer);
    pendingRequests.delete(requestId);
    refreshAgentStatus(pending.recipientId);
    sendMessage(pending.requester, message);
  }

  function refreshAgentStatus(agentId: string): void {
    const busy = [...pendingRequests.values()].some(({ recipientId }) => recipientId === agentId);
    registry.setStatus(agentId, busy ? "busy" : "idle");
  }

  function handleDisconnect(connection: AgentConnection): void {
    const disconnected = registry.unregister(connection);
    const disconnectedConnections = new Set([
      connection,
      ...(disconnected?.delegates ?? []),
    ]);

    for (const [requestId, pending] of pendingRequests) {
      if (disconnectedConnections.has(pending.requester)) {
        clearTimeout(pending.timer);
        pendingRequests.delete(requestId);
        refreshAgentStatus(pending.recipientId);
        continue;
      }

      if (disconnectedConnections.has(pending.recipient)) {
        finishRequest(requestId, {
          type: "error",
          requestId,
          code: "target_disconnected",
          message: `Target disconnected: ${pending.recipientId}`,
        });
      }
    }

    for (const delegate of disconnected?.delegates ?? []) {
      sendError(delegate, "not_registered", "Delegated registration ended");
      delegate.close?.();
    }

    if (logEvents && disconnected?.role === "agent") {
      console.info(`agent disconnected: ${disconnected.descriptor.agentId}`);
    }
  }

  function handleMessage(connection: AgentConnection, raw: string): void {
    const message = parseClientMessage(raw);
    if (!message) {
      sendError(connection, "invalid_message", "Message does not match protocol version 1");
      return;
    }

    if (message.type === "register_delegate") {
      if (registry.getByConnection(connection)) {
        sendError(connection, "agent_conflict", "This connection is already registered");
        return;
      }
      if (!registry.registerDelegate(message.agentId, message.delegationToken, connection)) {
        sendError(connection, "unauthorized", "Delegate registration rejected");
        return;
      }
      const registered = registry.getByConnection(connection);
      if (!registered) {
        sendError(connection, "unauthorized", "Delegate registration rejected");
        return;
      }
      sendMessage(connection, {
        type: "registered",
        agent: registered.descriptor,
        role: "delegate",
      });
      return;
    }

    if (message.type === "register") {
      if (registry.getByConnection(connection)) {
        sendError(connection, "agent_conflict", "This connection is already registered");
        return;
      }
      if (expectedToken !== undefined && message.token !== expectedToken) {
        sendError(connection, "unauthorized", "Registration token is invalid");
        return;
      }
      if (!registry.register(message.agent, connection, message.delegationToken)) {
        sendError(connection, "agent_conflict", `Agent is already connected: ${message.agent.agentId}`);
        return;
      }
      const registered = registry.get(message.agent.agentId);
      if (!registered) {
        sendError(connection, "not_registered", "Registration did not complete");
        return;
      }
      sendMessage(connection, { type: "registered", agent: registered.descriptor, role: "agent" });
      if (logEvents) console.info(`agent registered: ${message.agent.agentId}`);
      return;
    }

    const sender = requireRegistered(connection, "requestId" in message ? message.requestId : undefined);
    if (!sender) return;

    switch (message.type) {
      case "list":
        sendMessage(connection, { type: "agents", requestId: message.requestId, agents: registry.list() });
        return;
      case "ping":
        sendMessage(connection, { type: "pong", requestId: message.requestId });
        return;
      case "send": {
        if (pendingRequests.has(message.requestId)) {
          sendError(connection, "request_conflict", "Request ID is already active", message.requestId);
          return;
        }
        const target = registry.get(message.to);
        if (!target) {
          sendError(connection, "target_offline", `Target is not connected: ${message.to}`, message.requestId);
          return;
        }
        const requestedTimeoutMs = normalizeTimeoutMs(message.timeoutMs);
        const parent = registry.get(sender.descriptor.agentId);
        let timeoutMs = requestedTimeoutMs;
        if (parent) {
          for (const pending of pendingRequests.values()) {
            if (pending.recipient !== parent.connection) continue;
            timeoutMs = Math.min(timeoutMs, Math.max(1, pending.deadlineAt - Date.now()));
          }
        }
        const deadlineAt = Date.now() + timeoutMs;
        const timer = setTimeout(() => {
          finishRequest(message.requestId, {
            type: "error",
            requestId: message.requestId,
            code: "request_timeout",
            message: `Request timed out after ${timeoutMs}ms`,
          });
        }, timeoutMs);
        pendingRequests.set(message.requestId, {
          requestId: message.requestId,
          requester: connection,
          requesterId: sender.descriptor.agentId,
          recipient: target.connection,
          recipientId: target.descriptor.agentId,
          deadlineAt,
          timer,
        });
        refreshAgentStatus(target.descriptor.agentId);
        sendMessage(target.connection, {
          type: "deliver",
          requestId: message.requestId,
          from: sender.descriptor.agentId,
          content: message.content,
          timeoutMs,
        });
        sendMessage(connection, { type: "accepted", requestId: message.requestId, to: message.to });
        return;
      }
      case "reply": {
        if (sender.role === "delegate") {
          sendError(connection, "reply_forbidden", "Delegates cannot reply", message.requestId);
          return;
        }
        const pending = pendingRequests.get(message.requestId);
        if (!pending) {
          sendError(connection, "request_not_found", "Request is no longer active", message.requestId);
          return;
        }
        if (pending.recipient !== connection) {
          sendError(connection, "reply_forbidden", "Only the request recipient can reply", message.requestId);
          return;
        }
        finishRequest(message.requestId, {
          type: "result",
          requestId: message.requestId,
          from: sender.descriptor.agentId,
          ok: message.ok,
          content: message.content,
          error: message.error,
        });
      }
    }
  }

  return Bun.serve<SocketData>({
    hostname,
    port,
    fetch(request, server) {
      const url = new URL(request.url);
      if (url.pathname === "/healthz") {
        return Response.json({ status: "ok", connectedAgents: registry.list().length });
      }
      if (url.pathname === "/ws" && server.upgrade(request, { data: { connectedAt: Date.now() } })) {
        return undefined;
      }
      return new Response("Not found", { status: 404 });
    },
    websocket: {
      message(socket, message) {
        const raw = typeof message === "string" ? message : new TextDecoder().decode(message);
        handleMessage(socket, raw);
      },
      close(socket) {
        handleDisconnect(socket);
      },
    },
  });
}

if (import.meta.main) {
  const server = startRouter({
    hostname: process.env.ROUTER_HOST?.trim() || "127.0.0.1",
    logEvents: true,
  });
  console.info(`agent-session-router listening on ws://${server.hostname}:${server.port}/ws`);
}
