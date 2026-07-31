import type { AgentMessenger, AgentSendOptions, AgentSendResult } from "./agent-messenger";
import {
  DEFAULT_REQUEST_TIMEOUT_MS,
  PROTOCOL_VERSION,
  isAgentId,
  isRequestId,
  normalizeTimeoutMs,
  parseServerMessage,
  type AgentDescriptor,
  type ClientMessage,
  type RouterErrorCode,
  type ServerMessage,
} from "./protocol";
import type { SessionAdapter, SessionResult } from "./session-adapter";

export const DEFAULT_CONNECT_TIMEOUT_MS = 3_000;
export const DEFAULT_HEARTBEAT_INTERVAL_MS = 15_000;
export const DEFAULT_HEARTBEAT_TIMEOUT_MS = 5_000;
export const DEFAULT_RECONNECT_INITIAL_DELAY_MS = 250;
export const DEFAULT_RECONNECT_MAX_DELAY_MS = 10_000;
export const DEFAULT_RECONNECT_JITTER_RATIO = 0.2;
const LOCAL_TIMEOUT_GRACE_MS = 250;
const REQUEST_ID_ATTEMPTS = 16;

interface PendingList {
  resolve: (agents: AgentDescriptor[]) => void;
  reject: (error: GatewayRequestError) => void;
  timer: ReturnType<typeof setTimeout>;
}

interface PendingSend {
  resolve: (result: AgentSendResult) => void;
  timer: ReturnType<typeof setTimeout>;
}

interface PendingHeartbeat {
  requestId: string;
  timer: ReturnType<typeof setTimeout>;
}

export interface GatewayClientOptions {
  routerUrl: string;
  agent: AgentDescriptor;
  adapter: SessionAdapter;
  token?: string;
  delegationToken?: string;
  connectTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  reconnectInitialDelayMs?: number;
  reconnectMaxDelayMs?: number;
  reconnectJitterRatio?: number;
  random?: () => number;
  requestIdFactory?: () => string;
}

export class GatewayRequestError extends Error {
  constructor(readonly code: RouterErrorCode | "gateway_disconnected" | "gateway_not_connected") {
    super(code);
    this.name = "GatewayRequestError";
  }
}

class GatewayRegistrationError extends Error {
  constructor(readonly code: RouterErrorCode) {
    super(`Gateway registration failed: ${code}`);
    this.name = "GatewayRegistrationError";
  }
}

/**
 * Registers one named session with the router. Incoming deliveries are handled
 * by the provider adapter while outbound list/send calls remain independently
 * correlated, allowing an active worker to call another agent safely.
 */
export class GatewayClient implements AgentMessenger {
  readonly #routerUrl: string;
  readonly #agent: AgentDescriptor;
  readonly #adapter: SessionAdapter;
  readonly #token: string | undefined;
  readonly #delegationToken: string | undefined;
  readonly #connectTimeoutMs: number;
  readonly #heartbeatIntervalMs: number;
  readonly #heartbeatTimeoutMs: number;
  readonly #reconnectInitialDelayMs: number;
  readonly #reconnectMaxDelayMs: number;
  readonly #reconnectJitterRatio: number;
  readonly #random: () => number;
  readonly #requestIdFactory: () => string;

  readonly #pendingLists = new Map<string, PendingList>();
  readonly #pendingSends = new Map<string, PendingSend>();
  #socket: WebSocket | null = null;
  #registered = false;
  #connectPromise: Promise<void> | null = null;
  #heartbeatTimer: ReturnType<typeof setTimeout> | null = null;
  #pendingHeartbeat: PendingHeartbeat | null = null;
  #reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  #reconnectAttempt = 0;
  #reconnectEnabled = false;
  #hasConnected = false;
  #activeInboundRequestId: string | null = null;
  #activeInboundDeadline: number | null = null;

  constructor(options: GatewayClientOptions) {
    this.#routerUrl = options.routerUrl;
    this.#agent = options.agent;
    this.#adapter = options.adapter;
    this.#token = options.token;
    this.#delegationToken = options.delegationToken;
    this.#connectTimeoutMs = positiveInteger(options.connectTimeoutMs, DEFAULT_CONNECT_TIMEOUT_MS);
    this.#heartbeatIntervalMs = positiveInteger(
      options.heartbeatIntervalMs,
      DEFAULT_HEARTBEAT_INTERVAL_MS,
    );
    this.#heartbeatTimeoutMs = positiveInteger(
      options.heartbeatTimeoutMs,
      DEFAULT_HEARTBEAT_TIMEOUT_MS,
    );
    this.#reconnectInitialDelayMs = positiveInteger(
      options.reconnectInitialDelayMs,
      DEFAULT_RECONNECT_INITIAL_DELAY_MS,
    );
    this.#reconnectMaxDelayMs = Math.max(
      this.#reconnectInitialDelayMs,
      positiveInteger(options.reconnectMaxDelayMs, DEFAULT_RECONNECT_MAX_DELAY_MS),
    );
    const configuredJitter = options.reconnectJitterRatio ?? DEFAULT_RECONNECT_JITTER_RATIO;
    this.#reconnectJitterRatio = Number.isFinite(configuredJitter)
      ? Math.max(0, Math.min(configuredJitter, 1))
      : DEFAULT_RECONNECT_JITTER_RATIO;
    this.#random = options.random ?? Math.random;
    this.#requestIdFactory = options.requestIdFactory ?? (() => crypto.randomUUID());
  }

  get connected(): boolean {
    return this.#registered && this.#socket?.readyState === WebSocket.OPEN;
  }

  connect(): Promise<void> {
    this.#reconnectEnabled = true;
    if (this.connected) return Promise.resolve();
    return this.#beginConnectAttempt();
  }

  #beginConnectAttempt(): Promise<void> {
    if (this.#connectPromise) return this.#connectPromise;
    const attempt = this.#connectOnce();
    let tracked: Promise<void>;
    tracked = attempt.finally(() => {
      if (this.#connectPromise === tracked) this.#connectPromise = null;
      if (!this.connected) this.#scheduleReconnect();
    });
    this.#connectPromise = tracked;
    return tracked;
  }

  async #connectOnce(): Promise<void> {
    if (this.connected) return;
    if (this.#socket) throw new Error("Gateway connection is already in progress");

    const socket = new WebSocket(this.#routerUrl);
    this.#socket = socket;

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const timer = setTimeout(() => {
        finish(new Error("Timed out registering gateway with router"));
        socket.close();
      }, this.#connectTimeoutMs);

      const finish = (error?: Error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (error) {
          if (this.#socket === socket) this.#socket = null;
          reject(error);
        } else {
          resolve();
        }
      };

      socket.addEventListener("open", () => {
        this.#sendMessage(socket, {
          type: "register",
          protocolVersion: PROTOCOL_VERSION,
          agent: this.#agent,
          ...(this.#token === undefined ? {} : { token: this.#token }),
          ...(this.#delegationToken === undefined
            ? {}
            : { delegationToken: this.#delegationToken }),
        });
      });

      socket.addEventListener("message", (event) => {
        const message = parseServerMessage(event.data);
        if (!message) return;

        if (message.type === "registered") {
          if (
            message.agent.agentId !== this.#agent.agentId ||
            (message.role !== undefined && message.role !== "agent")
          ) {
            finish(new Error("Router registered an unexpected agent identifier"));
            socket.close();
            return;
          }
          this.#registered = true;
          this.#hasConnected = true;
          this.#reconnectAttempt = 0;
          this.#scheduleHeartbeat(socket);
          finish();
          return;
        }

        if (!settled && message.type === "error") {
          finish(new GatewayRegistrationError(message.code));
          socket.close();
          return;
        }

        this.#handleServerMessage(socket, message);
      });

      socket.addEventListener("error", () => {
        if (!settled) finish(new Error("Unable to connect gateway to router"));
      });

      socket.addEventListener("close", () => {
        if (this.#socket === socket) {
          this.#markDisconnected();
          if (settled) this.#scheduleReconnect();
        }
        if (!settled) finish(new Error("Router closed before gateway registration completed"));
      });
    });
  }

  disconnect(): void {
    this.#reconnectEnabled = false;
    this.#clearReconnectTimer();
    const socket = this.#socket;
    this.#markDisconnected();
    socket?.close();
  }

  listAgents(requestId?: string): Promise<AgentDescriptor[]> {
    const socket = this.#connectedSocket();
    const resolvedRequestId = this.#reserveRequestId(requestId);

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        if (!this.#pendingLists.delete(resolvedRequestId)) return;
        reject(new GatewayRequestError("request_timeout"));
      }, DEFAULT_REQUEST_TIMEOUT_MS);

      this.#pendingLists.set(resolvedRequestId, { resolve, reject, timer });
      this.#sendMessage(socket, { type: "list", requestId: resolvedRequestId });
    });
  }

  send(target: string, content: string, options: AgentSendOptions = {}): Promise<AgentSendResult> {
    const socket = this.#connectedSocket();
    if (!isAgentId(target)) throw new TypeError("Invalid target agent identifier");
    if (typeof content !== "string" || content.length === 0) {
      throw new TypeError("Message content must not be empty");
    }

    const requestId = this.#reserveRequestId(options.requestId);
    const requestedTimeoutMs = normalizeTimeoutMs(options.timeoutMs);
    const remainingInboundMs =
      this.#activeInboundDeadline === null
        ? requestedTimeoutMs
        : Math.max(1, this.#activeInboundDeadline - Date.now());
    const timeoutMs = Math.min(requestedTimeoutMs, remainingInboundMs);

    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        if (!this.#pendingSends.delete(requestId)) return;
        resolve({ requestId, ok: false, error: "request_timeout" });
      }, timeoutMs + LOCAL_TIMEOUT_GRACE_MS);

      this.#pendingSends.set(requestId, { resolve, timer });
      this.#sendMessage(socket, {
        type: "send",
        requestId,
        to: target,
        content,
        timeoutMs,
      });
    });
  }

  #handleServerMessage(socket: WebSocket, message: ServerMessage): void {
    switch (message.type) {
      case "agents": {
        const pending = this.#pendingLists.get(message.requestId);
        if (!pending) return;
        this.#pendingLists.delete(message.requestId);
        clearTimeout(pending.timer);
        pending.resolve(message.agents);
        return;
      }
      case "result": {
        const pending = this.#pendingSends.get(message.requestId);
        if (!pending) return;
        this.#pendingSends.delete(message.requestId);
        clearTimeout(pending.timer);
        pending.resolve(
          message.ok
            ? {
                requestId: message.requestId,
                from: message.from,
                ok: true,
                content: message.content ?? "",
              }
            : {
                requestId: message.requestId,
                from: message.from,
                ok: false,
                error: message.error ?? "session_adapter_failed",
              },
        );
        return;
      }
      case "error":
        this.#handleRequestError(message);
        return;
      case "deliver":
        void this.#handleDelivery(socket, message);
        return;
      case "pong":
        this.#handlePong(socket, message.requestId);
        return;
      default:
        return;
    }
  }

  #handleRequestError(message: Extract<ServerMessage, { type: "error" }>): void {
    if (!message.requestId) return;

    const list = this.#pendingLists.get(message.requestId);
    if (list) {
      this.#pendingLists.delete(message.requestId);
      clearTimeout(list.timer);
      list.reject(new GatewayRequestError(message.code));
      return;
    }

    const send = this.#pendingSends.get(message.requestId);
    if (!send) return;
    this.#pendingSends.delete(message.requestId);
    clearTimeout(send.timer);
    send.resolve({ requestId: message.requestId, ok: false, error: message.code });
  }

  async #handleDelivery(
    socket: WebSocket,
    message: Extract<ServerMessage, { type: "deliver" }>,
  ): Promise<void> {
    if (socket !== this.#socket || !this.#registered) return;

    if (this.#activeInboundRequestId !== null) {
      this.#sendReply(socket, message.requestId, { ok: false, error: "session_busy" });
      return;
    }

    const inboundTimeoutMs =
      message.timeoutMs === undefined
        ? DEFAULT_REQUEST_TIMEOUT_MS
        : normalizeTimeoutMs(message.timeoutMs);
    this.#activeInboundRequestId = message.requestId;
    this.#activeInboundDeadline = Date.now() + inboundTimeoutMs;
    let result: SessionResult;
    try {
      result = await this.#adapter.handle({
        requestId: message.requestId,
        from: message.from,
        content: message.content,
        timeoutMs: inboundTimeoutMs,
      });
    } catch {
      result = { ok: false, error: "session_adapter_failed" };
    } finally {
      if (this.#activeInboundRequestId === message.requestId) {
        this.#activeInboundRequestId = null;
        this.#activeInboundDeadline = null;
      }
    }

    if (socket === this.#socket && this.#registered) {
      this.#sendReply(socket, message.requestId, result);
    }
  }

  #sendReply(socket: WebSocket, requestId: string, result: SessionResult): void {
    this.#sendMessage(socket, {
      type: "reply",
      requestId,
      ok: result.ok,
      ...(result.ok ? { content: result.content } : { error: result.error }),
    });
  }

  #connectedSocket(): WebSocket {
    if (!this.connected || !this.#socket) throw new GatewayRequestError("gateway_not_connected");
    return this.#socket;
  }

  #reserveRequestId(explicit?: string): string {
    if (explicit !== undefined) {
      if (!isRequestId(explicit)) throw new TypeError("Invalid request identifier");
      if (this.#requestIdInUse(explicit)) throw new Error("Request identifier is already in use");
      return explicit;
    }

    for (let attempt = 0; attempt < REQUEST_ID_ATTEMPTS; attempt += 1) {
      const candidate = this.#requestIdFactory();
      if (isRequestId(candidate) && !this.#requestIdInUse(candidate)) return candidate;
    }
    throw new Error("Unable to allocate a unique request identifier");
  }

  #requestIdInUse(requestId: string): boolean {
    return (
      requestId === this.#activeInboundRequestId ||
      this.#pendingLists.has(requestId) ||
      this.#pendingSends.has(requestId)
    );
  }

  #sendMessage(socket: WebSocket, message: ClientMessage): void {
    if (socket.readyState !== WebSocket.OPEN) return;
    socket.send(JSON.stringify(message));
  }

  #scheduleHeartbeat(socket: WebSocket): void {
    this.#clearHeartbeat();
    this.#heartbeatTimer = setTimeout(() => {
      this.#heartbeatTimer = null;
      if (socket !== this.#socket || !this.connected) return;

      const requestId = `heartbeat:${crypto.randomUUID()}`;
      const timer = setTimeout(() => {
        if (this.#pendingHeartbeat?.requestId !== requestId) return;
        this.#pendingHeartbeat = null;
        if (socket !== this.#socket) return;
        this.#markDisconnected();
        socket.close();
        this.#scheduleReconnect();
      }, this.#heartbeatTimeoutMs);
      this.#pendingHeartbeat = { requestId, timer };
      this.#sendMessage(socket, { type: "ping", requestId });
    }, this.#heartbeatIntervalMs);
  }

  #handlePong(socket: WebSocket, requestId: string): void {
    const pending = this.#pendingHeartbeat;
    if (socket !== this.#socket || !pending || pending.requestId !== requestId) return;
    clearTimeout(pending.timer);
    this.#pendingHeartbeat = null;
    this.#scheduleHeartbeat(socket);
  }

  #clearHeartbeat(): void {
    if (this.#heartbeatTimer) clearTimeout(this.#heartbeatTimer);
    this.#heartbeatTimer = null;
    if (this.#pendingHeartbeat) clearTimeout(this.#pendingHeartbeat.timer);
    this.#pendingHeartbeat = null;
  }

  #scheduleReconnect(): void {
    if (
      !this.#reconnectEnabled ||
      !this.#hasConnected ||
      this.connected ||
      this.#socket ||
      this.#connectPromise ||
      this.#reconnectTimer
    ) {
      return;
    }

    const exponentialDelay = Math.min(
      this.#reconnectInitialDelayMs * 2 ** this.#reconnectAttempt,
      this.#reconnectMaxDelayMs,
    );
    this.#reconnectAttempt += 1;
    const randomSample = this.#random();
    const random = Number.isFinite(randomSample)
      ? Math.max(0, Math.min(randomSample, 1))
      : 0.5;
    const jitter = (random * 2 - 1) * this.#reconnectJitterRatio;
    const delay = Math.max(1, Math.floor(exponentialDelay * (1 + jitter)));
    this.#reconnectTimer = setTimeout(() => {
      this.#reconnectTimer = null;
      void this.#reconnect().catch(() => {});
    }, delay);
  }

  async #reconnect(): Promise<void> {
    if (!this.#reconnectEnabled || this.connected) return;
    try {
      await this.#beginConnectAttempt();
    } catch (error) {
      if (error instanceof GatewayRegistrationError && error.code === "unauthorized") {
        this.#reconnectEnabled = false;
        this.#clearReconnectTimer();
        return;
      }
      this.#scheduleReconnect();
    }
  }

  #clearReconnectTimer(): void {
    if (this.#reconnectTimer) clearTimeout(this.#reconnectTimer);
    this.#reconnectTimer = null;
  }

  #markDisconnected(): void {
    this.#clearHeartbeat();
    this.#socket = null;
    this.#registered = false;
    this.#activeInboundRequestId = null;
    this.#activeInboundDeadline = null;

    for (const [requestId, pending] of this.#pendingLists) {
      clearTimeout(pending.timer);
      pending.reject(new GatewayRequestError("gateway_disconnected"));
      this.#pendingLists.delete(requestId);
    }
    for (const [requestId, pending] of this.#pendingSends) {
      clearTimeout(pending.timer);
      pending.resolve({ requestId, ok: false, error: "gateway_disconnected" });
      this.#pendingSends.delete(requestId);
    }
  }
}

function positiveInteger(value: number | undefined, fallback: number): number {
  if (value === undefined || !Number.isFinite(value)) return fallback;
  return Math.max(1, Math.floor(value));
}
