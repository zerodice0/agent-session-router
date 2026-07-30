import type { AgentMessenger, AgentSendOptions, AgentSendResult } from "./agent-messenger";
import {
  DEFAULT_REQUEST_TIMEOUT_MS,
  PROTOCOL_VERSION,
  isAgentId,
  isDelegationToken,
  isRequestId,
  normalizeTimeoutMs,
  parseServerMessage,
  type AgentDescriptor,
  type ClientMessage,
  type RouterErrorCode,
  type ServerMessage,
} from "./protocol";

const DEFAULT_CONNECT_TIMEOUT_MS = 3_000;
const LOCAL_TIMEOUT_GRACE_MS = 250;
const REQUEST_ID_ATTEMPTS = 16;

interface PendingList {
  resolve: (agents: AgentDescriptor[]) => void;
  reject: (error: DelegatedMessengerError) => void;
  timer: ReturnType<typeof setTimeout>;
}

interface PendingSend {
  resolve: (result: AgentSendResult) => void;
  timer: ReturnType<typeof setTimeout>;
}

export interface DelegatedMessengerClientOptions {
  routerUrl: string;
  agentId: string;
  delegationToken: string;
  connectTimeoutMs?: number;
  requestIdFactory?: () => string;
}

export class DelegatedMessengerError extends Error {
  constructor(readonly code: RouterErrorCode | "gateway_disconnected" | "gateway_not_connected") {
    super(code);
    this.name = "DelegatedMessengerError";
  }
}

/**
 * Outbound-only router connection delegated by one registered gateway. It can
 * list agents and send correlated requests as that gateway, but never receives
 * deliveries or replies to requests.
 */
export class DelegatedMessengerClient implements AgentMessenger {
  readonly #routerUrl: string;
  readonly #agentId: string;
  readonly #delegationToken: string;
  readonly #connectTimeoutMs: number;
  readonly #requestIdFactory: () => string;
  readonly #pendingLists = new Map<string, PendingList>();
  readonly #pendingSends = new Map<string, PendingSend>();
  #socket: WebSocket | null = null;
  #registered = false;

  constructor(options: DelegatedMessengerClientOptions) {
    if (!isAgentId(options.agentId)) throw new TypeError("Invalid delegated agent identifier");
    if (!isDelegationToken(options.delegationToken)) {
      throw new TypeError("Invalid delegation token");
    }
    this.#routerUrl = options.routerUrl;
    this.#agentId = options.agentId;
    this.#delegationToken = options.delegationToken;
    this.#connectTimeoutMs = options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
    this.#requestIdFactory = options.requestIdFactory ?? (() => crypto.randomUUID());
  }

  get connected(): boolean {
    return this.#registered && this.#socket?.readyState === WebSocket.OPEN;
  }

  async connect(): Promise<void> {
    if (this.connected) return;
    if (this.#socket) throw new Error("Delegate connection is already in progress");

    const socket = new WebSocket(this.#routerUrl);
    this.#socket = socket;

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const timer = setTimeout(() => {
        finish(new Error("Timed out registering delegated messenger"));
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
          type: "register_delegate",
          protocolVersion: PROTOCOL_VERSION,
          agentId: this.#agentId,
          delegationToken: this.#delegationToken,
        });
      });

      socket.addEventListener("message", (event) => {
        const message = parseServerMessage(event.data);
        if (!message) return;

        if (message.type === "registered") {
          if (
            message.agent.agentId !== this.#agentId ||
            (message.role !== undefined && message.role !== "delegate")
          ) {
            finish(new Error("Router registered an unexpected delegated identity"));
            socket.close();
            return;
          }
          this.#registered = true;
          finish();
          return;
        }

        if (!settled && message.type === "error") {
          finish(new Error(`Delegate registration failed: ${message.code}`));
          socket.close();
          return;
        }

        this.#handleServerMessage(message);
      });

      socket.addEventListener("error", () => {
        if (!settled) finish(new Error("Unable to connect delegated messenger"));
      });

      socket.addEventListener("close", () => {
        if (this.#socket === socket) this.#markDisconnected();
        if (!settled) finish(new Error("Router closed before delegate registration completed"));
      });
    });
  }

  disconnect(): void {
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
        reject(new DelegatedMessengerError("request_timeout"));
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
    const timeoutMs = normalizeTimeoutMs(options.timeoutMs);

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

  #handleServerMessage(message: ServerMessage): void {
    if (message.type === "agents") {
      const pending = this.#pendingLists.get(message.requestId);
      if (!pending) return;
      this.#pendingLists.delete(message.requestId);
      clearTimeout(pending.timer);
      pending.resolve(message.agents);
      return;
    }

    if (message.type === "result") {
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

    if (message.type !== "error" || !message.requestId) return;
    const list = this.#pendingLists.get(message.requestId);
    if (list) {
      this.#pendingLists.delete(message.requestId);
      clearTimeout(list.timer);
      list.reject(new DelegatedMessengerError(message.code));
      return;
    }

    const send = this.#pendingSends.get(message.requestId);
    if (!send) return;
    this.#pendingSends.delete(message.requestId);
    clearTimeout(send.timer);
    send.resolve({ requestId: message.requestId, ok: false, error: message.code });
  }

  #reserveRequestId(requestId?: string): string {
    if (requestId !== undefined) {
      if (!isRequestId(requestId)) throw new TypeError("Invalid request identifier");
      if (this.#requestIdInUse(requestId)) throw new TypeError("Request identifier is already active");
      return requestId;
    }

    for (let attempt = 0; attempt < REQUEST_ID_ATTEMPTS; attempt += 1) {
      const generated = this.#requestIdFactory();
      if (!isRequestId(generated)) throw new TypeError("Request ID factory returned an invalid value");
      if (!this.#requestIdInUse(generated)) return generated;
    }
    throw new Error("Unable to allocate a unique request identifier");
  }

  #requestIdInUse(requestId: string): boolean {
    return this.#pendingLists.has(requestId) || this.#pendingSends.has(requestId);
  }

  #connectedSocket(): WebSocket {
    if (!this.connected || !this.#socket) {
      throw new DelegatedMessengerError("gateway_not_connected");
    }
    return this.#socket;
  }

  #sendMessage(socket: WebSocket, message: ClientMessage): void {
    socket.send(JSON.stringify(message));
  }

  #markDisconnected(): void {
    this.#registered = false;
    this.#socket = null;
    for (const pending of this.#pendingLists.values()) {
      clearTimeout(pending.timer);
      pending.reject(new DelegatedMessengerError("gateway_disconnected"));
    }
    this.#pendingLists.clear();
    for (const [requestId, pending] of this.#pendingSends) {
      clearTimeout(pending.timer);
      pending.resolve({ requestId, ok: false, error: "gateway_disconnected" });
    }
    this.#pendingSends.clear();
  }
}
