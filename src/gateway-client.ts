import {
  DEFAULT_REQUEST_TIMEOUT_MS,
  PROTOCOL_VERSION,
  normalizeTimeoutMs,
  type AgentDescriptor,
  type ServerMessage,
} from "./protocol";
import type { SessionAdapter, SessionResult } from "./session-adapter";

const DEFAULT_CONNECT_TIMEOUT_MS = 3_000;

export interface GatewayClientOptions {
  routerUrl: string;
  agent: AgentDescriptor;
  adapter: SessionAdapter;
  token?: string;
  connectTimeoutMs?: number;
}

/**
 * Registers one named session with the router and adapts targeted deliveries to
 * a provider-specific SessionAdapter. One inbound request is active at a time
 * so provider turns cannot be mixed or implicitly queued.
 */
export class GatewayClient {
  readonly #routerUrl: string;
  readonly #agent: AgentDescriptor;
  readonly #adapter: SessionAdapter;
  readonly #token: string | undefined;
  readonly #connectTimeoutMs: number;

  #socket: WebSocket | null = null;
  #registered = false;
  #activeRequestId: string | null = null;

  constructor(options: GatewayClientOptions) {
    this.#routerUrl = options.routerUrl;
    this.#agent = options.agent;
    this.#adapter = options.adapter;
    this.#token = options.token;
    this.#connectTimeoutMs = options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
  }

  get connected(): boolean {
    return this.#registered && this.#socket?.readyState === WebSocket.OPEN;
  }

  async connect(): Promise<void> {
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
        socket.send(
          JSON.stringify({
            type: "register",
            protocolVersion: PROTOCOL_VERSION,
            agent: this.#agent,
            ...(this.#token === undefined ? {} : { token: this.#token }),
          }),
        );
      });

      socket.addEventListener("message", (event) => {
        const message = parseServerMessage(event.data);
        if (!message) return;

        if (message.type === "registered") {
          if (message.agent.agentId !== this.#agent.agentId) {
            finish(new Error("Router registered an unexpected agent identifier"));
            socket.close();
            return;
          }
          this.#registered = true;
          finish();
          return;
        }

        if (!settled && message.type === "error") {
          finish(new Error(`Gateway registration failed: ${message.code}`));
          socket.close();
          return;
        }

        if (message.type === "deliver") {
          void this.#handleDelivery(socket, message);
        }
      });

      socket.addEventListener("error", () => {
        if (!settled) finish(new Error("Unable to connect gateway to router"));
      });

      socket.addEventListener("close", () => {
        if (this.#socket === socket) {
          this.#socket = null;
          this.#registered = false;
          this.#activeRequestId = null;
        }
        if (!settled) finish(new Error("Router closed before gateway registration completed"));
      });
    });
  }

  disconnect(): void {
    const socket = this.#socket;
    this.#socket = null;
    this.#registered = false;
    this.#activeRequestId = null;
    socket?.close();
  }

  async #handleDelivery(
    socket: WebSocket,
    message: Extract<ServerMessage, { type: "deliver" }>,
  ): Promise<void> {
    if (socket !== this.#socket || !this.#registered) return;

    if (this.#activeRequestId !== null) {
      this.#sendReply(socket, message.requestId, { ok: false, error: "session_busy" });
      return;
    }

    this.#activeRequestId = message.requestId;
    let result: SessionResult;
    try {
      result = await this.#adapter.handle({
        requestId: message.requestId,
        from: message.from,
        content: message.content,
        timeoutMs:
          message.timeoutMs === undefined
            ? DEFAULT_REQUEST_TIMEOUT_MS
            : normalizeTimeoutMs(message.timeoutMs),
      });
    } catch {
      result = { ok: false, error: "session_adapter_failed" };
    } finally {
      if (this.#activeRequestId === message.requestId) this.#activeRequestId = null;
    }

    if (socket === this.#socket && this.#registered) {
      this.#sendReply(socket, message.requestId, result);
    }
  }

  #sendReply(socket: WebSocket, requestId: string, result: SessionResult): void {
    if (socket.readyState !== WebSocket.OPEN) return;
    socket.send(
      JSON.stringify({
        type: "reply",
        requestId,
        ok: result.ok,
        ...(result.ok ? { content: result.content } : { error: result.error }),
      }),
    );
  }
}

function parseServerMessage(raw: unknown): ServerMessage | null {
  if (typeof raw !== "string") return null;
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || typeof (parsed as { type?: unknown }).type !== "string") {
      return null;
    }
    return parsed as ServerMessage;
  } catch {
    return null;
  }
}
