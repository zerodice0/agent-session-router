import type { CodexAppServerTransport } from "./codex-app-server-transport";
import type { SessionAdapter, SessionRequest, SessionResult } from "./session-adapter";

const DEFAULT_RPC_TIMEOUT_MS = 5_000;
const MAX_BUFFERED_EARLY_EVENTS = 256;

type JsonObject = Record<string, unknown>;
type JsonRpcId = number | string;

interface PendingRpc {
  resolve: (result: unknown) => void;
  reject: () => void;
  timer: ReturnType<typeof setTimeout>;
}

interface ActiveTurn {
  requestId: string;
  turnId: string | null;
  resolve: (result: SessionResult) => void;
  promise: Promise<SessionResult>;
  timer: ReturnType<typeof setTimeout>;
  finalMessages: string[];
  fallbackMessage: string | null;
  earlyEvents: JsonObject[];
}

export interface CodexClientInfo {
  name: string;
  title: string;
  version: string;
}

export type CodexThreadSelection =
  | { mode: "start"; params?: JsonObject }
  | { mode: "resume"; threadId: string; params?: JsonObject };

export interface CodexAppServerAdapterOptions {
  transport: CodexAppServerTransport;
  thread?: CodexThreadSelection;
  clientInfo?: CodexClientInfo;
  rpcTimeoutMs?: number;
}

/**
 * SessionAdapter for one gateway-owned Codex App Server thread. It uses only
 * the stable protocol surface and accepts one turn at a time.
 */
export class CodexAppServerAdapter implements SessionAdapter {
  readonly #transport: CodexAppServerTransport;
  readonly #thread: CodexThreadSelection;
  readonly #clientInfo: CodexClientInfo;
  readonly #rpcTimeoutMs: number;
  readonly #pendingRpcs = new Map<number, PendingRpc>();
  readonly #removeMessageListener: () => void;
  readonly #removeCloseListener: () => void;

  #nextRpcId = 1;
  #threadId: string | null = null;
  #activeTurn: ActiveTurn | null = null;
  #ready = false;
  #closed = false;

  private constructor(options: CodexAppServerAdapterOptions) {
    this.#transport = options.transport;
    this.#thread = options.thread ?? { mode: "start" };
    this.#clientInfo = options.clientInfo ?? {
      name: "agent_session_router",
      title: "Agent Session Router",
      version: "0.1.0",
    };
    this.#rpcTimeoutMs = Math.max(1, Math.floor(options.rpcTimeoutMs ?? DEFAULT_RPC_TIMEOUT_MS));
    this.#removeMessageListener = this.#transport.onMessage((message) =>
      this.#handleMessage(message),
    );
    this.#removeCloseListener = this.#transport.onClose(() => this.#handleTransportClose());
  }

  static async connect(options: CodexAppServerAdapterOptions): Promise<CodexAppServerAdapter> {
    const adapter = new CodexAppServerAdapter(options);
    try {
      await adapter.#initialize();
      return adapter;
    } catch {
      adapter.close();
      throw new Error("Unable to initialize Codex App Server adapter");
    }
  }

  get threadId(): string | null {
    return this.#threadId;
  }

  get ready(): boolean {
    return this.#ready && !this.#closed;
  }

  async handle(request: SessionRequest): Promise<SessionResult> {
    if (!this.ready || !this.#threadId) return { ok: false, error: "provider_not_ready" };
    if (this.#activeTurn) return { ok: false, error: "session_busy" };

    let resolveTurn!: (result: SessionResult) => void;
    const promise = new Promise<SessionResult>((resolve) => {
      resolveTurn = resolve;
    });
    let timeoutTimer!: ReturnType<typeof setTimeout>;
    const active: ActiveTurn = {
      requestId: request.requestId,
      turnId: null,
      resolve: resolveTurn,
      promise,
      timer: timeoutTimer,
      finalMessages: [],
      fallbackMessage: null,
      earlyEvents: [],
    };
    timeoutTimer = setTimeout(() => this.#timeoutTurn(active), request.timeoutMs);
    active.timer = timeoutTimer;
    this.#activeTurn = active;

    try {
      const response = await this.#request(
        "turn/start",
        {
          threadId: this.#threadId,
          input: [{ type: "text", text: request.content }],
        },
        Math.min(this.#rpcTimeoutMs, request.timeoutMs),
      );
      const turnId = readNestedString(response, "turn", "id");
      if (!turnId) throw new Error("Invalid turn/start response");

      if (this.#activeTurn !== active) {
        void this.#interrupt(turnId);
        return active.promise;
      }

      active.turnId = turnId;
      for (const event of active.earlyEvents.splice(0)) this.#handleNotification(event);
    } catch {
      if (this.#activeTurn === active) {
        this.#finishTurn(active, { ok: false, error: "codex_protocol_error" });
      }
    }

    return active.promise;
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#ready = false;
    this.#removeMessageListener();
    this.#removeCloseListener();
    this.#failPendingRpcs();
    if (this.#activeTurn) {
      this.#finishTurn(this.#activeTurn, { ok: false, error: "provider_disconnected" });
    }
    this.#transport.close();
  }

  async #initialize(): Promise<void> {
    await this.#request("initialize", { clientInfo: this.#clientInfo }, this.#rpcTimeoutMs);
    await this.#notify("initialized", {});

    const response =
      this.#thread.mode === "start"
        ? await this.#request("thread/start", this.#thread.params ?? {}, this.#rpcTimeoutMs)
        : await this.#request(
            "thread/resume",
            { ...(this.#thread.params ?? {}), threadId: this.#thread.threadId },
            this.#rpcTimeoutMs,
          );
    const threadId = readNestedString(response, "thread", "id");
    if (!threadId) throw new Error("Invalid thread response");
    this.#threadId = threadId;
    this.#ready = true;
  }

  #handleMessage(value: unknown): void {
    if (this.#closed || !isJsonObject(value)) return;
    const id = value.id;

    if (typeof value.method === "string") {
      if (isJsonRpcId(id)) {
        void this.#handleServerRequest(id, value.method);
      } else {
        this.#handleNotification(value);
      }
      return;
    }

    if (typeof id !== "number") return;
    const pending = this.#pendingRpcs.get(id);
    if (!pending) return;
    this.#pendingRpcs.delete(id);
    clearTimeout(pending.timer);
    const hasResult = Object.hasOwn(value, "result");
    const hasError = Object.hasOwn(value, "error");
    if (hasResult !== hasError && hasResult) pending.resolve(value.result);
    else pending.reject();
  }

  #handleNotification(message: JsonObject): void {
    const method = message.method;
    if (method !== "item/completed" && method !== "turn/completed") return;
    if (!isJsonObject(message.params)) return;

    const active = this.#activeTurn;
    if (!active || message.params.threadId !== this.#threadId) return;
    if (active.turnId === null) {
      if (active.earlyEvents.length >= MAX_BUFFERED_EARLY_EVENTS) {
        this.#finishTurn(active, { ok: false, error: "codex_protocol_error" });
        return;
      }
      active.earlyEvents.push(message);
      return;
    }

    if (method === "item/completed") {
      if (message.params.turnId !== active.turnId || !isJsonObject(message.params.item)) return;
      const item = message.params.item;
      if (item.type !== "agentMessage" || typeof item.text !== "string") return;
      if (item.phase === "final_answer") active.finalMessages.push(item.text);
      else if (item.phase === undefined) active.fallbackMessage = item.text;
      return;
    }

    if (!isJsonObject(message.params.turn)) return;
    const turn = message.params.turn;
    if (turn.id !== active.turnId || typeof turn.status !== "string") return;

    if (turn.status === "completed") {
      const content =
        active.finalMessages.length > 0
          ? active.finalMessages.join("\n")
          : active.fallbackMessage;
      this.#finishTurn(
        active,
        content === null
          ? { ok: false, error: "codex_no_final_response" }
          : { ok: true, content },
      );
    } else if (turn.status === "interrupted") {
      this.#finishTurn(active, { ok: false, error: "codex_turn_interrupted" });
    } else if (turn.status === "failed") {
      this.#finishTurn(active, { ok: false, error: "codex_turn_failed" });
    } else {
      this.#finishTurn(active, { ok: false, error: "codex_protocol_error" });
    }
  }

  async #handleServerRequest(id: JsonRpcId, method: string): Promise<void> {
    if (
      method === "item/commandExecution/requestApproval" ||
      method === "item/fileChange/requestApproval"
    ) {
      await this.#sendServerResponse(id, { decision: "decline" });
      return;
    }
    if (method === "item/permissions/requestApproval") {
      await this.#sendServerResponse(id, { permissions: [] });
      return;
    }
    if (method === "mcpServer/elicitation/request") {
      await this.#sendServerResponse(id, { action: "decline", content: null });
      return;
    }

    await this.#sendServerError(id, -32601, "Unsupported server request");
  }

  #timeoutTurn(active: ActiveTurn): void {
    if (this.#activeTurn !== active) return;
    const turnId = active.turnId;
    this.#finishTurn(active, { ok: false, error: "request_timeout" });
    if (turnId) void this.#interrupt(turnId);
  }

  async #interrupt(turnId: string): Promise<void> {
    if (!this.#threadId || this.#closed) return;
    try {
      await this.#request(
        "turn/interrupt",
        { threadId: this.#threadId, turnId },
        this.#rpcTimeoutMs,
      );
    } catch {
      // The caller has already received a terminal result; interruption is best effort.
    }
  }

  #finishTurn(active: ActiveTurn, result: SessionResult): void {
    if (this.#activeTurn !== active) return;
    this.#activeTurn = null;
    clearTimeout(active.timer);
    active.earlyEvents.length = 0;
    active.resolve(result);
  }

  #request(method: string, params: JsonObject, timeoutMs: number): Promise<unknown> {
    if (this.#closed) return Promise.reject(new Error("Codex transport is closed"));
    const id = this.#nextRpcId++;

    return new Promise((resolve, reject) => {
      const rejectPending = () => reject(new Error("Codex protocol request failed"));
      const timer = setTimeout(() => {
        if (!this.#pendingRpcs.delete(id)) return;
        rejectPending();
      }, Math.max(1, timeoutMs));
      this.#pendingRpcs.set(id, { resolve, reject: rejectPending, timer });

      void Promise.resolve(this.#transport.send({ method, id, params })).catch(() => {
        const pending = this.#pendingRpcs.get(id);
        if (!pending) return;
        this.#pendingRpcs.delete(id);
        clearTimeout(pending.timer);
        pending.reject();
      });
    });
  }

  async #notify(method: string, params: JsonObject): Promise<void> {
    if (this.#closed) throw new Error("Codex transport is closed");
    await this.#transport.send({ method, params });
  }

  async #sendServerResponse(id: JsonRpcId, result: JsonObject): Promise<void> {
    if (this.#closed) return;
    try {
      await this.#transport.send({ id, result });
    } catch {
      this.#handleTransportClose();
    }
  }

  async #sendServerError(id: JsonRpcId, code: number, message: string): Promise<void> {
    if (this.#closed) return;
    try {
      await this.#transport.send({ id, error: { code, message } });
    } catch {
      this.#handleTransportClose();
    }
  }

  #handleTransportClose(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#ready = false;
    this.#removeMessageListener();
    this.#removeCloseListener();
    this.#failPendingRpcs();
    if (this.#activeTurn) {
      this.#finishTurn(this.#activeTurn, { ok: false, error: "provider_disconnected" });
    }
  }

  #failPendingRpcs(): void {
    for (const [id, pending] of this.#pendingRpcs) {
      clearTimeout(pending.timer);
      pending.reject();
      this.#pendingRpcs.delete(id);
    }
  }
}

function isJsonObject(value: unknown): value is JsonObject {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isJsonRpcId(value: unknown): value is JsonRpcId {
  return typeof value === "number" || typeof value === "string";
}

function readNestedString(value: unknown, objectKey: string, stringKey: string): string | null {
  if (!isJsonObject(value) || !isJsonObject(value[objectKey])) return null;
  const nested = value[objectKey];
  return typeof nested[stringKey] === "string" ? nested[stringKey] : null;
}
