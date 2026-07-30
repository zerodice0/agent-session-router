import {
  query as claudeQuery,
  startup as claudeStartup,
  type Options,
  type SDKMessage,
} from "@anthropic-ai/claude-agent-sdk";
import type { SessionAdapter, SessionRequest, SessionResult } from "./session-adapter";

const DEFAULT_INITIALIZE_TIMEOUT_MS = 10_000;
const PROVIDER_TIMEOUT_MARGIN_MS = 100;

export interface ClaudeQueryHandle extends AsyncIterable<SDKMessage> {
  close(): void;
}

export interface ClaudeWarmQuery {
  query(prompt: string): ClaudeQueryHandle;
  close(): void;
}

export interface ClaudeSdkRuntime {
  query(params: { prompt: string; options: Options }): ClaudeQueryHandle;
  startup(params: { options: Options; initializeTimeoutMs: number }): Promise<ClaudeWarmQuery>;
}

export interface ClaudeAgentSdkAdapterOptions {
  baseOptions: Omit<Options, "abortController" | "resume" | "continue" | "forkSession">;
  resumeSessionId?: string;
  initializeTimeoutMs?: number;
  runtime?: ClaudeSdkRuntime;
}

interface ActiveQuery {
  requestId: string;
  controller: AbortController;
  handle: ClaudeQueryHandle | null;
  timedOut: boolean;
}

const DEFAULT_RUNTIME: ClaudeSdkRuntime = {
  query: (params) => claudeQuery(params),
  startup: (params) => claudeStartup(params),
};

/**
 * Owns one resumable Claude Agent SDK session. Each router delivery becomes
 * one SDK query, and only its terminal result is returned to the router.
 */
export class ClaudeAgentSdkAdapter implements SessionAdapter {
  readonly #baseOptions: ClaudeAgentSdkAdapterOptions["baseOptions"];
  readonly #runtime: ClaudeSdkRuntime;
  #sessionId: string | null;
  #warm: { handle: ClaudeWarmQuery; controller: AbortController } | null;
  #active: ActiveQuery | null = null;
  #closed = false;

  private constructor(
    options: ClaudeAgentSdkAdapterOptions,
    warm: ClaudeWarmQuery,
    warmController: AbortController,
  ) {
    this.#baseOptions = options.baseOptions;
    this.#runtime = options.runtime ?? DEFAULT_RUNTIME;
    this.#sessionId = options.resumeSessionId ?? null;
    this.#warm = { handle: warm, controller: warmController };
  }

  static async connect(options: ClaudeAgentSdkAdapterOptions): Promise<ClaudeAgentSdkAdapter> {
    const runtime = options.runtime ?? DEFAULT_RUNTIME;
    const controller = new AbortController();
    try {
      const warm = await runtime.startup({
        options: queryOptions(options.baseOptions, controller, options.resumeSessionId),
        initializeTimeoutMs: Math.max(
          1,
          Math.floor(options.initializeTimeoutMs ?? DEFAULT_INITIALIZE_TIMEOUT_MS),
        ),
      });
      return new ClaudeAgentSdkAdapter(options, warm, controller);
    } catch {
      controller.abort();
      throw new Error("Unable to initialize Claude Agent SDK adapter");
    }
  }

  get sessionId(): string | null {
    return this.#sessionId;
  }

  get ready(): boolean {
    return !this.#closed;
  }

  async handle(request: SessionRequest): Promise<SessionResult> {
    if (this.#closed) return { ok: false, error: "provider_not_ready" };
    if (this.#active) return { ok: false, error: "session_busy" };

    const warm = this.#warm;
    this.#warm = null;
    const controller = warm?.controller ?? new AbortController();
    const active: ActiveQuery = {
      requestId: request.requestId,
      controller,
      handle: null,
      timedOut: false,
    };
    this.#active = active;

    const timer = setTimeout(() => {
      if (this.#active !== active) return;
      active.timedOut = true;
      active.controller.abort();
      active.handle?.close();
    }, Math.max(1, request.timeoutMs - PROVIDER_TIMEOUT_MARGIN_MS));

    try {
      const handle = warm
        ? warm.handle.query(request.content)
        : this.#runtime.query({
            prompt: request.content,
            options: queryOptions(this.#baseOptions, controller, this.#sessionId ?? undefined),
          });
      active.handle = handle;

      for await (const message of handle) {
        if (message.type !== "result") continue;
        clearTimeout(timer);
        if (active.timedOut) return { ok: false, error: "request_timeout" };
        if (this.#closed) return { ok: false, error: "provider_disconnected" };
        if (!isSessionId(message.session_id)) {
          return { ok: false, error: "claude_protocol_error" };
        }
        this.#sessionId = message.session_id;
        return message.subtype === "success" && message.is_error === false
          ? { ok: true, content: message.result }
          : {
              ok: false,
              error:
                message.subtype === "success"
                  ? "claude_execution_error"
                  : mapClaudeErrorSubtype(message.subtype),
            };
      }

      if (active.timedOut) return { ok: false, error: "request_timeout" };
      if (this.#closed) return { ok: false, error: "provider_disconnected" };
      return { ok: false, error: "claude_no_result" };
    } catch {
      if (active.timedOut) return { ok: false, error: "request_timeout" };
      if (this.#closed) return { ok: false, error: "provider_disconnected" };
      return { ok: false, error: "claude_sdk_error" };
    } finally {
      clearTimeout(timer);
      active.handle?.close();
      if (this.#active === active) this.#active = null;
    }
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#warm) {
      this.#warm.controller.abort();
      this.#warm.handle.close();
      this.#warm = null;
    }
    if (this.#active) {
      this.#active.controller.abort();
      this.#active.handle?.close();
    }
  }
}

function queryOptions(
  baseOptions: ClaudeAgentSdkAdapterOptions["baseOptions"],
  abortController: AbortController,
  resume?: string,
): Options {
  return {
    ...baseOptions,
    abortController,
    ...(resume === undefined ? {} : { resume }),
  };
}

function isSessionId(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)
  );
}

function mapClaudeErrorSubtype(subtype: string): string {
  switch (subtype) {
    case "error_max_turns":
      return "claude_max_turns";
    case "error_max_budget_usd":
      return "claude_max_budget";
    case "error_max_structured_output_retries":
      return "claude_structured_output_error";
    default:
      return "claude_execution_error";
  }
}
