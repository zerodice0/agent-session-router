import {
  query as claudeQuery,
  startup as claudeStartup,
  type Options,
  type SDKMessage,
} from "@anthropic-ai/claude-agent-sdk";
import { pathToFileURL } from "node:url";
import { resolve } from "node:path";
import { z } from "zod";

export const MAX_FRAME_BYTES = 1024 * 1024;
const PROVIDER_TIMEOUT_MARGIN_MS = 100;
const MCP_TOOL_TIMEOUT_MS = 605_000;
const SERVER_NAME = "agent_session_router";

const RequestIdSchema = z.union([z.string().min(1), z.number().int().safe()]);
const McpEnvironmentSchema = z.record(z.string(), z.string());
const InitializeRequestSchema = z
  .object({
    v: z.literal(1),
    id: RequestIdSchema,
    method: z.literal("initialize"),
    params: z
      .object({
        resumeSessionId: z.string().uuid().optional(),
        cwd: z.string().min(1),
        executablePath: z.string().min(1).optional(),
        initializeTimeoutMs: z.number().int().positive(),
        maxTurns: z.number().int().positive(),
        mcp: z
          .object({
            command: z.string().min(1),
            args: z.array(z.string()),
            env: McpEnvironmentSchema,
          })
          .strict(),
      })
      .strict(),
  })
  .strict();
const TurnRequestSchema = z
  .object({
    v: z.literal(1),
    id: RequestIdSchema,
    method: z.literal("turn"),
    params: z
      .object({
        requestId: z.string().min(1),
        prompt: z.string(),
        timeoutMs: z.number().int().positive(),
      })
      .strict(),
  })
  .strict();
const CancelRequestSchema = z
  .object({
    v: z.literal(1),
    id: RequestIdSchema,
    method: z.literal("cancel"),
    params: z
      .object({
        requestId: z.string().min(1),
        reason: z.enum(["request_timeout", "provider_disconnected", "task_interrupted"]),
      })
      .strict(),
  })
  .strict();
const ShutdownRequestSchema = z
  .object({
    v: z.literal(1),
    id: RequestIdSchema,
    method: z.literal("shutdown"),
    params: z.object({}).strict(),
  })
  .strict();
export const BridgeRequestSchema = z.discriminatedUnion("method", [
  InitializeRequestSchema,
  TurnRequestSchema,
  CancelRequestSchema,
  ShutdownRequestSchema,
]);
export type BridgeRequest = z.infer<typeof BridgeRequestSchema>;
export type BridgeRequestId = z.infer<typeof RequestIdSchema>;

export type BridgeResponse =
  | { v: 1; id: BridgeRequestId; ok: true; result: unknown }
  | {
      v: 1;
      id: BridgeRequestId;
      ok: false;
      error: { code: string; sessionId?: string };
    };

export interface ClaudeQueryHandle extends AsyncIterable<SDKMessage> {
  close(): void;
}

export interface ClaudeWarmHandle {
  query(prompt: string): ClaudeQueryHandle;
  close(): void;
}

export interface ClaudeSdkRuntime {
  startup(params: {
    options: Options;
    initializeTimeoutMs: number;
  }): Promise<ClaudeWarmHandle>;
  query(params: { prompt: string; options: Options }): ClaudeQueryHandle;
}

const DEFAULT_RUNTIME: ClaudeSdkRuntime = {
  startup: (params) => claudeStartup(params),
  query: (params) => claudeQuery(params),
};

const MODEL_TOOL_NAMES = [
  "agent_list",
  "agent_send",
  "workspace_list",
  "workspace_join",
  "workspace_leave",
  "workspace_members",
  "workspace_post",
  "workspace_history",
  "task_list",
  "task_get",
  "task_history",
  "task_create",
  "task_edit",
  "task_assign",
  "task_note",
  "task_begin",
  "task_checkpoint",
  "task_pause",
  "task_complete",
  "task_cancel",
  "task_reopen",
  "task_request",
  "integration_list",
  "task_import",
  "task_link",
  "task_publish",
  "task_external_status",
] as const;

interface ActiveTurn {
  requestId: string;
  controller: AbortController;
  handle: ClaudeQueryHandle | null;
  cancellationCode: string | null;
}

export class ClaudeBridgeSession {
  private options: Omit<Options, "abortController" | "resume" | "continue" | "forkSession"> | null = null;
  private sessionId: string | null = null;
  private warm: { handle: ClaudeWarmHandle; controller: AbortController } | null = null;
  private active: ActiveTurn | null = null;
  private closed = false;

  constructor(
    private readonly runtime: ClaudeSdkRuntime = DEFAULT_RUNTIME,
    private readonly sourceEnvironment: Record<string, string | undefined> = process.env,
  ) {}

  async initialize(params: z.infer<typeof InitializeRequestSchema>["params"]): Promise<unknown> {
    if (this.closed || this.options) throw new BridgeFailure("bridge_protocol_error");
    const controller = new AbortController();
    const options = createClaudeOptions(params, controller, this.sourceEnvironment);
    try {
      const handle = await this.runtime.startup({
        options,
        initializeTimeoutMs: params.initializeTimeoutMs,
      });
      this.options = withoutLifecycleOptions(options);
      this.sessionId = params.resumeSessionId ?? null;
      this.warm = { handle, controller };
      return { state: "ready", ...(this.sessionId ? { sessionId: this.sessionId } : {}) };
    } catch {
      controller.abort();
      throw new BridgeFailure("claude_initialize_failed");
    }
  }

  async turn(params: z.infer<typeof TurnRequestSchema>["params"]): Promise<unknown> {
    if (this.closed || !this.options) throw new BridgeFailure("bridge_protocol_error", this.sessionId);
    if (this.active) throw new BridgeFailure("session_busy", this.sessionId);

    const warm = this.warm;
    this.warm = null;
    const controller = warm?.controller ?? new AbortController();
    const active: ActiveTurn = {
      requestId: params.requestId,
      controller,
      handle: null,
      cancellationCode: null,
    };
    this.active = active;
    const timer = setTimeout(() => {
      if (this.active !== active) return;
      active.cancellationCode = "request_timeout";
      active.controller.abort();
      active.handle?.close();
    }, Math.max(1, params.timeoutMs - PROVIDER_TIMEOUT_MARGIN_MS));
    timer.unref?.();

    try {
      const handle = warm
        ? warm.handle.query(params.prompt)
        : this.runtime.query({
            prompt: params.prompt,
            options: {
              ...this.options,
              abortController: controller,
              ...(this.sessionId ? { resume: this.sessionId } : {}),
            },
          });
      active.handle = handle;
      for await (const message of handle) {
        if (message.type !== "result") continue;
        if (active.cancellationCode) {
          throw new BridgeFailure(active.cancellationCode, this.sessionId);
        }
        if (!z.string().uuid().safeParse(message.session_id).success) {
          throw new BridgeFailure("claude_protocol_error", this.sessionId);
        }
        this.sessionId = message.session_id;
        if (message.subtype === "success" && message.is_error === false) {
          return { content: message.result, sessionId: message.session_id };
        }
        throw new BridgeFailure(mapClaudeError(message.subtype), message.session_id);
      }
      if (active.cancellationCode) {
        throw new BridgeFailure(active.cancellationCode, this.sessionId);
      }
      throw new BridgeFailure("claude_no_result", this.sessionId);
    } catch (error) {
      if (error instanceof BridgeFailure) throw error;
      if (active.cancellationCode) {
        throw new BridgeFailure(active.cancellationCode, this.sessionId);
      }
      throw new BridgeFailure("claude_sdk_error", this.sessionId);
    } finally {
      clearTimeout(timer);
      active.handle?.close();
      if (this.active === active) this.active = null;
    }
  }

  cancel(params: z.infer<typeof CancelRequestSchema>["params"]): { cancelled: boolean } {
    const active = this.active;
    if (!active || active.requestId !== params.requestId) return { cancelled: false };
    active.cancellationCode = params.reason;
    active.controller.abort();
    active.handle?.close();
    return { cancelled: true };
  }

  shutdown(): { state: "closed" } {
    this.close();
    return { state: "closed" };
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.warm) {
      this.warm.controller.abort();
      this.warm.handle.close();
      this.warm = null;
    }
    if (this.active) {
      this.active.cancellationCode ??= "provider_disconnected";
      this.active.controller.abort();
      this.active.handle?.close();
    }
  }
}

export class BridgeServer {
  private readonly ids = new Set<BridgeRequestId>();
  private initializing: Promise<unknown> | null = null;

  constructor(private readonly session: ClaudeBridgeSession) {}

  async dispatch(raw: unknown): Promise<BridgeResponse> {
    const parsed = BridgeRequestSchema.safeParse(raw);
    if (!parsed.success) {
      const id = extractRequestId(raw);
      return failure(id, "bridge_protocol_error");
    }
    const request = parsed.data;
    if (this.ids.has(request.id)) return failure(request.id, "bridge_protocol_error");
    this.ids.add(request.id);
    try {
      let result: unknown;
      if (request.method === "initialize") {
        this.initializing = this.session.initialize(request.params);
        result = await this.initializing;
      } else if (request.method === "turn") {
        if (this.initializing) await this.initializing;
        result = await this.session.turn(request.params);
      } else if (request.method === "cancel") {
        result = this.session.cancel(request.params);
      } else {
        result = this.session.shutdown();
      }
      return { v: 1, id: request.id, ok: true, result };
    } catch (error) {
      if (error instanceof BridgeFailure) {
        return failure(request.id, error.code, error.sessionId);
      }
      return failure(request.id, "bridge_protocol_error");
    }
  }

  close(): void {
    this.session.close();
  }
}

export class FrameDecoder {
  private buffer = Buffer.alloc(0);

  push(chunk: Uint8Array): unknown[] {
    if (chunk.byteLength === 0) return [];
    this.buffer = Buffer.concat([this.buffer, Buffer.from(chunk)]);
    const frames: unknown[] = [];
    while (this.buffer.byteLength >= 4) {
      const length = this.buffer.readUInt32BE(0);
      if (length === 0 || length > MAX_FRAME_BYTES) throw new BridgeFailure("bridge_protocol_error");
      if (this.buffer.byteLength < length + 4) break;
      const payload = this.buffer.subarray(4, length + 4);
      this.buffer = this.buffer.subarray(length + 4);
      let text: string;
      try {
        text = new TextDecoder("utf-8", { fatal: true }).decode(payload);
        frames.push(JSON.parse(text));
      } catch {
        throw new BridgeFailure("bridge_protocol_error");
      }
    }
    if (this.buffer.byteLength > MAX_FRAME_BYTES + 4) {
      throw new BridgeFailure("bridge_protocol_error");
    }
    return frames;
  }

  finish(): void {
    if (this.buffer.byteLength !== 0) throw new BridgeFailure("bridge_protocol_error");
  }
}

export function encodeFrame(value: unknown): Uint8Array {
  const payload = Buffer.from(JSON.stringify(value), "utf8");
  if (payload.byteLength === 0 || payload.byteLength > MAX_FRAME_BYTES) {
    throw new BridgeFailure("bridge_protocol_error");
  }
  const frame = Buffer.allocUnsafe(payload.byteLength + 4);
  frame.writeUInt32BE(payload.byteLength, 0);
  payload.copy(frame, 4);
  return frame;
}

class BridgeFailure extends Error {
  constructor(
    readonly code: string,
    readonly sessionId?: string | null,
  ) {
    super(code);
  }
}

function createClaudeOptions(
  params: z.infer<typeof InitializeRequestSchema>["params"],
  abortController: AbortController,
  sourceEnvironment: Record<string, string | undefined>,
): Options {
  const env = sanitizeProviderEnvironment(sourceEnvironment);
  return {
    abortController,
    tools: [],
    allowedTools: MODEL_TOOL_NAMES.map((name) => `mcp__${SERVER_NAME}__${name}`),
    permissionMode: "dontAsk",
    settingSources: [],
    skills: [],
    plugins: [],
    strictMcpConfig: true,
    persistSession: true,
    includePartialMessages: false,
    promptSuggestions: false,
    maxTurns: params.maxTurns,
    cwd: params.cwd,
    ...(params.executablePath ? { pathToClaudeCodeExecutable: params.executablePath } : {}),
    ...(params.resumeSessionId ? { resume: params.resumeSessionId } : {}),
    mcpServers: {
      [SERVER_NAME]: {
        type: "stdio",
        command: params.mcp.command,
        args: params.mcp.args,
        env: sanitizeMcpEnvironment(params.mcp.env),
        alwaysLoad: true,
        timeout: MCP_TOOL_TIMEOUT_MS,
      },
    },
    env,
    stderr: () => {},
  };
}

function withoutLifecycleOptions(
  options: Options,
): Omit<Options, "abortController" | "resume" | "continue" | "forkSession"> {
  const {
    abortController: _abortController,
    resume: _resume,
    continue: _continue,
    forkSession: _forkSession,
    ...base
  } = options;
  return base;
}

export function sanitizeProviderEnvironment(
  source: Record<string, string | undefined>,
): Record<string, string | undefined> {
  const output: Record<string, string | undefined> = {};
  for (const [key, value] of Object.entries(source)) {
    if (!isForbiddenEnvironmentKey(key)) output[key] = value;
  }
  output.CLAUDE_AGENT_SDK_CLIENT_APP = "agent-session-router/0.1.0";
  return output;
}

function sanitizeMcpEnvironment(source: Record<string, string>): Record<string, string> {
  const output: Record<string, string> = {};
  for (const [key, value] of Object.entries(source)) {
    if (!isForbiddenEnvironmentKey(key)) output[key] = value;
  }
  return output;
}

function isForbiddenEnvironmentKey(key: string): boolean {
  return (
    [
      "ROUTER_TOKEN",
      "ROUTER_URL",
      "ASR_CREDENTIAL_FILE",
      "ASR_WORKSPACE",
      "ASR_DATA_DIR",
      "ASR_INTEGRATIONS_FILE",
      "ROUTER_PUBLIC_URL",
      "GITHUB_TOKEN",
      "GITHUB_ENTERPRISE_TOKEN",
      "GH_TOKEN",
      "LINEAR_API_KEY",
      "DEBUG",
      "PAGER",
      "CLICOLOR_FORCE",
      "CLAUDE_CWD",
      "CLAUDE_SESSION_ID",
      "CLAUDE_CODE_EXECUTABLE",
    ].includes(key) ||
    key.startsWith("ASR_") ||
    key.startsWith("GH_") ||
    key.startsWith("ROUTER_TLS_") ||
    key.startsWith("GATEWAY_") ||
    key.startsWith("AGENT_ROUTER_")
  );
}

function mapClaudeError(subtype: string): string {
  if (subtype === "error_max_turns") return "claude_max_turns";
  if (subtype === "error_max_budget_usd") return "claude_max_budget";
  if (subtype === "error_max_structured_output_retries") {
    return "claude_structured_output_error";
  }
  return "claude_execution_error";
}

function failure(
  id: BridgeRequestId,
  code: string,
  sessionId?: string | null,
): BridgeResponse {
  return {
    v: 1,
    id,
    ok: false,
    error: { code, ...(sessionId ? { sessionId } : {}) },
  };
}

function extractRequestId(raw: unknown): BridgeRequestId {
  const parsed = z.object({ id: RequestIdSchema }).passthrough().safeParse(raw);
  return parsed.success ? parsed.data.id : "invalid";
}

async function runBridge(): Promise<void> {
  const server = new BridgeServer(new ClaudeBridgeSession());
  const decoder = new FrameDecoder();
  let writes = Promise.resolve();
  try {
    for await (const chunk of process.stdin) {
      for (const raw of decoder.push(chunk)) {
        void server.dispatch(raw).then((response) => {
          writes = writes.then(
            () =>
              new Promise<void>((resolveWrite, rejectWrite) => {
                process.stdout.write(encodeFrame(response), (error) => {
                  if (error) rejectWrite(error);
                  else resolveWrite();
                });
              }),
          );
          return writes;
        });
      }
    }
    decoder.finish();
    await writes;
  } finally {
    server.close();
  }
}

const entryPath = process.argv[1];
if (entryPath && import.meta.url === pathToFileURL(resolve(entryPath)).href) {
  runBridge().catch(() => {
    process.exitCode = 1;
  });
}
