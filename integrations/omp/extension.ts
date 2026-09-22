import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import {
  StdioClientTransport,
  getDefaultEnvironment,
} from "@modelcontextprotocol/sdk/client/stdio.js";
import type { Notification, Request, Result } from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod";

const MAX_BUFFER_BYTES = 1024 * 1024;
const DEFAULT_TIMEOUT_MS = 65_000;
const DEFAULT_WORK_TIMEOUT_MS = 600_000;
const WORK_TIMEOUT_MARGIN_MS = 5_000;
const PREFIX = "notifications/agent_session_router/";

const TaskRefSchema = z
  .object({ taskId: z.number().int().positive(), attemptId: z.string().min(1) })
  .strict();
const WorkNotificationSchema = z
  .object({
    method: z.literal(`${PREFIX}work`),
    params: z
      .object({
        v: z.literal(1),
        workspace: z.string().min(1),
        requestId: z.string().min(1),
        from: z.string().min(1),
        content: z.string(),
        timeoutMs: z.number().int().positive(),
        task: z
          .object({ id: z.number().int().positive(), expectedVersion: z.number().int().positive() })
          .strict()
          .optional(),
      })
      .strict(),
  })
  .strict();
const UnreadNotificationSchema = z
  .object({
    method: z.literal(`${PREFIX}unread`),
    params: z
      .object({
        v: z.literal(1),
        workspace: z.string().min(1),
        cursor: z.number().int().nonnegative(),
        count: z.number().int().nonnegative(),
      })
      .strict(),
  })
  .strict();
const CancelNotificationSchema = z
  .object({
    method: z.literal(`${PREFIX}cancel`),
    params: z
      .object({
        v: z.literal(1),
        workspace: z.string().min(1),
        requestId: z.string().min(1),
        task: TaskRefSchema.optional(),
      })
      .strict(),
  })
  .strict();
const TaskAttemptNotificationSchema = z
  .object({
    method: z.literal(`${PREFIX}task_attempt`),
    params: z
      .object({
        v: z.literal(1),
        workspace: z.string().min(1),
        taskId: z.number().int().positive(),
        attempt: z
          .object({ id: z.string().min(1), sessionId: z.string().min(1) })
          .passthrough()
          .nullable(),
        closedAttemptId: z.string().min(1).nullable(),
      })
      .strict(),
  })
  .strict();
const AgentEndEventSchema = z.object({ isTerminal: z.boolean().optional() }).passthrough();
const AttemptBindingSchema = z
  .object({
    task: z
      .object({
        id: z.number().int().positive(),
        currentAttempt: z
          .object({ id: z.string().min(1), sessionId: z.string().min(1) })
          .passthrough(),
      })
      .passthrough(),
  })
  .passthrough();
const StructuredAttemptBindingSchema = z
  .object({ structuredContent: AttemptBindingSchema })
  .passthrough();

type WorkNotification = z.infer<typeof WorkNotificationSchema>["params"];
type AsrNotification =
  | z.infer<typeof WorkNotificationSchema>
  | z.infer<typeof UnreadNotificationSchema>
  | z.infer<typeof CancelNotificationSchema>
  | z.infer<typeof TaskAttemptNotificationSchema>;

type JsonObject = Record<string, unknown>;

export interface OmpContext {
  cwd: string;
  hasUI: boolean;
  isIdle(): boolean;
  abort(): void;
  ui: { notify(message: string, level?: "info" | "warning" | "error"): void };
}

export interface OmpHost {
  zod: typeof z;
  registerTool(definition: {
    name: string;
    label: string;
    description: string;
    parameters: z.ZodType;
    execute(
      toolCallId: string,
      params: JsonObject,
      signal: AbortSignal | undefined,
      onUpdate: unknown,
      context: OmpContext,
    ): Promise<unknown>;
  }): void;
  registerCommand(
    name: string,
    command: {
      description: string;
      handler(args: string, context: OmpContext): Promise<void>;
    },
  ): void;
  on(
    event: string,
    handler: (event: unknown, context: OmpContext) => void | Promise<void>,
  ): void;
  sendUserMessage(
    content: string,
    options: { attribution: "agent"; deliverAs: "followUp" },
  ): void;
}

export interface ChildSelection {
  executable: string;
  credentialFile?: string;
  profile?: string;
  workspace?: string;
  cwd: string;
}

export interface ToolCallOptions {
  signal?: AbortSignal;
  timeout: number;
  maxTotalTimeout: number;
  resetTimeoutOnProgress: false;
}

export interface OmpMcpConnection {
  callTool(name: string, args: JsonObject, options: ToolCallOptions): Promise<unknown>;
  notify(method: string, params: JsonObject): Promise<void>;
  close(): Promise<void>;
}

export interface OmpExtensionDependencies {
  connect(
    selection: ChildSelection,
    onNotification: (notification: AsrNotification) => Promise<void>,
  ): Promise<OmpMcpConnection>;
  env?: Record<string, string | undefined>;
}

const TOOL_NAMES = [
  "agent_list",
  "agent_send",
  "agent_reply",
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

class SdkMcpConnection implements OmpMcpConnection {
  constructor(private readonly client: Client<Request, Notification, Result>) {}

  callTool(name: string, args: JsonObject, options: ToolCallOptions): Promise<unknown> {
    return this.client.callTool({ name, arguments: args }, undefined, options);
  }

  notify(method: string, params: JsonObject): Promise<void> {
    return this.client.notification({ method, params });
  }

  close(): Promise<void> {
    return this.client.close();
  }
}

async function connectSdk(
  selection: ChildSelection,
  onNotification: (notification: AsrNotification) => Promise<void>,
): Promise<OmpMcpConnection> {
  const args: string[] = [];
  if (selection.credentialFile) args.push("--credential", selection.credentialFile);
  if (selection.profile) args.push("--profile", selection.profile);
  args.push("mcp", "omp");

  const env = getDefaultEnvironment();
  for (const key of ["ASR_CONFIG_PATH", "ASR_CA_FILE", "ROUTER_URL"] as const) {
    const value = process.env[key];
    if (value !== undefined) env[key] = value;
  }

  const transport = new StdioClientTransport({
    command: selection.executable,
    args,
    cwd: selection.cwd,
    env,
    stderr: "ignore",
    maxBufferSize: MAX_BUFFER_BYTES,
  });
  const client = new Client<Request, Notification, Result>(
    { name: "agent-session-router-omp", version: "0.1.0" },
    { capabilities: {} },
  );
  client.setNotificationHandler(WorkNotificationSchema, (notification) =>
    onNotification(notification),
  );
  client.setNotificationHandler(UnreadNotificationSchema, (notification) =>
    onNotification(notification),
  );
  client.setNotificationHandler(CancelNotificationSchema, (notification) =>
    onNotification(notification),
  );
  client.setNotificationHandler(TaskAttemptNotificationSchema, (notification) =>
    onNotification(notification),
  );
  await client.connect(transport);
  return new SdkMcpConnection(client);
}

interface ActiveWork {
  request: WorkNotification;
  replied: boolean;
  attempt: { id: string; sessionId: string } | null;
  attemptClosed: boolean;
}

class OmpController {
  private connection: OmpMcpConnection | null = null;
  private connecting: Promise<OmpMcpConnection> | null = null;
  private selection: ChildSelection | null = null;
  private context: OmpContext | null = null;
  private activeWork: ActiveWork | null = null;
  private consumedInitialWorkspace = false;

  constructor(
    private readonly pi: OmpHost,
    private readonly dependencies: OmpExtensionDependencies,
  ) {}

  async start(context: OmpContext): Promise<void> {
    this.context = context;
    if (!context.hasUI) return;
    const env = this.dependencies.env ?? process.env;
    const executable = env.ASR_EXECUTABLE;
    if (!executable) return;
    const initialWorkspace = this.consumedInitialWorkspace ? undefined : env.ASR_WORKSPACE;
    this.consumedInitialWorkspace = true;
    this.selection = {
      executable,
      credentialFile: env.ASR_CREDENTIAL_FILE,
      profile: env.ASR_PROFILE,
      workspace: initialWorkspace,
      cwd: context.cwd,
    };
    await this.ensureConnected();
    if (initialWorkspace) {
      await this.call("workspace_join", { workspace: initialWorkspace }, undefined);
    }
    await this.notify(`${PREFIX}host_state`, { v: 1, ready: context.isIdle() });
  }

  async execute(name: string, params: JsonObject, signal?: AbortSignal): Promise<unknown> {
    const result = await this.call(name, params, signal);
    const active = this.activeWork;
    if (
      name === "agent_reply" &&
      active !== null &&
      active.request.requestId === params.requestId
    ) {
      active.replied = true;
    } else if (name === "task_begin") {
      this.bindAttemptFromResult(result);
    } else if (name === "task_pause" || name === "task_complete") {
      this.closeAttemptFromParams(params);
    }
    return result;
  }

  async command(raw: string, context: OmpContext): Promise<void> {
    this.context = context;
    try {
      const words = splitCommand(raw);
      const command = words.shift();
      if (!command) throw new Error("usage");
      if (command === "join") {
        const parsed = parseJoin(words);
        if (this.connection || this.connecting) throw new Error("connected");
        const executable = (this.dependencies.env ?? process.env).ASR_EXECUTABLE;
        if (!executable) throw new Error("configuration");
        this.selection = { executable, cwd: context.cwd, ...parsed };
        await this.ensureConnected();
        await this.call("workspace_join", { workspace: parsed.workspace }, undefined);
      } else if (command === "list") {
        await this.call("workspace_list", {}, undefined);
      } else if (command === "members") {
        await this.call("workspace_members", {}, undefined);
      } else if (command === "history") {
        await this.call("workspace_history", {}, undefined);
      } else if (command === "post") {
        const content = words.join(" ");
        if (!content) throw new Error("usage");
        await this.call("workspace_post", { content }, undefined);
      } else if (command === "leave") {
        await this.call("workspace_leave", {}, undefined);
        await this.close();
      } else {
        throw new Error("usage");
      }
      context.ui.notify("agent-session-router command completed", "info");
    } catch {
      context.ui.notify("agent-session-router command failed", "error");
    }
  }

  async lifecycle(eventName: string, event: unknown, context: OmpContext): Promise<void> {
    this.context = context;
    if (eventName === "agent_start") {
      await this.notify(`${PREFIX}host_state`, { v: 1, ready: false });
      return;
    }
    if (eventName === "agent_end") {
      const parsedEvent = AgentEndEventSchema.safeParse(event);
      const terminal = !parsedEvent.success || parsedEvent.data.isTerminal !== false;
      if (!terminal || !context.isIdle()) return;
      const active = this.activeWork;
      if (active?.attempt && !active.attemptClosed) {
        await this.notify(`${PREFIX}task_terminal`, {
          v: 1,
          workspace: active.request.workspace,
          taskId: active.request.task?.id,
          attemptId: active.attempt.id,
          sessionId: active.attempt.sessionId,
          reason: "turn_ended",
        });
      }
      if (active && !active.replied) {
        await this.notify(`${PREFIX}work_finished`, {
          v: 1,
          requestId: active.request.requestId,
          error: "reply_missing",
        });
      }
      this.activeWork = null;
      await this.notify(`${PREFIX}host_state`, { v: 1, ready: true });
      return;
    }
    if (["session_switch", "session_branch", "session_tree", "session_shutdown"].includes(eventName)) {
      await this.close();
    }
  }

  async close(): Promise<void> {
    const connection = this.connection;
    this.connection = null;
    this.connecting = null;
    this.selection = null;
    this.activeWork = null;
    if (connection) await connection.close();
  }

  private async ensureConnected(): Promise<OmpMcpConnection> {
    if (this.connection) return this.connection;
    if (this.connecting) return this.connecting;
    if (!this.selection) throw new Error("configuration");
    this.connecting = this.dependencies
      .connect(this.selection, (notification) => this.handleNotification(notification))
      .then((connection) => {
        this.connection = connection;
        this.connecting = null;
        return connection;
      })
      .catch((error: unknown) => {
        this.connecting = null;
        throw error;
      });
    return this.connecting;
  }

  private async call(name: string, params: JsonObject, signal?: AbortSignal): Promise<unknown> {
    const connection = await this.ensureConnected();
    const requested = numberField(params, "timeoutMs") ?? DEFAULT_WORK_TIMEOUT_MS;
    const timeout = name === "agent_send" || name === "task_request"
      ? requested + WORK_TIMEOUT_MARGIN_MS
      : DEFAULT_TIMEOUT_MS;
    return connection.callTool(name, params, {
      signal,
      timeout,
      maxTotalTimeout: timeout,
      resetTimeoutOnProgress: false,
    });
  }

  private async notify(method: string, params: JsonObject): Promise<void> {
    if (!this.connection) return;
    await this.connection.notify(method, params);
  }

  private async handleNotification(notification: AsrNotification): Promise<void> {
    const context = this.context;
    if (!context) return;
    if (notification.method === `${PREFIX}work`) {
      if (this.activeWork || !context.isIdle()) {
        await this.notify(`${PREFIX}work_finished`, {
          v: 1,
          requestId: notification.params.requestId,
          error: "session_busy",
        });
        return;
      }
      this.activeWork = {
        request: notification.params,
        replied: false,
        attempt: null,
        attemptClosed: false,
      };
      this.pi.sendUserMessage(
        `[agent_session_router work]\n${JSON.stringify(notification.params)}`,
        { attribution: "agent", deliverAs: "followUp" },
      );
      return;
    }
    if (notification.method === `${PREFIX}cancel`) {
      const active = this.activeWork;
      if (!active || active.request.requestId !== notification.params.requestId) return;
      if (notification.params.task && active.attempt?.id !== notification.params.task.attemptId) return;
      context.abort();
      return;
    }
    if (notification.method === `${PREFIX}unread`) {
      await this.notify(`${PREFIX}consumed`, {
        v: 1,
        workspace: notification.params.workspace,
        cursor: notification.params.cursor,
      });
      return;
    }
    const active = this.activeWork;
    if (!active || active.request.task?.id !== notification.params.taskId) return;
    if (notification.params.attempt) {
      active.attempt = {
        id: notification.params.attempt.id,
        sessionId: notification.params.attempt.sessionId,
      };
      active.attemptClosed = false;
    } else if (
      active.attempt &&
      notification.params.closedAttemptId === active.attempt.id
    ) {
      active.attemptClosed = true;
    }
  }

  private bindAttemptFromResult(value: unknown): void {
    const active = this.activeWork;
    if (!active) return;
    const direct = AttemptBindingSchema.safeParse(value);
    const structured = direct.success ? null : StructuredAttemptBindingSchema.safeParse(value);
    const task = direct.success
      ? direct.data.task
      : structured?.success
        ? structured.data.structuredContent.task
        : null;
    const attempt = task?.currentAttempt;
    if (
      attempt &&
      active.request.task &&
      task.id === active.request.task.id
    ) {
      active.attempt = { id: attempt.id, sessionId: attempt.sessionId };
      active.attemptClosed = false;
    }
  }

  private closeAttemptFromParams(params: JsonObject): void {
    const active = this.activeWork;
    if (active !== null && active.attempt?.id === params.attemptId) {
      active.attemptClosed = true;
    }
  }
}

export function createOmpExtension(
  dependencies: Partial<OmpExtensionDependencies> = {},
): (pi: OmpHost) => void {
  const resolved: OmpExtensionDependencies = {
    connect: dependencies.connect ?? connectSdk,
    env: dependencies.env,
  };
  return (pi) => {
    const controller = new OmpController(pi, resolved);
    for (const name of TOOL_NAMES) {
      pi.registerTool({
        name,
        label: name,
        description: `Forward ${name} to the agent-session-router MCP server.`,
        parameters: pi.zod.object({}).passthrough(),
        execute: (_id, params, signal) => controller.execute(name, params, signal),
      });
    }
    pi.registerCommand("workspace", {
      description: "Join or inspect agent-session-router workspaces",
      handler: (args, context) => controller.command(args, context),
    });
    pi.on("session_start", (_event, context) => controller.start(context));
    for (const eventName of [
      "agent_start",
      "agent_end",
      "session_switch",
      "session_branch",
      "session_tree",
      "session_shutdown",
    ]) {
      pi.on(eventName, (event, context) => controller.lifecycle(eventName, event, context));
    }
  };
}

export default createOmpExtension();

function splitCommand(input: string): string[] {
  const words: string[] = [];
  let word = "";
  let quote: "'" | '"' | null = null;
  let escaped = false;
  for (const character of input.trim()) {
    if (escaped) {
      word += character;
      escaped = false;
    } else if (character === "\\" && quote !== "'") {
      escaped = true;
    } else if (quote) {
      if (character === quote) quote = null;
      else word += character;
    } else if (character === "'" || character === '"') {
      quote = character;
    } else if (/\s/.test(character)) {
      if (word) {
        words.push(word);
        word = "";
      }
    } else {
      word += character;
    }
  }
  if (escaped || quote) throw new Error("usage");
  if (word) words.push(word);
  return words;
}

function parseJoin(words: string[]): Omit<ChildSelection, "executable" | "cwd"> & { workspace: string } {
  const workspace = words.shift();
  if (!workspace) throw new Error("usage");
  let credentialFile: string | undefined;
  let profile: string | undefined;
  while (words.length > 0) {
    const option = words.shift();
    const value = words.shift();
    if (!value) throw new Error("usage");
    if (option === "--credential" && credentialFile === undefined) credentialFile = value;
    else if (option === "--profile" && profile === undefined) profile = value;
    else throw new Error("usage");
  }
  if (!credentialFile) throw new Error("usage");
  return { workspace, credentialFile, profile };
}

function numberField(value: JsonObject, key: string): number | null {
  const field = value[key];
  return typeof field === "number" && Number.isSafeInteger(field) && field > 0 ? field : null;
}

