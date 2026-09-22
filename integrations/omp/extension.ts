import {
  closeSync,
  constants,
  fstatSync,
  lstatSync,
  openSync,
  readSync,
  type Stats,
} from "node:fs";
import { dirname, isAbsolute, join, parse, resolve } from "node:path";
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
const MAX_BINDING_BYTES = 16 * 1024;
// config::validate_profile_name and protocol::is_workspace_name share this grammar.
const NativeNameSchema = z.string().regex(/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}(?![\s\S])/);
const OnboardingBindingSchema = z.object({
  version: z.literal(1),
  executable: z.string().min(1),
  profile: NativeNameSchema,
  workspace: NativeNameSchema,
}).strict();
const WorkspaceJoinSchema = z.object({ name: NativeNameSchema }).strict();
const WorkspacePageSchema = z.object({
  workspaces: z.array(z.object({
    name: NativeNameSchema,
    createdAt: z.number().int().nonnegative(),
    connectedAgents: z.number().int().nonnegative(),
  }).strict()).max(100),
  nextCursor: NativeNameSchema.nullable(),
  hasMore: z.boolean(),
}).strict();
const MembersSchema = z.object({
  agents: z.array(z.object({
    agentId: z.string().min(1),
    side: z.string(),
    client: z.string(),
  }).passthrough()),
}).strict();

export type OnboardingBinding = z.infer<typeof OnboardingBindingSchema>;

class OmpConfigurationError extends Error {
  constructor(readonly code: string) {
    super(`agent-session-router: ${code}; repair the OMP onboarding configuration and restart the OMP process.`);
    this.name = "OmpConfigurationError";
  }
}

class WorkspaceCommandError extends Error {
  constructor(readonly code: string) {
    super(code);
  }
}

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
  sendMessage(
    message: { customType: string; content: string; display: true },
    options: { triggerTurn: false },
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
  for (const key of ["ASR_CONFIG_PATH", "XDG_CONFIG_HOME", "ASR_CA_FILE", "ROUTER_URL"] as const) {
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
  private starting: Promise<void> | null = null;
  private joinedWorkspace: string | null = null;
  private membershipChanging = false;
  private membershipUncertain = false;
  private readonly pendingRequests = new Set<symbol>();
  private generation = 0;

  constructor(
    private readonly pi: OmpHost,
    private readonly dependencies: OmpExtensionDependencies,
  ) {}

  async start(context: OmpContext): Promise<void> {
    this.context = context;
    if (!context.hasUI) return;
    if (this.starting) return this.starting;
    if (this.connection) return;
    const generation = this.generation;
    try {
      this.selection ??= this.readSelection(context);
      if (!this.selection) return;
      const initialWorkspace = this.selection.workspace;
      this.starting = (async () => {
        await this.ensureConnected();
        // OMP's backend intentionally rejects initial_workspace. Join only after MCP connects.
        if (initialWorkspace) {
          checkedResult(await this.changeMembership("workspace_join", { name: initialWorkspace }));
        }
        await this.notify(`${PREFIX}host_state`, { v: 1, ready: context.isIdle() });
      })().catch((error: unknown) => {
        if (generation === this.generation) context.ui.notify(commandErrorMessage(error), "error");
      });
      await this.starting;
    } catch (error) {
      context.ui.notify(commandErrorMessage(error), "error");
    } finally {
      if (generation === this.generation) this.starting = null;
    }
  }

  private readSelection(context: OmpContext): ChildSelection | null {
    const env = this.dependencies.env ?? process.env;
    if (env.ASR_EXECUTABLE !== undefined) {
      if (!env.ASR_EXECUTABLE) throw new OmpConfigurationError("onboarding_executable_invalid");
      return {
        executable: env.ASR_EXECUTABLE,
        credentialFile: env.ASR_CREDENTIAL_FILE,
        profile: env.ASR_PROFILE,
        workspace: env.ASR_WORKSPACE,
        cwd: context.cwd,
      };
    }
    const binding = readOnboardingBinding(env);
    return binding
      ? { executable: binding.executable, profile: binding.profile, workspace: binding.workspace, cwd: context.cwd }
      : null;
  }

  async execute(name: string, params: JsonObject, signal?: AbortSignal): Promise<unknown> {
    if (name === "workspace_join" || name === "workspace_leave") {
      return this.changeMembership(name, params, signal);
    }
    const result = await this.call(name, params, signal);
    if (toolFailure(result)) return result;
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

  async command(raw: string, context: OmpContext, namespaced = false): Promise<void> {
    this.context = context;
    try {
      const words = splitCommand(raw);
      if (namespaced && words.shift() !== "workspace") throw new WorkspaceCommandError("usage");
      const command = words.shift();
      if (!command) throw new WorkspaceCommandError("usage");
      let result: unknown;
      if (command === "create") {
        const workspace = words.shift();
        if (!NativeNameSchema.safeParse(workspace).success || words.length) {
          throw new WorkspaceCommandError("usage");
        }
        this.display("Server administrator action", {
          instruction: `Run on the server: asr onboarding prompt --workspace ${workspace} --create-workspace`,
        });
        return;
      }
      if (command === "join") {
        const parsed = parseJoin(words);
        this.selection ??= this.readSelection(context);
        if (!this.selection) throw new OmpConfigurationError("onboarding_configuration_required");
        if (this.connection || this.connecting) {
          if (
            (parsed.profile !== undefined && parsed.profile !== this.selection.profile) ||
            (parsed.credentialFile !== undefined && parsed.credentialFile !== this.selection.credentialFile)
          ) {
            throw new WorkspaceCommandError("profile_restart_required");
          }
        } else {
          if (parsed.profile !== undefined) this.selection.profile = parsed.profile;
          if (parsed.credentialFile !== undefined) this.selection.credentialFile = parsed.credentialFile;
        }
        result = checkedResult(await this.execute("workspace_join", { name: parsed.workspace }));
      } else if (command === "list" || command === "find") {
        if (command === "list" && words.length) throw new WorkspaceCommandError("usage");
        const query = asciiLower(words.join(" "));
        result = { workspaces: await this.listWorkspaces(query) };
      } else if (command === "members" || command === "history") {
        if (words.length) throw new WorkspaceCommandError("usage");
        result = checkedResult(await this.execute(`workspace_${command}`, {}));
      } else if (command === "post") {
        const content = words.join(" ");
        if (!content) throw new WorkspaceCommandError("usage");
        result = checkedResult(await this.execute("workspace_post", { content }));
      } else if (command === "leave") {
        if (words.length) throw new WorkspaceCommandError("usage");
        result = checkedResult(await this.execute("workspace_leave", {}));
        // Keep the MCP connection; a connected but unjoined provider may inspect and rejoin.
      } else if (command === "status") {
        if (words.length) throw new WorkspaceCommandError("usage");
        result = await this.status();
      } else {
        throw new WorkspaceCommandError("usage");
      }
      this.display(`Workspace ${command}`, result);
    } catch (error) {
      context.ui.notify(commandErrorMessage(error), "error");
    }
  }

  private display(label: string, value: unknown): void {
    const json = JSON.stringify(value, null, 2).replace(/[\u007f-\u009f]/g, (character) =>
      `\\u${character.charCodeAt(0).toString(16).padStart(4, "0")}`,
    );
    this.pi.sendMessage({
      customType: "agent-session-router",
      content: `${label}\n\n\`\`\`json\n${json}\n\`\`\``,
      display: true,
    }, { triggerTurn: false });
  }

  private async listWorkspaces(query = ""): Promise<z.infer<typeof WorkspacePageSchema>["workspaces"]> {
    const generation = this.generation;
    const workspaces: z.infer<typeof WorkspacePageSchema>["workspaces"] = [];
    const seenCursors = new Set<string>();
    let after: string | undefined;
    for (;;) {
      const result = checkedResult(await this.call("workspace_list", { limit: 100, ...(after ? { after } : {}) }));
      if (generation !== this.generation) throw new WorkspaceCommandError("session_changed");
      const parsed = WorkspacePageSchema.safeParse(result);
      if (!parsed.success) throw new WorkspaceCommandError("invalid_mcp_response");
      const page = parsed.data;
      for (const workspace of page.workspaces) {
        if (asciiLower(workspace.name).includes(query)) workspaces.push(workspace);
      }
      if (!page.hasMore) return workspaces;
      if (!page.nextCursor || seenCursors.has(page.nextCursor) || (after && page.nextCursor <= after)) {
        throw new WorkspaceCommandError("invalid_workspace_cursor");
      }
      seenCursors.add(page.nextCursor);
      after = page.nextCursor;
    }
  }

  private async status(): Promise<JsonObject> {
    const generation = this.generation;
    const workspaces = await this.listWorkspaces();
    const workspace = this.joinedWorkspace;
    if (!workspace || this.membershipUncertain) {
      return { workspace, workspaces, participation: "not_checked" };
    }
    const before = MembersSchema.parse(checkedResult(await this.call("workspace_members", {})));
    const peers = MembersSchema.parse(checkedResult(await this.call("agent_list", {})));
    const after = MembersSchema.parse(checkedResult(await this.call("workspace_members", {})));
    const beforeIds = before.agents.map((agent) => agent.agentId).sort();
    const afterIds = after.agents.map((agent) => agent.agentId).sort();
    const peerIds = new Set(peers.agents.map((agent) => agent.agentId));
    const self = after.agents.filter((agent) => !peerIds.has(agent.agentId));
    const stable = beforeIds.length === afterIds.length &&
      beforeIds.every((id, index) => id === afterIds[index]) &&
      new Set(afterIds).size === afterIds.length &&
      peers.agents.every((agent) => afterIds.includes(agent.agentId));
    // The native agent_list excludes exactly this connection, unlike workspace_members.
    const confirmed = stable && self.length === 1 && self[0]?.side === "generic" &&
      self[0]?.client === "omp" && this.joinedWorkspace === workspace && generation === this.generation &&
      !this.membershipChanging && !this.membershipUncertain &&
      workspaces.some((entry) => entry.name === workspace);
    return {
      workspace,
      workspaces,
      members: after.agents,
      participation: confirmed ? "confirmed" : "not_checked",
      ...(confirmed ? { agentId: self[0]?.agentId } : {}),
    };
  }

  private async changeMembership(
    name: "workspace_join" | "workspace_leave",
    params: JsonObject,
    signal?: AbortSignal,
  ): Promise<unknown> {
    const joining = name === "workspace_join" ? WorkspaceJoinSchema.safeParse(params) : null;
    if ((joining && !joining.success) || (!joining && Object.keys(params).length !== 0)) {
      throw new WorkspaceCommandError("invalid_message");
    }
    if (this.membershipUncertain) throw new WorkspaceCommandError("leave_unconfirmed");
    // OMP is streaming during user-directed tool calls too; only ASR work fences membership changes.
    if (this.membershipChanging || this.activeWork || this.pendingRequests.size > 0) {
      throw new WorkspaceCommandError("workspace_busy");
    }
    const expected = joining?.success ? joining.data.name : null;
    if (expected && this.joinedWorkspace && this.joinedWorkspace !== expected) {
      throw new WorkspaceCommandError("leave_required");
    }
    this.membershipChanging = true;
    const generation = this.generation;
    try {
      const value = await this.call(name, params, signal);
      const failure = toolFailure(value);
      if (failure) {
        if (failure.code === "leave_unconfirmed") this.membershipUncertain = true;
        return value;
      }
      const result = structuredResult(value);
      if (generation !== this.generation) throw new WorkspaceCommandError("session_changed");
      if (expected ? result.workspace !== expected : result.workspace !== this.joinedWorkspace) {
        throw new WorkspaceCommandError("invalid_mcp_response");
      }
      this.joinedWorkspace = expected;
      return value;
    } catch (error) {
      if (generation === this.generation) this.membershipUncertain = true;
      throw error;
    } finally {
      if (generation === this.generation) this.membershipChanging = false;
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
    this.generation += 1;
    this.connection = null;
    this.connecting = null;
    this.starting = null;
    this.selection = null;
    this.activeWork = null;
    this.joinedWorkspace = null;
    this.membershipChanging = false;
    this.membershipUncertain = false;
    this.pendingRequests.clear();
    if (connection) await connection.close();
  }

  private async ensureConnected(): Promise<OmpMcpConnection> {
    if (this.connection) return this.connection;
    if (this.connecting) return this.connecting;
    if (!this.selection && this.context) this.selection = this.readSelection(this.context);
    if (!this.selection) throw new OmpConfigurationError("onboarding_configuration_required");
    const generation = this.generation;
    this.connecting = this.dependencies
      .connect(this.selection, (notification) =>
        generation === this.generation ? this.handleNotification(notification) : Promise.resolve(),
      )
      .then(async (connection) => {
        if (generation !== this.generation) {
          await connection.close();
          throw new WorkspaceCommandError("session_changed");
        }
        this.connection = connection;
        this.connecting = null;
        return connection;
      })
      .catch((error: unknown) => {
        if (generation === this.generation) this.connecting = null;
        throw error;
      });
    return this.connecting;
  }

  private async call(name: string, params: JsonObject, signal?: AbortSignal): Promise<unknown> {
    const isRequest = name === "agent_send" || name === "task_request";
    if (isRequest && (this.membershipChanging || this.membershipUncertain)) {
      throw new WorkspaceCommandError("workspace_busy");
    }
    const pending = isRequest ? Symbol() : null;
    if (pending) this.pendingRequests.add(pending);
    try {
      const connection = await this.ensureConnected();
      const requested = numberField(params, "timeoutMs") ?? DEFAULT_WORK_TIMEOUT_MS;
      const timeout = isRequest ? requested + WORK_TIMEOUT_MARGIN_MS : DEFAULT_TIMEOUT_MS;
      return await connection.callTool(name, params, {
        signal,
        timeout,
        maxTotalTimeout: timeout,
        resetTimeoutOnProgress: false,
      });
    } finally {
      if (pending) this.pendingRequests.delete(pending);
    }
  }

  private async notify(method: string, params: JsonObject): Promise<void> {
    if (!this.connection) return;
    await this.connection.notify(method, params);
  }

  private async handleNotification(notification: AsrNotification): Promise<void> {
    const context = this.context;
    if (!context) return;
    if (notification.method === `${PREFIX}work`) {
      if (this.activeWork || this.membershipChanging || this.membershipUncertain || !context.isIdle()) {
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
    pi.registerCommand("asr", {
      description: "Use ASR workspace list, find, join, members, history, post, leave, or status",
      handler: (args, context) => controller.command(args, context, true),
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
  if (escaped || quote) throw new WorkspaceCommandError("usage");
  if (word) words.push(word);
  return words;
}

function parseJoin(words: string[]): Omit<ChildSelection, "executable" | "cwd"> & { workspace: string } {
  const workspace = words.shift();
  if (!NativeNameSchema.safeParse(workspace).success || workspace === undefined) {
    throw new WorkspaceCommandError("usage");
  }
  let credentialFile: string | undefined;
  let profile: string | undefined;
  while (words.length > 0) {
    const option = words.shift();
    const value = words.shift();
    if (!value) throw new WorkspaceCommandError("usage");
    if (option === "--credential" && credentialFile === undefined) credentialFile = value;
    else if (option === "--profile" && profile === undefined && NativeNameSchema.safeParse(value).success) profile = value;
    else throw new WorkspaceCommandError("usage");
  }
  return { workspace, credentialFile, profile };
}

function numberField(value: JsonObject, key: string): number | null {
  const field = value[key];
  return typeof field === "number" && Number.isSafeInteger(field) && field > 0 ? field : null;
}

/** Reads only the public launch descriptor, never the profile or credential contents. */
export function readOnboardingBinding(
  env: Record<string, string | undefined>,
): OnboardingBinding | undefined {
  let configPath: string;
  if (env.ASR_CONFIG_PATH) {
    configPath = env.ASR_CONFIG_PATH;
    rejectUnsafePath(configPath);
    if (configPath === "~" || configPath.startsWith("~/")) {
      if (!env.HOME) throw new OmpConfigurationError("onboarding_home_required");
      rejectUnsafePath(env.HOME);
      configPath = configPath === "~" ? env.HOME : join(env.HOME, configPath.slice(2));
    }
  } else if (env.XDG_CONFIG_HOME) {
    // Rust deliberately does not expand a tilde in XDG_CONFIG_HOME.
    configPath = `${env.XDG_CONFIG_HOME}/agent-session-router/config.json`;
  } else if (env.HOME) {
    configPath = `${env.HOME}/.config/agent-session-router/config.json`;
  } else {
    return undefined;
  }
  rejectUnsafePath(configPath);
  const configDirectory = dirname(resolve(configPath));
  const hostsDirectory = join(configDirectory, "hosts");
  const descriptor = join(hostsDirectory, "omp.json");
  let fd: number | undefined;
  try {
    const parents = inspectDirectories(hostsDirectory, [configDirectory, hostsDirectory]);
    if (!parents) return undefined;
    let before: Stats;
    try {
      before = lstatSync(descriptor);
    } catch (error) {
      if (isMissing(error)) return undefined;
      throw error;
    }
    validatePrivateFile(before);
    fd = openSync(descriptor, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    const opened = fstatSync(fd);
    validatePrivateFile(opened);
    if (!sameFile(before, opened)) throw new OmpConfigurationError("onboarding_binding_changed");
    const bytes = Buffer.alloc(MAX_BINDING_BYTES + 1);
    let size = 0;
    while (size < bytes.length) {
      const count = readSync(fd, bytes, size, bytes.length - size, null);
      if (count === 0) break;
      size += count;
    }
    if (size > MAX_BINDING_BYTES) throw new OmpConfigurationError("onboarding_binding_too_large");
    const after = fstatSync(fd);
    if (
      !sameFile(opened, lstatSync(descriptor)) ||
      after.size !== opened.size ||
      after.mtimeMs !== opened.mtimeMs ||
      after.ctimeMs !== opened.ctimeMs ||
      size !== opened.size
    ) {
      throw new OmpConfigurationError("onboarding_binding_changed");
    }
    for (const [path, metadata] of parents) {
      const current = lstatSync(path);
      if (!current.isDirectory() || !sameFile(metadata, current)) {
        throw new OmpConfigurationError("onboarding_binding_changed");
      }
    }
    let value: unknown;
    try {
      const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, size));
      const keys = new Set<string>();
      for (const token of text.matchAll(/("(?:[^"\\]|\\[\s\S])*")\s*(:)?/g)) {
        if (token[2]) {
          const key = JSON.parse(token[1]!) as string;
          if (keys.has(key)) throw new OmpConfigurationError("onboarding_binding_invalid");
          keys.add(key);
        }
      }
      value = JSON.parse(text);
    } catch {
      throw new OmpConfigurationError("onboarding_binding_invalid");
    }
    const parsed = OnboardingBindingSchema.safeParse(value);
    if (!parsed.success) throw new OmpConfigurationError("onboarding_binding_invalid");
    const binding = parsed.data;
    if (!isAbsolute(binding.executable)) {
      throw new OmpConfigurationError("onboarding_executable_invalid");
    }
    rejectUnsafePath(binding.executable);
    if (!inspectDirectories(dirname(binding.executable), [])) {
      throw new OmpConfigurationError("onboarding_executable_invalid");
    }
    let executable: Stats;
    try {
      executable = lstatSync(binding.executable);
    } catch {
      throw new OmpConfigurationError("onboarding_executable_invalid");
    }
    if (
      !executable.isFile() ||
      executable.uid !== currentUid() ||
      (executable.mode & 0o022) !== 0 ||
      (executable.mode & 0o7000) !== 0 ||
      (executable.mode & 0o100) === 0
    ) {
      throw new OmpConfigurationError("onboarding_executable_invalid");
    }
    return binding;
  } catch (error) {
    if (error instanceof OmpConfigurationError) throw error;
    throw new OmpConfigurationError("onboarding_binding_unreadable");
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
}

function currentUid(): number {
  if (!process.getuid) throw new OmpConfigurationError("onboarding_platform_unsupported");
  return process.getuid();
}

function rejectUnsafePath(path: string): void {
  if (path.includes("\0") || path.split("/").includes("..")) {
    throw new OmpConfigurationError("onboarding_path_unsafe");
  }
}

function isMissing(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT";
}

function sameFile(left: Stats, right: Stats): boolean {
  return left.dev === right.dev && left.ino === right.ino && left.mode === right.mode && left.uid === right.uid;
}

function validatePrivateFile(metadata: Stats): void {
  if (!metadata.isFile() || metadata.uid !== currentUid() || (metadata.mode & 0o077) !== 0 || metadata.nlink !== 1) {
    throw new OmpConfigurationError("onboarding_binding_permissions");
  }
  if (metadata.size > MAX_BINDING_BYTES) throw new OmpConfigurationError("onboarding_binding_too_large");
}

function inspectDirectories(
  path: string,
  privatePaths: readonly string[],
): Array<[string, Stats]> | undefined {
  rejectUnsafePath(path);
  const absolute = resolve(path);
  let current = parse(absolute).root;
  const inspected: Array<[string, Stats]> = [];
  for (const component of absolute.slice(current.length).split("/").filter(Boolean)) {
    current = join(current, component);
    let metadata: Stats;
    try {
      metadata = lstatSync(current);
    } catch (error) {
      if (isMissing(error)) return undefined;
      throw error;
    }
    if (!metadata.isDirectory()) throw new OmpConfigurationError("onboarding_path_unsafe");
    if (privatePaths.includes(current) && (metadata.uid !== currentUid() || (metadata.mode & 0o077) !== 0)) {
      throw new OmpConfigurationError("onboarding_binding_permissions");
    }
    inspected.push([current, metadata]);
  }
  return inspected;
}

function structuredResult(value: unknown): JsonObject {
  if (typeof value !== "object" || value === null || !("structuredContent" in value)) {
    throw new WorkspaceCommandError("invalid_mcp_response");
  }
  const content = value.structuredContent;
  if (typeof content !== "object" || content === null || Array.isArray(content)) {
    throw new WorkspaceCommandError("invalid_mcp_response");
  }
  return content as JsonObject;
}

function toolFailure(value: unknown): WorkspaceCommandError | undefined {
  if (typeof value !== "object" || value === null || !("isError" in value) || value.isError !== true) {
    return undefined;
  }
  const content = structuredResult(value);
  // Only native bounded error codes are displayed, never subprocess output or arbitrary exceptions.
  const code = typeof content.error === "string" && /^[a-z][a-z0-9_]{0,63}(?![\s\S])/.test(content.error)
    ? content.error
    : "mcp_tool_failed";
  return new WorkspaceCommandError(code);
}

function checkedResult(value: unknown): JsonObject {
  const error = toolFailure(value);
  if (error) throw error;
  return structuredResult(value);
}

function asciiLower(value: string): string {
  return value.replace(/[A-Z]/g, (character) => String.fromCharCode(character.charCodeAt(0) + 32));
}

function commandErrorMessage(error: unknown): string {
  if (error instanceof OmpConfigurationError) return error.message;
  const code = error instanceof WorkspaceCommandError ? error.code : "mcp_connection_failed";
  const instruction = code === "usage"
    ? "Use /asr workspace list|find QUERY|join NAME|members|history|post TEXT|leave|status."
    : code === "profile_restart_required"
      ? "Profile and credential changes require a new OMP process; live connections are not replaced."
      : code === "leave_required"
        ? "Leave the current workspace successfully before joining another room."
        : code === "workspace_busy" || code === "task_stop_unconfirmed"
          ? "Wait for pending/running work and confirmed task stop before changing workspaces."
          : code === "leave_unconfirmed"
            ? "Membership is uncertain. Restart the OMP process; do not retry leave or switch rooms here."
            : "Inspect the configuration or server state and retry; after plugin changes restart the OMP process.";
  return `agent-session-router: ${code}. ${instruction}`;
}
