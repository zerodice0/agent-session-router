import { describe, expect, test } from "bun:test";
import {
  chmodSync,
  linkSync,
  mkdirSync,
  mkdtempSync,
  realpathSync,
  renameSync,
  rmSync,
  symlinkSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { z } from "zod";
import {
  createOmpExtension,
  readOnboardingBinding,
  type OnboardingBinding,
  type ChildSelection,
  type OmpContext,
  type OmpHost,
  type OmpMcpConnection,
  type ToolCallOptions,
} from "./extension";

type Handler = (event: unknown, context: OmpContext) => void | Promise<void>;
type Tool = Parameters<OmpHost["registerTool"]>[0];
type Command = Parameters<OmpHost["registerCommand"]>[1];

class FakeConnection implements OmpMcpConnection {
  readonly calls: Array<{ name: string; args: Record<string, unknown>; options: ToolCallOptions }> = [];
  readonly notifications: Array<{ method: string; params: Record<string, unknown> }> = [];
  closed = false;
  nextResult: unknown;
  workspace: string | null = null;
  respond?: (name: string, args: Record<string, unknown>) => unknown | Promise<unknown>;

  async callTool(
    name: string,
    args: Record<string, unknown>,
    options: ToolCallOptions,
  ): Promise<unknown> {
    // Match src/mcp.rs's deny_unknown_fields DTOs; {workspace} must never pass as a join.
    if (name === "workspace_join") z.object({ name: z.string() }).strict().parse(args);
    else if (name === "workspace_list") {
      z.object({ after: z.string().optional(), limit: z.number().int().min(1).max(100).optional() }).strict().parse(args);
    } else if (name === "workspace_members" || name === "workspace_leave" || name === "agent_list") {
      z.object({}).strict().parse(args);
    } else if (name === "workspace_post") {
      z.object({ content: z.string().min(1) }).strict().parse(args);
    } else if (name === "workspace_history") {
      z.object({ after: z.number().int().nonnegative().optional(), limit: z.number().int().max(100).optional() }).strict().parse(args);
    }
    this.calls.push({ name, args, options });
    const supplied = await this.respond?.(name, args);
    if (supplied !== undefined) return supplied;
    if (this.nextResult !== undefined) return this.nextResult;
    if (name === "workspace_join") {
      this.workspace = args.name as string;
      return { structuredContent: { workspace: this.workspace, cursor: 0 } };
    }
    if (name === "workspace_leave") {
      const workspace = this.workspace;
      this.workspace = null;
      return { structuredContent: { workspace } };
    }
    if (name === "workspace_list") {
      return {
        structuredContent: {
          workspaces: this.workspace ? [{ name: this.workspace, createdAt: 1, connectedAgents: 1 }] : [],
          hasMore: false,
          nextCursor: null,
        },
      };
    }
    if (name === "workspace_members") {
      return { structuredContent: { agents: this.workspace ? [
        { agentId: "omp-self", side: "generic", client: "omp", status: "idle", ready: true },
      ] : [] } };
    }
    if (name === "agent_list") return { structuredContent: { agents: [] } };
    if (name === "workspace_history") {
      return { structuredContent: { events: [{ seq: 1, content: "durable chat" }], hasMore: false, nextCursor: 1 } };
    }
    if (name === "workspace_post") return { structuredContent: { workspace: this.workspace, seq: 2 } };
    return { structuredContent: {}, content: [{ type: "text", text: "ok" }] };
  }

  async notify(method: string, params: Record<string, unknown>): Promise<void> {
    this.notifications.push({ method, params });
  }

  async close(): Promise<void> {
    this.closed = true;
  }
}

class FakeHost implements OmpHost {
  readonly zod = z;
  readonly tools: Record<string, Tool> = {};
  readonly commands: Record<string, Command> = {};
  readonly handlers: Record<string, Handler[]> = {};
  readonly messages: Array<{
    content: string;
    options: { attribution: "agent"; deliverAs: "followUp" };
  }> = [];

  readonly displayed: Array<{
    message: Parameters<OmpHost["sendMessage"]>[0];
    options: Parameters<OmpHost["sendMessage"]>[1];
  }> = [];

  sendMessage(
    message: Parameters<OmpHost["sendMessage"]>[0],
    options: Parameters<OmpHost["sendMessage"]>[1],
  ): void {
    this.displayed.push({ message, options });
  }
  registerTool(tool: Tool): void {
    this.tools[tool.name] = tool;
  }

  registerCommand(name: string, command: Command): void {
    this.commands[name] = command;
  }

  on(event: string, handler: Handler): void {
    (this.handlers[event] ??= []).push(handler);
  }

  sendUserMessage(
    content: string,
    options: { attribution: "agent"; deliverAs: "followUp" },
  ): void {
    this.messages.push({ content, options });
  }

  async emit(event: string, payload: unknown, context: OmpContext): Promise<void> {
    for (const handler of this.handlers[event] ?? []) await handler(payload, context);
  }
}

function createContext(): OmpContext & { aborts: number; idle: boolean; notices: string[] } {
  const notices: string[] = [];
  return {
    cwd: "/tmp/project",
    hasUI: true,
    idle: true,
    aborts: 0,
    notices,
    isIdle() {
      return this.idle;
    },
    abort() {
      this.aborts += 1;
    },
    ui: { notify(message) { notices.push(message); } },
  };
}

describe("OMP retained extension", () => {
  test("forwards tools with bounded SDK options and owns terminal work lifecycle", async () => {
    const host = new FakeHost();
    const connection = new FakeConnection();
    let selection: ChildSelection | undefined;
    let receive: ((notification: unknown) => Promise<void>) | undefined;
    createOmpExtension({
      env: {
        ASR_EXECUTABLE: "/opt/asr/bin/asr",
        ASR_CREDENTIAL_FILE: "/tmp/agent.json",
      },
      async connect(value, onNotification) {
        selection = value;
        receive = onNotification as (notification: unknown) => Promise<void>;
        return connection;
      },
    })(host);
    const context = createContext();

    await host.emit("session_start", {}, context);
    expect(selection).toEqual({
      executable: "/opt/asr/bin/asr",
      credentialFile: "/tmp/agent.json",
      profile: undefined,
      workspace: undefined,
      cwd: "/tmp/project",
    });
    expect(connection.notifications.at(-1)).toEqual({
      method: "notifications/agent_session_router/host_state",
      params: { v: 1, ready: true },
    });

    await receive?.({
      method: "notifications/agent_session_router/work",
      params: {
        v: 1,
        workspace: "red",
        requestId: "request-1",
        from: "coordinator",
        content: "review this",
        timeoutMs: 4_000,
        task: { id: 7, expectedVersion: 2 },
      },
    });
    expect(host.messages).toHaveLength(1);
    expect(host.messages[0]?.options).toEqual({
      attribution: "agent",
      deliverAs: "followUp",
    });

    connection.nextResult = {
      structuredContent: {
        task: {
          id: 7,
          currentAttempt: { id: "attempt-1", sessionId: "session-1" },
        },
      },
    };
    await host.tools.task_begin?.execute(
      "tool-1",
      { taskId: 7, timeoutMs: 12_000 },
      undefined,
      undefined,
      context,
    );
    expect(connection.calls.at(-1)?.options).toMatchObject({
      timeout: 65_000,
      maxTotalTimeout: 65_000,
      resetTimeoutOnProgress: false,
    });

    await host.tools.agent_send?.execute(
      "tool-2",
      { targetId: "worker", timeoutMs: 12_000 },
      undefined,
      undefined,
      context,
    );
    expect(connection.calls.at(-1)?.options.timeout).toBe(17_000);

    await host.emit("agent_start", {}, context);
    const beforeNonterminal = connection.notifications.length;
    await host.emit("agent_end", { isTerminal: false }, context);
    expect(connection.notifications).toHaveLength(beforeNonterminal);
    await host.emit("agent_end", { isTerminal: true }, context);
    expect(connection.notifications.slice(-3).map((entry) => entry.method)).toEqual([
      "notifications/agent_session_router/task_terminal",
      "notifications/agent_session_router/work_finished",
      "notifications/agent_session_router/host_state",
    ]);
  });

  test("rejects overlapping work, aborts only an exact cancellation, and closes on session change", async () => {
    const host = new FakeHost();
    const connection = new FakeConnection();
    let receive: ((notification: unknown) => Promise<void>) | undefined;
    createOmpExtension({
      env: { ASR_EXECUTABLE: "/opt/asr/bin/asr" },
      async connect(_selection, onNotification) {
        receive = onNotification as (notification: unknown) => Promise<void>;
        return connection;
      },
    })(host);
    const context = createContext();
    await host.emit("session_start", {}, context);

    const work = {
      method: "notifications/agent_session_router/work",
      params: {
        v: 1,
        workspace: "red",
        requestId: "request-1",
        from: "coordinator",
        content: "first",
        timeoutMs: 1_000,
      },
    };
    await receive?.(work);
    await receive?.({ ...work, params: { ...work.params, requestId: "request-2" } });
    expect(connection.notifications.at(-1)).toEqual({
      method: "notifications/agent_session_router/work_finished",
      params: { v: 1, requestId: "request-2", error: "session_busy" },
    });

    await receive?.({
      method: "notifications/agent_session_router/cancel",
      params: { v: 1, workspace: "red", requestId: "other" },
    });
    expect(context.aborts).toBe(0);
    await receive?.({
      method: "notifications/agent_session_router/cancel",
      params: { v: 1, workspace: "red", requestId: "request-1" },
    });
    expect(context.aborts).toBe(1);

    await host.emit("session_switch", {}, context);
    expect(connection.closed).toBe(true);
  });
});
function onboardingFixture() {
  const home = realpathSync(mkdtempSync(join(tmpdir(), "asr-omp-binding-")));
  const configPath = join(home, ".config", "agent-session-router", "config.json");
  const descriptor = join(dirname(configPath), "hosts", "omp.json");
  mkdirSync(dirname(descriptor), { recursive: true, mode: 0o700 });
  const executable = join(home, "bin", "asr");
  mkdirSync(dirname(executable), { mode: 0o700 });
  writeFileSync(executable, "fixture executable; never invoked by these tests", { mode: 0o755 });
  const binding: OnboardingBinding = { version: 1, executable, profile: "office", workspace: "team-room" };
  writeFileSync(descriptor, JSON.stringify(binding), { mode: 0o600 });
  return {
    home,
    configPath,
    descriptor,
    binding,
    env: { HOME: home, ASR_CONFIG_PATH: configPath },
    remove() { rmSync(home, { recursive: true, force: true }); },
  };
}

function displayedResult(host: FakeHost): Record<string, unknown> {
  const message = host.displayed.at(-1);
  expect(message?.message.display).toBe(true);
  expect(message?.options.triggerTurn).toBe(false);
  const json = message?.message.content.match(/```json\n([\s\S]*)\n```$/)?.[1];
  if (!json) throw new Error("missing persistent command result");
  return JSON.parse(json) as Record<string, unknown>;
}

describe("OMP onboarding descriptor", () => {
  test("uses native path precedence and tilde expansion without reading config or credentials", () => {
    const fixture = onboardingFixture();
    try {
      expect(readOnboardingBinding({ HOME: fixture.home })).toEqual(fixture.binding);
      expect(readOnboardingBinding({
        HOME: "/not-used",
        XDG_CONFIG_HOME: join(fixture.home, ".config"),
      })).toEqual(fixture.binding);
      expect(readOnboardingBinding({
        HOME: fixture.home,
        XDG_CONFIG_HOME: "/not-used",
        ASR_CONFIG_PATH: "~/.config/agent-session-router/config.json",
        ASR_CREDENTIAL_FILE: "/must-not-read",
      })).toEqual(fixture.binding);
      expect(readOnboardingBinding({
        HOME: "/not-used",
        XDG_CONFIG_HOME: "/not-used",
        ASR_CONFIG_PATH: fixture.configPath,
      })).toEqual(fixture.binding);
      expect(() => readOnboardingBinding({
        HOME: fixture.home,
        ASR_CONFIG_PATH: "~/../config.json",
      })).toThrow("onboarding_path_unsafe");
    } finally {
      fixture.remove();
    }
  });

  test("absence leaves standalone sessions unconnected while invalid descriptors fail before connecting", async () => {
    const fixture = onboardingFixture();
    try {
      unlinkSync(fixture.descriptor);
      const host = new FakeHost();
      let connections = 0;
      createOmpExtension({
        env: fixture.env,
        async connect() { connections += 1; return new FakeConnection(); },
      })(host);
      const context = createContext();
      await host.emit("session_start", {}, context);
      expect(connections).toBe(0);
      expect(context.notices).toEqual([]);
      writeFileSync(fixture.descriptor, JSON.stringify({ ...fixture.binding, profile: "SECRET-invalid\n" }), { mode: 0o600 });
      await host.emit("session_start", {}, context);
      expect(connections).toBe(0);
      expect(context.notices.at(-1)).toContain("onboarding_binding_invalid");
      expect(context.notices.join(" ")).not.toContain("SECRET");
    } finally {
      fixture.remove();
    }
  });

  test("rejects native name boundaries, unknown/duplicate fields and relative executables", () => {
    const fixture = onboardingFixture();
    try {
      for (const [field, value] of [
        ["profile", "office\n"],
        ["workspace", "team room"],
        ["profile", "équipe"],
        ["workspace", `a${"b".repeat(64)}`],
        ["profile", "-office"],
        ["version", 2],
        ["credentialToken", "SECRET-token"],
        ["executable", "./asr"],
      ] as const) {
        writeFileSync(fixture.descriptor, JSON.stringify({ ...fixture.binding, [field]: value }));
        expect(() => readOnboardingBinding(fixture.env)).toThrow("onboarding_");
      }
      writeFileSync(fixture.descriptor, JSON.stringify(fixture.binding).replace(
        '"profile":"office"',
        '"profile":"office","\\u0070rofile":"office"',
      ));
      expect(() => readOnboardingBinding(fixture.env)).toThrow("onboarding_binding_invalid");
      writeFileSync(fixture.descriptor, JSON.stringify({
        ...fixture.binding, profile: `A${"b".repeat(63)}`, workspace: "a.B_-9",
      }));
      expect(readOnboardingBinding(fixture.env)?.workspace).toBe("a.B_-9");
    } finally {
      fixture.remove();
    }
  });

  test("rejects exposed descriptor/parent modes and hardlinks", () => {
    const fixture = onboardingFixture();
    try {
      chmodSync(fixture.descriptor, 0o644);
      expect(() => readOnboardingBinding(fixture.env)).toThrow("onboarding_binding_permissions");
      chmodSync(fixture.descriptor, 0o600);
      chmodSync(dirname(fixture.descriptor), 0o755);
      expect(() => readOnboardingBinding(fixture.env)).toThrow("onboarding_binding_permissions");
      chmodSync(dirname(fixture.descriptor), 0o700);
      chmodSync(dirname(fixture.configPath), 0o755);
      expect(() => readOnboardingBinding(fixture.env)).toThrow("onboarding_binding_permissions");
      chmodSync(dirname(fixture.configPath), 0o700);
      linkSync(fixture.descriptor, join(fixture.home, "second-link"));
      expect(() => readOnboardingBinding(fixture.env)).toThrow("onboarding_binding_permissions");
    } finally {
      fixture.remove();
    }
  });

  test("does not follow descriptor, parent, or executable symlinks", () => {
    const fixture = onboardingFixture();
    try {
      const saved = join(fixture.home, "saved-descriptor");
      renameSync(fixture.descriptor, saved);
      symlinkSync(saved, fixture.descriptor);
      expect(() => readOnboardingBinding(fixture.env)).toThrow("onboarding_binding_permissions");
      unlinkSync(fixture.descriptor);
      renameSync(saved, fixture.descriptor);
      const hosts = dirname(fixture.descriptor);
      const movedHosts = join(fixture.home, "saved-hosts");
      renameSync(hosts, movedHosts);
      symlinkSync(movedHosts, hosts);
      expect(() => readOnboardingBinding(fixture.env)).toThrow("onboarding_path_unsafe");
      unlinkSync(hosts);
      renameSync(movedHosts, hosts);
      const savedExecutable = join(fixture.home, "saved-asr");
      renameSync(fixture.binding.executable, savedExecutable);
      symlinkSync(savedExecutable, fixture.binding.executable);
      expect(() => readOnboardingBinding(fixture.env)).toThrow("onboarding_executable_invalid");
    } finally {
      fixture.remove();
    }
  });

  test("bounds descriptor bytes and rejects nonregular/nonexecutable paths", () => {
    const fixture = onboardingFixture();
    try {
      writeFileSync(fixture.descriptor, " ".repeat(16 * 1024 + 1));
      expect(() => readOnboardingBinding(fixture.env)).toThrow("onboarding_binding_too_large");
      writeFileSync(fixture.descriptor, JSON.stringify(fixture.binding));
      chmodSync(fixture.binding.executable, 0o644);
      expect(() => readOnboardingBinding(fixture.env)).toThrow("onboarding_executable_invalid");
      chmodSync(fixture.binding.executable, 0o777);
      expect(() => readOnboardingBinding(fixture.env)).toThrow("onboarding_executable_invalid");
      unlinkSync(fixture.binding.executable);
      mkdirSync(fixture.binding.executable, { mode: 0o700 });
      expect(() => readOnboardingBinding(fixture.env)).toThrow("onboarding_executable_invalid");
      unlinkSync(fixture.descriptor);
      mkdirSync(fixture.descriptor, { mode: 0o700 });
      expect(() => readOnboardingBinding(fixture.env)).toThrow("onboarding_binding_permissions");
    } finally {
      fixture.remove();
    }
  });

  test("concurrent startup waits for MCP connection and sends only one native-schema join", async () => {
    const fixture = onboardingFixture();
    try {
      const host = new FakeHost();
      const connection = new FakeConnection();
      const { promise, resolve } = Promise.withResolvers<OmpMcpConnection>();
      let connects = 0;
      createOmpExtension({
        env: fixture.env,
        async connect() { connects += 1; return promise; },
      })(host);
      const context = createContext();
      const first = host.emit("session_start", {}, context);
      const second = host.emit("session_start", {}, context);
      expect(connection.workspace).toBeNull();
      resolve(connection);
      await Promise.all([first, second]);
      expect(connects).toBe(1);
      expect(connection.calls.filter((call) => call.name === "workspace_join").map((call) => call.args))
        .toEqual([{ name: "team-room" }]);
      expect(connection.workspace).toBe("team-room");
    } finally {
      fixture.remove();
    }
  });

  test("ordinary startup joins once after connecting and reapplies the workspace only for a new session", async () => {
    const fixture = onboardingFixture();
    try {
      const host = new FakeHost();
      const selections: ChildSelection[] = [];
      const connections: FakeConnection[] = [];
      createOmpExtension({
        env: { ...fixture.env, ASR_CREDENTIAL_FILE: "/must-not-use", ASR_PROFILE: "ambient" },
        async connect(selection) {
          selections.push(selection);
          const connection = new FakeConnection();
          connections.push(connection);
          return connection;
        },
      })(host);
      const context = createContext();
      await host.emit("session_start", {}, context);
      await host.emit("session_start", {}, context);
      expect(selections).toEqual([{
        executable: fixture.binding.executable,
        profile: "office",
        workspace: "team-room",
        cwd: context.cwd,
      }]);
      expect(connections[0]?.calls.filter((call) => call.name === "workspace_join").map((call) => call.args))
        .toEqual([{ name: "team-room" }]);
      writeFileSync(fixture.descriptor, JSON.stringify({ ...fixture.binding, workspace: "second-room" }));
      await host.emit("session_start", {}, context);
      expect(connections[0]?.workspace).toBe("team-room");
      await host.emit("session_switch", {}, context);
      expect(connections[0]?.closed).toBe(true);
      await host.emit("session_start", {}, context);
      expect(connections[1]?.workspace).toBe("second-room");
      expect(connections[1]?.calls.filter((call) => call.name === "workspace_join")).toHaveLength(1);
    } finally {
      fixture.remove();
    }
  });

  test("explicit wrapper selection bypasses even an invalid descriptor", async () => {
    const fixture = onboardingFixture();
    try {
      writeFileSync(fixture.descriptor, "invalid SECRET configuration");
      const host = new FakeHost();
      const connection = new FakeConnection();
      let selection: ChildSelection | undefined;
      createOmpExtension({
        env: {
          ...fixture.env, ASR_EXECUTABLE: "/wrapper/asr", ASR_PROFILE: "wrapper",
          ASR_WORKSPACE: "wrapper-room", ASR_CREDENTIAL_FILE: "/wrapper/credential.json",
        },
        async connect(value) { selection = value; return connection; },
      })(host);
      const context = createContext();
      await host.emit("session_start", {}, context);
      expect(selection).toMatchObject({
        executable: "/wrapper/asr", profile: "wrapper", credentialFile: "/wrapper/credential.json",
      });
      expect(connection.workspace).toBe("wrapper-room");
      expect(context.notices).toEqual([]);
    } finally {
      fixture.remove();
    }
  });
});

describe("OMP workspace commands", () => {
  test("both command aliases join an unjoined connection, preserve it on leave, and never hot-swap profiles", async () => {
    const host = new FakeHost();
    const connection = new FakeConnection();
    let connects = 0;
    createOmpExtension({
      env: { ASR_EXECUTABLE: "/wrapper/asr", ASR_PROFILE: "office" },
      async connect() { connects += 1; return connection; },
    })(host);
    const context = createContext();
    await host.emit("session_start", {}, context);
    await host.commands.asr!.handler("workspace join red", context);
    expect(connection.workspace).toBe("red");
    expect(displayedResult(host)).toEqual({ workspace: "red", cursor: 0 });
    await host.commands.workspace!.handler("join blue", context);
    expect(context.notices.at(-1)).toContain("leave_required");
    expect(connection.workspace).toBe("red");
    await host.commands.asr!.handler("workspace leave", context);
    expect(connection.workspace).toBeNull();
    expect(connection.closed).toBe(false);
    await host.commands.workspace!.handler("join blue --profile other", context);
    expect(context.notices.at(-1)).toContain("profile_restart_required");
    expect(connection.workspace).toBeNull();
    await host.commands.workspace!.handler("join blue", context);
    expect(connection.workspace).toBe("blue");
    expect(connects).toBe(1);
    await expect(host.tools.workspace_join!.execute(
      "wrong-schema", { workspace: "blue" }, undefined, undefined, context,
    )).rejects.toThrow("invalid_message");
  });

  test("list/find traverse accessible pages and show real data without prompting an agent turn", async () => {
    const host = new FakeHost();
    const connection = new FakeConnection();
    connection.respond = (name, args) => {
      if (name !== "workspace_list") return undefined;
      const names = args.after ? ["MIX-room", "zebra"] : ["Alpha", "K-room"];
      return { structuredContent: {
        workspaces: names.map((name) => ({ name, createdAt: 1, connectedAgents: 0 })),
        hasMore: !args.after,
        nextCursor: args.after ? null : "K-room",
      } };
    };
    createOmpExtension({
      env: { ASR_EXECUTABLE: "/wrapper/asr" },
      async connect() { return connection; },
    })(host);
    const context = createContext();
    await host.emit("session_start", {}, context);
    await host.commands.asr!.handler("workspace list", context);
    expect((displayedResult(host).workspaces as Array<{ name: string }>).map((entry) => entry.name))
      .toEqual(["Alpha", "K-room", "MIX-room", "zebra"]);
    await host.commands.workspace!.handler("find mix", context);
    expect(displayedResult(host).workspaces).toEqual([{ name: "MIX-room", createdAt: 1, connectedAgents: 0 }]);
    await host.commands.asr!.handler('workspace find ""', context);
    expect((displayedResult(host).workspaces as unknown[])).toHaveLength(4);
    await host.commands.asr!.handler("workspace find K", context);
    expect(displayedResult(host).workspaces).toEqual([]);
    expect(host.messages).toEqual([]);
    expect(context.notices).toEqual([]);
  });

  test("repeated pagination cursors fail instead of looping or claiming a complete list", async () => {
    const host = new FakeHost();
    const connection = new FakeConnection();
    connection.respond = (name) => name === "workspace_list" ? {
      structuredContent: { workspaces: [], hasMore: true, nextCursor: "repeat" },
    } : undefined;
    createOmpExtension({
      env: { ASR_EXECUTABLE: "/wrapper/asr" },
      async connect() { return connection; },
    })(host);
    const context = createContext();
    await host.emit("session_start", {}, context);
    await host.commands.workspace!.handler("list", context);
    expect(connection.calls.filter((call) => call.name === "workspace_list")).toHaveLength(2);
    expect(context.notices.at(-1)).toContain("invalid_workspace_cursor");
    expect(host.displayed).toEqual([]);
  });

  test("members/history/post show MCP results while create stays a server-admin instruction", async () => {
    const host = new FakeHost();
    const connection = new FakeConnection();
    createOmpExtension({
      env: { ASR_EXECUTABLE: "/wrapper/asr", ASR_WORKSPACE: "red" },
      async connect() { return connection; },
    })(host);
    const context = createContext();
    await host.emit("session_start", {}, context);
    await host.commands.workspace!.handler("members", context);
    expect(displayedResult(host).agents).toEqual([
      { agentId: "omp-self", side: "generic", client: "omp", status: "idle", ready: true },
    ]);
    await host.commands.asr!.handler("workspace history", context);
    expect(displayedResult(host).events).toEqual([{ seq: 1, content: "durable chat" }]);
    await host.commands.asr!.handler('workspace post "hello team"', context);
    expect(displayedResult(host)).toEqual({ workspace: "red", seq: 2 });
    expect(connection.calls.at(-1)?.args).toEqual({ content: "hello team" });
    const callsBeforeCreate = connection.calls.length;
    await host.commands.workspace!.handler("create blue", context);
    expect(displayedResult(host).instruction).toContain("asr onboarding prompt --workspace blue --create-workspace");
    expect(connection.calls.length).toBe(callsBeforeCreate);
    expect(host.messages).toEqual([]);
  });

  test("status requires a current join, accessible listing and unambiguous stable self evidence", async () => {
    const host = new FakeHost();
    const connection = new FakeConnection();
    createOmpExtension({
      env: { ASR_EXECUTABLE: "/wrapper/asr" },
      async connect() { return connection; },
    })(host);
    const context = createContext();
    await host.emit("session_start", {}, context);
    await host.commands.workspace!.handler("status", context);
    expect(displayedResult(host).participation).toBe("not_checked");
    await host.commands.workspace!.handler("join red", context);
    await host.commands.asr!.handler("workspace status", context);
    expect(displayedResult(host)).toMatchObject({
      workspace: "red", agentId: "omp-self", participation: "confirmed",
    });
    let memberReads = 0;
    connection.respond = (name) => {
      if (name !== "workspace_members") return undefined;
      memberReads += 1;
      return { structuredContent: { agents: [{
        agentId: memberReads === 1 ? "old-omp" : "new-omp", side: "generic", client: "omp",
      }] } };
    };
    await host.commands.workspace!.handler("status", context);
    expect(displayedResult(host).participation).toBe("not_checked");
    expect(displayedResult(host).agentId).toBeUndefined();
    connection.respond = (name) => name === "workspace_members" ? {
      structuredContent: { agents: ["one", "two"].map((agentId) => ({ agentId, side: "generic", client: "omp" })) },
    } : undefined;
    await host.commands.workspace!.handler("status", context);
    expect(displayedResult(host).participation).toBe("not_checked");
  });

  test("registered workspace tools run during a user turn without bypassing leave or native stop fences", async () => {
    const host = new FakeHost();
    const connection = new FakeConnection();
    createOmpExtension({
      env: { ASR_EXECUTABLE: "/wrapper/asr" },
      async connect() { return connection; },
    })(host);
    const context = createContext();
    await host.emit("session_start", {}, context);
    context.idle = false;
    await host.emit("agent_start", {}, context);
    expect(await host.tools.workspace_join!.execute(
      "join-red", { name: "red" }, undefined, undefined, context,
    )).toMatchObject({ structuredContent: { workspace: "red" } });
    await expect(host.tools.workspace_join!.execute(
      "join-blue", { name: "blue" }, undefined, undefined, context,
    )).rejects.toThrow("leave_required");
    connection.respond = (name) => name === "workspace_leave" ? {
      isError: true, structuredContent: { ok: false, error: "task_stop_unconfirmed" },
    } : undefined;
    expect(await host.tools.workspace_leave!.execute(
      "fenced-leave", {}, undefined, undefined, context,
    )).toMatchObject({ isError: true, structuredContent: { error: "task_stop_unconfirmed" } });
    expect(connection.workspace).toBe("red");
    await expect(host.tools.workspace_join!.execute(
      "still-joined", { name: "blue" }, undefined, undefined, context,
    )).rejects.toThrow("leave_required");
    connection.respond = undefined;
    expect(await host.tools.workspace_leave!.execute(
      "leave-red", {}, undefined, undefined, context,
    )).toMatchObject({ structuredContent: { workspace: "red" } });
    expect(await host.tools.workspace_join!.execute(
      "join-blue-after-leave", { name: "blue" }, undefined, undefined, context,
    )).toMatchObject({ structuredContent: { workspace: "blue" } });
    expect(connection.workspace).toBe("blue");
    expect(connection.closed).toBe(false);
  });

  test("active work and native stop fences retain membership; failed tool results never count as leave", async () => {
    const host = new FakeHost();
    const connection = new FakeConnection();
    let receive: ((notification: unknown) => Promise<void>) | undefined;
    createOmpExtension({
      env: { ASR_EXECUTABLE: "/wrapper/asr", ASR_WORKSPACE: "red" },
      async connect(_selection, onNotification) {
        receive = onNotification as (notification: unknown) => Promise<void>;
        return connection;
      },
    })(host);
    const context = createContext();
    await host.emit("session_start", {}, context);
    await receive?.({
      method: "notifications/agent_session_router/work",
      params: { v: 1, workspace: "red", requestId: "work", from: "manager", content: "review", timeoutMs: 1_000 },
    });
    context.idle = false;
    await expect(host.tools.workspace_leave!.execute(
      "active-work-leave", {}, undefined, undefined, context,
    )).rejects.toThrow("workspace_busy");
    await host.commands.workspace!.handler("leave", context);
    expect(context.notices.at(-1)).toContain("workspace_busy");
    expect(connection.calls.filter((call) => call.name === "workspace_leave")).toEqual([]);
    context.idle = true;
    await host.emit("agent_end", { isTerminal: true }, context);
    connection.respond = (name) => name === "workspace_leave" ? {
      isError: true, structuredContent: { ok: false, error: "task_stop_unconfirmed" },
      content: [{ type: "text", text: "must not show SECRET raw output" }],
    } : undefined;
    await host.commands.asr!.handler("workspace leave", context);
    expect(context.notices.at(-1)).toContain("task_stop_unconfirmed");
    expect(context.notices.join(" ")).not.toContain("SECRET");
    expect(connection.workspace).toBe("red");
    expect(connection.closed).toBe(false);
    await host.commands.workspace!.handler("join blue", context);
    expect(context.notices.at(-1)).toContain("leave_required");
    connection.respond = undefined;
    await host.commands.workspace!.handler("leave", context);
    await host.commands.workspace!.handler("join blue", context);
    expect(connection.workspace).toBe("blue");
  });

  test("pending requests block leave and uncertain leave cannot be retried or turned into a room switch", async () => {
    const host = new FakeHost();
    const connection = new FakeConnection();
    const { promise: pending, resolve: release } = Promise.withResolvers<unknown>();
    const { promise: started, resolve: entered } = Promise.withResolvers<void>();
    connection.respond = (name) => {
      if (name !== "agent_send") return undefined;
      entered();
      return pending;
    };
    createOmpExtension({
      env: { ASR_EXECUTABLE: "/wrapper/asr", ASR_WORKSPACE: "red" },
      async connect() { return connection; },
    })(host);
    const context = createContext();
    await host.emit("session_start", {}, context);
    context.idle = false;
    const request = host.tools.agent_send!.execute("send", { targetId: "peer" }, undefined, undefined, context);
    await started;
    await expect(host.tools.workspace_leave!.execute(
      "pending-request-leave", {}, undefined, undefined, context,
    )).rejects.toThrow("workspace_busy");
    await host.commands.workspace!.handler("leave", context);
    expect(context.notices.at(-1)).toContain("workspace_busy");
    expect(connection.workspace).toBe("red");
    release({ structuredContent: { ok: true } });
    await request;
    connection.respond = (name) => name === "workspace_leave" ? {
      isError: true, structuredContent: { ok: false, error: "leave_unconfirmed" },
    } : undefined;
    await host.commands.workspace!.handler("leave", context);
    const leaveCalls = connection.calls.filter((call) => call.name === "workspace_leave").length;
    await host.commands.workspace!.handler("leave", context);
    await host.commands.workspace!.handler("join blue", context);
    expect(context.notices.at(-1)).toContain("leave_unconfirmed");
    expect(connection.calls.filter((call) => call.name === "workspace_leave")).toHaveLength(leaveCalls);
    expect(connection.workspace).toBe("red");
    expect(connection.closed).toBe(false);
  });
});
