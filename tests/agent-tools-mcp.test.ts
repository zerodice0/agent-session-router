import { describe, expect, test } from "bun:test";
import type { AgentMessenger, AgentSendOptions, AgentSendResult } from "../src/agent-messenger";
import {
  AGENT_ROUTER_AGENT_ID_ENV,
  AGENT_ROUTER_DELEGATION_TOKEN_ENV,
  AGENT_ROUTER_URL_ENV,
  createAgentToolsEnvironment,
  createCodexAppServerCommand,
  createDelegationToken,
} from "../src/agent-tools-config";
import { runAgentListTool, runAgentSendTool } from "../src/agent-tools-mcp";
import { isDelegationToken, type AgentDescriptor } from "../src/protocol";

class RecordingMessenger implements AgentMessenger {
  readonly sends: Array<{ target: string; content: string; options?: AgentSendOptions }> = [];

  constructor(
    readonly agents: AgentDescriptor[],
    readonly result: AgentSendResult = {
      requestId: "tool-request",
      from: "local:worker-b",
      ok: true,
      content: "worker-result",
    },
  ) {}

  async listAgents(): Promise<AgentDescriptor[]> {
    return this.agents;
  }

  async send(
    target: string,
    content: string,
    options?: AgentSendOptions,
  ): Promise<AgentSendResult> {
    this.sends.push({ target, content, options });
    return this.result;
  }
}

describe("agent tools MCP", () => {
  test("lists other agents without exposing the caller as a target", async () => {
    const messenger = new RecordingMessenger([
      { agentId: "local:worker-a", side: "codex" },
      { agentId: "local:worker-b", side: "codex" },
    ]);

    const result = await runAgentListTool(messenger, "local:worker-a");
    expect(result.isError).not.toBe(true);
    expect(result.structuredContent).toEqual({
      agents: [{ agentId: "local:worker-b", side: "codex" }],
    });
  });

  test("returns a correlated agent response and rejects self-send", async () => {
    const messenger = new RecordingMessenger([]);
    const result = await runAgentSendTool(messenger, "local:worker-a", {
      target: "local:worker-b",
      message: "delegated-task",
      timeoutMs: 5_000,
    });

    expect(result).toMatchObject({
      structuredContent: {
        requestId: "tool-request",
        from: "local:worker-b",
        ok: true,
      },
      content: [{ type: "text", text: "worker-result" }],
    });
    expect(messenger.sends).toEqual([
      {
        target: "local:worker-b",
        content: "delegated-task",
        options: { timeoutMs: 5_000 },
      },
    ]);

    const self = await runAgentSendTool(messenger, "local:worker-a", {
      target: "local:worker-a",
      message: "self-task",
    });
    expect(self).toMatchObject({ isError: true });
    expect(messenger.sends).toHaveLength(1);
  });

  test("maps router failures without returning message content", async () => {
    const messenger = new RecordingMessenger([], {
      requestId: "tool-request",
      ok: false,
      error: "target_offline",
    });
    const result = await runAgentSendTool(messenger, "local:worker-a", {
      target: "local:worker-b",
      message: "delegated-task",
    });

    expect(result).toMatchObject({
      isError: true,
      structuredContent: {
        requestId: "tool-request",
        ok: false,
        error: "target_offline",
      },
    });
  });
});

describe("Codex MCP runtime configuration", () => {
  test("uses only env variable names in process arguments", () => {
    const command = createCodexAppServerCommand();
    const joined = command.join(" ");
    expect(command.slice(0, 2)).toEqual(["codex", "app-server"]);
    expect(joined).toContain(AGENT_ROUTER_URL_ENV);
    expect(joined).toContain(AGENT_ROUTER_AGENT_ID_ENV);
    expect(joined).toContain(AGENT_ROUTER_DELEGATION_TOKEN_ENV);
    expect(joined).toContain('enabled_tools=["agent_list","agent_send"]');
    expect(joined).toContain('default_tools_approval_mode="approve"');
  });

  test("creates a valid high-entropy delegation token and isolated environment", () => {
    const delegationToken = createDelegationToken();
    expect(isDelegationToken(delegationToken)).toBe(true);
    expect(createAgentToolsEnvironment({
      routerUrl: "ws://127.0.0.1:18787/ws",
      agentId: "local:worker-a",
      delegationToken,
    })).toEqual({
      [AGENT_ROUTER_URL_ENV]: "ws://127.0.0.1:18787/ws",
      [AGENT_ROUTER_AGENT_ID_ENV]: "local:worker-a",
      [AGENT_ROUTER_DELEGATION_TOKEN_ENV]: delegationToken,
    });
  });
});
