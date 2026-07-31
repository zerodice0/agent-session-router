import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import type { AgentMessenger } from "./agent-messenger";
import { runAgentListTool, runAgentSendTool } from "./agent-tools-mcp";
import {
  DEFAULT_CODEX_CLI_WAIT_MS,
  MAX_CODEX_CLI_WAIT_MS,
  type CodexCliInboxAdapter,
} from "./codex-cli-inbox-adapter";
import { MAX_REQUEST_TIMEOUT_MS } from "./protocol";

export const CODEX_CLI_MCP_SERVER_NAME = "agent-session-router-codex-cli";

const CODEX_CLI_INSTRUCTIONS =
  "Use agent_list and agent_send for targeted requests to other connected agents. " +
  "When asked to receive routed work, call agent_wait. If it returns a request, handle only that request, " +
  "then call agent_reply exactly once with the same requestId before waiting again. " +
  "A wait timeout is normal and may be retried; never invent or reuse request IDs.";

export interface CodexCliMcpOptions {
  agentId: string;
  adapter: CodexCliInboxAdapter;
  messenger: AgentMessenger;
}

export function createCodexCliMcpServer(options: CodexCliMcpOptions): McpServer {
  const server = new McpServer(
    { name: CODEX_CLI_MCP_SERVER_NAME, version: "0.1.0" },
    { instructions: CODEX_CLI_INSTRUCTIONS },
  );

  server.registerTool(
    "agent_list",
    {
      title: "List connected agents",
      description:
        "List other agents with their provider side, router-derived status, and optional public activity.",
      inputSchema: {},
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async () => runAgentListTool(options.messenger, options.agentId),
  );

  server.registerTool(
    "agent_send",
    {
      title: "Send a request to another agent",
      description:
        "Send one correlated request to a specific connected agent and wait for its final response.",
      inputSchema: {
        target: z.string().min(1).max(128),
        message: z.string().min(1),
        timeoutMs: z.number().int().min(1).max(MAX_REQUEST_TIMEOUT_MS).optional(),
      },
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: false,
        openWorldHint: true,
      },
    },
    async ({ target, message, timeoutMs }) =>
      runAgentSendTool(options.messenger, options.agentId, {
        target,
        message,
        ...(timeoutMs === undefined ? {} : { timeoutMs }),
      }),
  );

  server.registerTool(
    "agent_wait",
    {
      title: "Wait for one routed request",
      description:
        "Wait for one inbound request. After receiving it, call agent_reply once with the same requestId.",
      inputSchema: {
        waitMs: z
          .number()
          .int()
          .min(1)
          .max(MAX_CODEX_CLI_WAIT_MS)
          .optional()
          .default(DEFAULT_CODEX_CLI_WAIT_MS),
      },
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: false,
        openWorldHint: false,
      },
    },
    async ({ waitMs }) => runAgentWaitTool(options.adapter, waitMs),
  );

  server.registerTool(
    "agent_reply",
    {
      title: "Reply to one routed request",
      description:
        "Complete the request returned by agent_wait using its exact requestId and final response text.",
      inputSchema: {
        requestId: z.string().min(1).max(128),
        text: z.string().min(1),
      },
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: false,
        openWorldHint: false,
      },
    },
    async ({ requestId, text }) => runAgentReplyTool(options.adapter, requestId, text),
  );

  return server;
}

export async function runAgentWaitTool(adapter: CodexCliInboxAdapter, waitMs: number) {
  const result = await adapter.wait(waitMs);
  if (!result.ok) {
    const normalTimeout = result.error === "wait_timeout";
    return {
      ...(normalTimeout ? {} : { isError: true }),
      content: [
        {
          type: "text" as const,
          text: normalTimeout
            ? "No routed request arrived before the wait deadline."
            : `agent_wait failed: ${result.error}`,
        },
      ],
      structuredContent: { ok: false, error: result.error },
    };
  }

  const message = {
    requestId: result.message.requestId,
    from: result.message.from,
    message: result.message.content,
    timeoutMs: result.message.timeoutMs,
  };
  return {
    content: [{ type: "text" as const, text: JSON.stringify(message) }],
    structuredContent: { ok: true, ...message },
  };
}

export function runAgentReplyTool(
  adapter: CodexCliInboxAdapter,
  requestId: string,
  text: string,
) {
  const result = adapter.reply(requestId, text);
  if (!result.ok) {
    return {
      isError: true,
      content: [{ type: "text" as const, text: `agent_reply failed: ${result.error}` }],
      structuredContent: { ok: false, error: result.error },
    };
  }
  return {
    content: [{ type: "text" as const, text: "agent_reply accepted" }],
    structuredContent: { ok: true },
  };
}
