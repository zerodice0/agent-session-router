import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import type { AgentMessenger } from "./agent-messenger";
import {
  CLAUDE_CHANNEL_NOTIFICATION_METHOD,
  type ClaudeChannelAdapter,
  type ClaudeChannelEvent,
} from "./claude-channel-adapter";
import { runAgentListTool, runAgentSendTool } from "./agent-tools-mcp";
import { MAX_REQUEST_TIMEOUT_MS } from "./protocol";

export const CLAUDE_CHANNEL_SERVER_NAME = "agent-session-router-channel";

const CHANNEL_INSTRUCTIONS =
  "Targeted agent requests arrive as <channel> events with request_id, from, and timeout_ms. " +
  "Handle one request at a time and call agent_reply exactly once with the same request_id and final text. " +
  "Use agent_list and agent_send for correlated requests to other connected agents.";

export interface ClaudeChannelMcpOptions {
  agentId: string;
  adapter: ClaudeChannelAdapter;
  messenger: AgentMessenger;
}

export interface ClaudeChannelMcpRuntime {
  server: McpServer;
  notify(event: ClaudeChannelEvent): Promise<void>;
}

export function createClaudeChannelMcpServer(
  options: ClaudeChannelMcpOptions,
): ClaudeChannelMcpRuntime {
  const server = new McpServer(
    { name: CLAUDE_CHANNEL_SERVER_NAME, version: "0.1.0" },
    {
      capabilities: {
        experimental: { "claude/channel": {} },
        tools: {},
      },
      instructions: CHANNEL_INSTRUCTIONS,
    },
  );

  server.registerTool(
    "agent_reply",
    {
      title: "Reply to a routed request",
      description:
        "Complete the current channel request using its exact request_id and final response text.",
      inputSchema: {
        request_id: z.string().min(1).max(128),
        text: z.string().min(1),
      },
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: false,
        openWorldHint: false,
      },
    },
    async ({ request_id, text }) => {
      const result = options.adapter.reply(request_id, text);
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
    },
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

  return {
    server,
    notify(event) {
      return server.server.notification({
        method: CLAUDE_CHANNEL_NOTIFICATION_METHOD,
        params: event,
      });
    },
  };
}
