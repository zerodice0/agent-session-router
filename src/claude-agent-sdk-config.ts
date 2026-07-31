import {
  createSdkMcpServer,
  tool,
  type McpServerConfig,
  type Options,
} from "@anthropic-ai/claude-agent-sdk";
import { z } from "zod";
import {
  AGENT_TOOLS_MCP_SERVER_ID,
  type AgentToolsRuntimeConfig,
} from "./agent-tools-config";
import { runAgentListTool, runAgentSendTool } from "./agent-tools-mcp";
import { DelegatedMessengerClient } from "./delegated-messenger-client";
import { MAX_REQUEST_TIMEOUT_MS } from "./protocol";

export const CLAUDE_AGENT_LIST_TOOL = `mcp__${AGENT_TOOLS_MCP_SERVER_ID}__agent_list`;
export const CLAUDE_AGENT_SEND_TOOL = `mcp__${AGENT_TOOLS_MCP_SERVER_ID}__agent_send`;

const CENTRAL_ENVIRONMENT_KEYS = new Set([
  "ROUTER_TOKEN",
  "ROUTER_URL",
  "GATEWAY_AGENT_ID",
  "GATEWAY_AGENT_ACTIVITY",
  "AGENT_ROUTER_URL",
  "AGENT_ROUTER_AGENT_ID",
  "AGENT_ROUTER_DELEGATION_TOKEN",
  "CLAUDE_CWD",
  "CLAUDE_SESSION_ID",
  "CLAUDE_CODE_EXECUTABLE",
  "CODEX_CWD",
  "CODEX_THREAD_ID",
]);

export interface ClaudeAgentSdkRuntimeConfig extends AgentToolsRuntimeConfig {
  cwd?: string;
  executablePath?: string;
  maxTurns?: number;
  processEnvironment?: Record<string, string | undefined>;
}

export interface ClaudeAgentSdkConfiguration {
  options: Omit<Options, "abortController" | "resume" | "continue" | "forkSession">;
  close(): void;
}

/**
 * Keeps provider authentication/runtime variables while removing central
 * router credentials and addressing metadata from the Claude subprocess.
 */
export function createClaudeProcessEnvironment(
  source: Record<string, string | undefined> = process.env,
): Record<string, string | undefined> {
  const environment: Record<string, string | undefined> = {};
  for (const [key, value] of Object.entries(source)) {
    if (!CENTRAL_ENVIRONMENT_KEYS.has(key)) environment[key] = value;
  }
  environment.CLAUDE_AGENT_SDK_CLIENT_APP = "agent-session-router/0.1.0";
  return environment;
}

export function createClaudeAgentSdkConfiguration(
  config: ClaudeAgentSdkRuntimeConfig,
): ClaudeAgentSdkConfiguration {
  const tools = createClaudeAgentTools(config);
  return {
    close: tools.close,
    options: {
      tools: [],
      allowedTools: [CLAUDE_AGENT_LIST_TOOL, CLAUDE_AGENT_SEND_TOOL],
      permissionMode: "dontAsk",
      settingSources: [],
      skills: [],
      plugins: [],
      strictMcpConfig: true,
      persistSession: true,
      maxTurns: Math.max(1, Math.floor(config.maxTurns ?? 8)),
      includePartialMessages: false,
      promptSuggestions: false,
      mcpServers: {
        [AGENT_TOOLS_MCP_SERVER_ID]: tools.server,
      },
      env: createClaudeProcessEnvironment(config.processEnvironment),
      stderr: () => {},
      ...(config.cwd === undefined ? {} : { cwd: config.cwd }),
      ...(config.executablePath === undefined
        ? {}
        : { pathToClaudeCodeExecutable: config.executablePath }),
    },
  };
}

function createClaudeAgentTools(config: AgentToolsRuntimeConfig): {
  server: McpServerConfig;
  close(): void;
} {
  let client: DelegatedMessengerClient | null = null;
  let connecting: Promise<DelegatedMessengerClient> | null = null;
  let closed = false;

  const messenger = async (): Promise<DelegatedMessengerClient> => {
    if (closed) throw new Error("Claude agent tools are closed");
    if (client?.connected) return client;
    if (connecting) return connecting;
    const next = new DelegatedMessengerClient(config);
    connecting = next
      .connect()
      .then(() => {
        if (closed) {
          next.disconnect();
          throw new Error("Claude agent tools are closed");
        }
        client = next;
        return next;
      })
      .finally(() => {
        connecting = null;
      });
    return connecting;
  };

  const server = createSdkMcpServer({
    name: AGENT_TOOLS_MCP_SERVER_ID,
    version: "0.1.0",
    alwaysLoad: true,
    tools: [
      tool(
        "agent_list",
        "List other agents with their provider side, router-derived status, and optional public activity.",
        {},
        async () => runAgentListTool(await messenger(), config.agentId),
        {
          annotations: {
            readOnlyHint: true,
            destructiveHint: false,
            idempotentHint: true,
            openWorldHint: false,
          },
        },
      ),
      tool(
        "agent_send",
        "Send one correlated request to a specific connected agent and wait for its final response.",
        {
          target: z.string().min(1).max(128),
          message: z.string().min(1),
          timeoutMs: z.number().int().min(1).max(MAX_REQUEST_TIMEOUT_MS).optional(),
        },
        async (input) =>
          runAgentSendTool(await messenger(), config.agentId, {
            target: input.target,
            message: input.message,
            ...(input.timeoutMs === undefined ? {} : { timeoutMs: input.timeoutMs }),
          }),
        {
          annotations: {
            readOnlyHint: false,
            destructiveHint: false,
            idempotentHint: false,
            openWorldHint: true,
          },
        },
      ),
    ],
  });

  return {
    server,
    close() {
      closed = true;
      client?.disconnect();
      client = null;
    },
  };
}
