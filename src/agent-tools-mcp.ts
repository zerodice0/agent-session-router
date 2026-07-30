import { McpServer, type CallToolResult } from "@modelcontextprotocol/server";
import { serveStdio } from "@modelcontextprotocol/server/stdio";
import { z } from "zod";
import type { AgentMessenger } from "./agent-messenger";
import {
  AGENT_ROUTER_AGENT_ID_ENV,
  AGENT_ROUTER_DELEGATION_TOKEN_ENV,
  AGENT_ROUTER_URL_ENV,
} from "./agent-tools-config";
import { DelegatedMessengerClient } from "./delegated-messenger-client";
import { MAX_REQUEST_TIMEOUT_MS, isAgentId, isDelegationToken } from "./protocol";

interface AgentToolsMcpOptions {
  ownAgentId: string;
  messenger: () => Promise<AgentMessenger>;
}

interface AgentSendInput {
  target: string;
  message: string;
  timeoutMs?: number;
}

export async function runAgentListTool(
  messenger: AgentMessenger,
  ownAgentId: string,
): Promise<CallToolResult> {
  try {
    const agents = (await messenger.listAgents()).filter(({ agentId }) => agentId !== ownAgentId);
    return {
      content: [{ type: "text", text: JSON.stringify({ agents }) }],
      structuredContent: { agents },
    };
  } catch {
    return {
      isError: true,
      content: [{ type: "text", text: "agent_list failed" }],
    };
  }
}

export async function runAgentSendTool(
  messenger: AgentMessenger,
  ownAgentId: string,
  input: AgentSendInput,
): Promise<CallToolResult> {
  if (input.target === ownAgentId) {
    return {
      isError: true,
      content: [{ type: "text", text: "agent_send rejected: target_self" }],
    };
  }

  try {
    const result = await messenger.send(input.target, input.message, {
      ...(input.timeoutMs === undefined ? {} : { timeoutMs: input.timeoutMs }),
    });
    if (!result.ok) {
      return {
        isError: true,
        content: [{ type: "text", text: `agent_send failed: ${result.error}` }],
        structuredContent: {
          requestId: result.requestId,
          ok: false,
          error: result.error,
        },
      };
    }

    return {
      content: [{ type: "text", text: result.content }],
      structuredContent: {
        requestId: result.requestId,
        from: result.from,
        ok: true,
      },
    };
  } catch {
    return {
      isError: true,
      content: [{ type: "text", text: "agent_send failed" }],
    };
  }
}

export function createAgentToolsMcpServer(options: AgentToolsMcpOptions): McpServer {
  const server = new McpServer({
    name: "agent-session-router",
    version: "0.1.0",
  });

  server.registerTool(
    "agent_list",
    {
      title: "List connected agents",
      description: "List other agents currently available through the session router.",
      inputSchema: z.object({}),
      outputSchema: z.object({
        agents: z.array(
          z.object({
            agentId: z.string(),
            side: z.enum(["claude", "codex", "generic"]),
          }),
        ),
      }),
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: false,
      },
    },
    async () => runAgentListTool(await options.messenger(), options.ownAgentId),
  );

  server.registerTool(
    "agent_send",
    {
      title: "Send a request to another agent",
      description:
        "Send one correlated request to a specific connected agent and wait for its final response.",
      inputSchema: z.object({
        target: z.string().min(1).max(128),
        message: z.string().min(1),
        timeoutMs: z.number().int().min(1).max(MAX_REQUEST_TIMEOUT_MS).optional(),
      }),
      annotations: {
        readOnlyHint: false,
        destructiveHint: false,
        idempotentHint: false,
        openWorldHint: true,
      },
    },
    async (input) => runAgentSendTool(await options.messenger(), options.ownAgentId, input),
  );

  return server;
}

function environmentOptions(): {
  routerUrl: string;
  agentId: string;
  delegationToken: string;
} {
  const routerUrl = process.env[AGENT_ROUTER_URL_ENV]?.trim();
  const agentId = process.env[AGENT_ROUTER_AGENT_ID_ENV]?.trim();
  const delegationToken = process.env[AGENT_ROUTER_DELEGATION_TOKEN_ENV]?.trim();
  if (!routerUrl || !agentId || !delegationToken) throw new Error("Missing MCP bridge configuration");
  if (!isAgentId(agentId) || !isDelegationToken(delegationToken)) {
    throw new Error("Invalid MCP bridge configuration");
  }
  return { routerUrl, agentId, delegationToken };
}

function lazyDelegatedMessenger(options: ReturnType<typeof environmentOptions>) {
  let client: DelegatedMessengerClient | null = null;
  let connecting: Promise<DelegatedMessengerClient> | null = null;

  return async (): Promise<AgentMessenger> => {
    if (client?.connected) return client;
    if (connecting) return connecting;

    const next = new DelegatedMessengerClient(options);
    connecting = next
      .connect()
      .then(() => {
        client = next;
        return next;
      })
      .finally(() => {
        connecting = null;
      });
    return connecting;
  };
}

if (import.meta.main) {
  try {
    const options = environmentOptions();
    serveStdio(() =>
      createAgentToolsMcpServer({
        ownAgentId: options.agentId,
        messenger: lazyDelegatedMessenger(options),
      }),
    );
  } catch {
    console.error("agent tools MCP failed");
    process.exitCode = 1;
  }
}
