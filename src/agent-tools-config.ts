import { fileURLToPath } from "node:url";

export const AGENT_ROUTER_URL_ENV = "AGENT_ROUTER_URL";
export const AGENT_ROUTER_AGENT_ID_ENV = "AGENT_ROUTER_AGENT_ID";
export const AGENT_ROUTER_DELEGATION_TOKEN_ENV = "AGENT_ROUTER_DELEGATION_TOKEN";

export const AGENT_TOOLS_MCP_SERVER_ID = "agent_session_router";
const MCP_ENV_VARS = [
  AGENT_ROUTER_URL_ENV,
  AGENT_ROUTER_AGENT_ID_ENV,
  AGENT_ROUTER_DELEGATION_TOKEN_ENV,
];

export interface AgentToolsRuntimeConfig {
  routerUrl: string;
  agentId: string;
  delegationToken: string;
}

export function createDelegationToken(): string {
  return `${crypto.randomUUID().replaceAll("-", "")}${crypto
    .randomUUID()
    .replaceAll("-", "")}`;
}

export function createCodexAppServerCommand(): string[] {
  const serverScript = fileURLToPath(new URL("./agent-tools-mcp.ts", import.meta.url));
  const configPrefix = `mcp_servers.${AGENT_TOOLS_MCP_SERVER_ID}`;
  return [
    "codex",
    "app-server",
    "-c",
    `${configPrefix}.command=${JSON.stringify(process.execPath)}`,
    "-c",
    `${configPrefix}.args=${JSON.stringify([serverScript])}`,
    "-c",
    `${configPrefix}.env_vars=${JSON.stringify(MCP_ENV_VARS)}`,
    "-c",
    `${configPrefix}.required=true`,
    "-c",
    `${configPrefix}.enabled_tools=["agent_list","agent_send"]`,
    "-c",
    `${configPrefix}.default_tools_approval_mode="approve"`,
    "-c",
    `${configPrefix}.tool_timeout_sec=600`,
    "--listen",
    "stdio://",
  ];
}

export function createAgentToolsEnvironment(
  config: AgentToolsRuntimeConfig,
): Record<string, string> {
  return {
    [AGENT_ROUTER_URL_ENV]: config.routerUrl,
    [AGENT_ROUTER_AGENT_ID_ENV]: config.agentId,
    [AGENT_ROUTER_DELEGATION_TOKEN_ENV]: config.delegationToken,
  };
}
