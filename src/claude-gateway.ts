import { createDelegationToken } from "./agent-tools-config";
import { ClaudeAgentSdkAdapter } from "./claude-agent-sdk-adapter";
import {
  createClaudeAgentSdkConfiguration,
  type ClaudeAgentSdkConfiguration,
} from "./claude-agent-sdk-config";
import { GatewayClient } from "./gateway-client";
import { isAgentId } from "./protocol";

const routerUrl = process.env.ROUTER_URL ?? "ws://127.0.0.1:8787/ws";
const agentId = process.env.GATEWAY_AGENT_ID ?? "local:claude";
const providerCwd = process.env.CLAUDE_CWD;
const resumeSessionId = process.env.CLAUDE_SESSION_ID;
const executablePath = process.env.CLAUDE_CODE_EXECUTABLE;

if (!isAgentId(agentId)) throw new Error("Invalid GATEWAY_AGENT_ID");

const delegationToken = createDelegationToken();
let adapter: ClaudeAgentSdkAdapter | null = null;
let gateway: GatewayClient | null = null;
let sdkConfiguration: ClaudeAgentSdkConfiguration | null = null;

try {
  sdkConfiguration = createClaudeAgentSdkConfiguration({
    routerUrl,
    agentId,
    delegationToken,
    ...(providerCwd === undefined ? {} : { cwd: providerCwd }),
    ...(executablePath === undefined ? {} : { executablePath }),
  });
  adapter = await ClaudeAgentSdkAdapter.connect({
    baseOptions: sdkConfiguration.options,
    ...(resumeSessionId === undefined ? {} : { resumeSessionId }),
  });
  gateway = new GatewayClient({
    routerUrl,
    agent: { agentId, side: "claude" },
    adapter,
    token: process.env.ROUTER_TOKEN,
    delegationToken,
  });
  await gateway.connect();
  await waitForShutdown();
} catch {
  console.error("Claude gateway failed");
  process.exitCode = 1;
} finally {
  gateway?.disconnect();
  adapter?.close();
  sdkConfiguration?.close();
}

function waitForShutdown(): Promise<void> {
  return new Promise((resolve) => {
    const finish = () => resolve();
    process.once("SIGINT", finish);
    process.once("SIGTERM", finish);
  });
}
