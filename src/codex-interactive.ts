import { createInterface } from "node:readline/promises";
import { CodexAppServerAdapter } from "./codex-app-server-adapter";
import { CodexStdioTransport } from "./codex-app-server-transport";
import { CodexInteractiveConsole } from "./codex-interactive-console";
import { GatewayClient } from "./gateway-client";
import {
  createAgentToolsEnvironment,
  createCodexAppServerCommand,
  createDelegationToken,
} from "./agent-tools-config";
import { normalizeTimeoutMs } from "./protocol";
import { runtimeAgent } from "./runtime-agent";

const routerUrl = process.env.ROUTER_URL ?? "ws://127.0.0.1:8787/ws";
const agent = runtimeAgent("local:codex", "codex");
const agentId = agent.agentId;
const providerCwd = process.env.CODEX_CWD;
const resumeThreadId = process.env.CODEX_THREAD_ID;
const configuredTurnTimeoutMs = readOptionalNumber(process.env.CODEX_TURN_TIMEOUT_MS);

const delegationToken = createDelegationToken();
const transport = CodexStdioTransport.spawn({
  command: createCodexAppServerCommand(),
  ...(providerCwd === undefined ? {} : { cwd: providerCwd }),
  env: createAgentToolsEnvironment({ routerUrl, agentId, delegationToken }),
});
let adapter: CodexAppServerAdapter | null = null;
let gateway: GatewayClient | null = null;
const terminal = createInterface({ input: process.stdin, output: process.stdout });

try {
  adapter = await CodexAppServerAdapter.connect({
    transport,
    thread:
      resumeThreadId === undefined
        ? { mode: "start" }
        : { mode: "resume", threadId: resumeThreadId },
  });
  gateway = new GatewayClient({
    routerUrl,
    agent,
    adapter,
    token: process.env.ROUTER_TOKEN,
    delegationToken,
  });
  await gateway.connect();

  const interactive = new CodexInteractiveConsole({
    agentId,
    adapter,
    messenger: gateway,
    ...(configuredTurnTimeoutMs === undefined
      ? {}
      : { turnTimeoutMs: normalizeTimeoutMs(configuredTurnTimeoutMs) }),
    write: (text) => process.stdout.write(text),
  });

  process.stdout.write(`Interactive Codex connected as ${agentId}. Type /help for commands.\n`);
  while (true) {
    let line: string;
    try {
      line = await terminal.question(`codex(${agentId})> `);
    } catch {
      break;
    }
    if ((await interactive.execute(line)) === "exit") break;
  }
} catch {
  console.error("Interactive Codex gateway failed");
  process.exitCode = 1;
} finally {
  terminal.close();
  gateway?.disconnect();
  adapter?.close();
  transport.close();
}

function readOptionalNumber(value: string | undefined): number | undefined {
  if (value === undefined || value.trim() === "") return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}
