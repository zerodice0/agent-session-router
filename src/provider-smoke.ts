import { GatewayClient } from "./gateway-client";
import { MockSessionAdapter } from "./mock-session-adapter";
import { isAgentId, normalizeTimeoutMs } from "./protocol";

const routerUrl = process.env.ROUTER_URL ?? "ws://127.0.0.1:8787/ws";
const target = process.env.SMOKE_TARGET ?? "local:codex";
const configuredTimeoutMs = Number(process.env.SMOKE_TIMEOUT_MS ?? 120_000);
const timeoutMs = normalizeTimeoutMs(configuredTimeoutMs);
if (!isAgentId(target)) throw new Error("Invalid SMOKE_TARGET");

const coordinator = new GatewayClient({
  routerUrl,
  agent: { agentId: `smoke:coordinator-${crypto.randomUUID()}`, side: "generic" },
  adapter: new MockSessionAdapter(() => ({ ok: false, error: "session_busy" })),
  token: process.env.ROUTER_TOKEN,
});

class ProviderSmokeError extends Error {
  constructor(readonly code: string) {
    super(code);
  }
}

try {
  await coordinator.connect();
  const result = await coordinator.send(target, "Return a brief readiness acknowledgement.", {
    timeoutMs,
  });
  if (!result.ok || result.content.trim().length === 0) {
    throw new ProviderSmokeError(result.ok ? "empty_response" : result.error);
  }
  console.info("provider smoke test passed");
} catch (error) {
  const code = error instanceof ProviderSmokeError ? error.code : "gateway_error";
  console.error(`provider smoke test failed: ${code}`);
  process.exitCode = 1;
} finally {
  coordinator.disconnect();
}
