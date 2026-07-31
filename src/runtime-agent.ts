import {
  isAgentActivity,
  isAgentId,
  type AgentDescriptor,
  type AgentSide,
} from "./protocol";

export const GATEWAY_AGENT_ACTIVITY_ENV = "GATEWAY_AGENT_ACTIVITY";

export function runtimeAgent(
  defaultAgentId: string,
  side: AgentSide,
  environment: Record<string, string | undefined> = process.env,
): AgentDescriptor {
  const agentId = environment.GATEWAY_AGENT_ID?.trim() || defaultAgentId;
  if (!isAgentId(agentId)) throw new Error("Invalid GATEWAY_AGENT_ID");

  const configuredActivity = environment[GATEWAY_AGENT_ACTIVITY_ENV];
  if (configuredActivity === undefined || configuredActivity === "") return { agentId, side };
  if (!isAgentActivity(configuredActivity)) {
    throw new Error("Invalid GATEWAY_AGENT_ACTIVITY");
  }
  return { agentId, side, activity: configuredActivity };
}
