import { describe, expect, test } from "bun:test";
import { AgentRegistry, type AgentConnection } from "../src/registry";

function connection(): AgentConnection {
  return { send() {} };
}

describe("AgentRegistry", () => {
  test("registers, resolves, and unregisters one active agent", () => {
    const registry = new AgentRegistry();
    const socket = connection();
    const descriptor = { agentId: "local:reviewer", side: "claude" as const };

    expect(registry.register(descriptor, socket)).toBe(true);
    expect(registry.get(descriptor.agentId)?.connection).toBe(socket);
    expect(registry.unregister(socket)?.descriptor).toEqual(descriptor);
    expect(registry.list()).toEqual([]);
  });

  test("rejects duplicate live identifiers", () => {
    const registry = new AgentRegistry();
    const descriptor = { agentId: "local:tester", side: "generic" as const };

    expect(registry.register(descriptor, connection())).toBe(true);
    expect(registry.register(descriptor, connection())).toBe(false);
  });

  test("lists agents in stable identifier order", () => {
    const registry = new AgentRegistry();
    registry.register({ agentId: "local:worker-b", side: "generic" }, connection());
    registry.register({ agentId: "local:worker-a", side: "generic" }, connection());

    expect(registry.list().map(({ agentId }) => agentId)).toEqual(["local:worker-a", "local:worker-b"]);
  });
});

