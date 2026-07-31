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
    expect(registry.unregister(socket)?.descriptor).toEqual({ ...descriptor, status: "idle" });
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
    registry.register(
      { agentId: "local:worker-a", side: "generic", activity: "reviewing tests" },
      connection(),
    );

    expect(registry.list().map(({ agentId }) => agentId)).toEqual(["local:worker-a", "local:worker-b"]);
    expect(registry.list()[0]).toEqual({
      agentId: "local:worker-a",
      side: "generic",
      activity: "reviewing tests",
      status: "idle",
    });
    registry.setStatus("local:worker-a", "busy");
    expect(registry.list()[0]?.status).toBe("busy");
  });

  test("authorizes outbound-only delegates without listing them as agents", () => {
    const registry = new AgentRegistry();
    const primary = connection();
    const delegate = connection();
    const rejected = connection();
    const descriptor = { agentId: "local:worker-a", side: "codex" as const };
    const delegationToken = "d".repeat(64);

    expect(registry.register(descriptor, primary, delegationToken)).toBe(true);
    expect(registry.registerDelegate(descriptor.agentId, "x".repeat(64), rejected)).toBe(false);
    expect(registry.registerDelegate(descriptor.agentId, delegationToken, delegate)).toBe(true);
    expect(registry.getByConnection(delegate)).toMatchObject({ descriptor, role: "delegate" });
    expect(registry.list()).toEqual([{ ...descriptor, status: "idle" }]);

    expect(registry.unregister(delegate)).toMatchObject({ descriptor, role: "delegate", delegates: [] });
    expect(registry.get(descriptor.agentId)?.connection).toBe(primary);
  });

  test("revokes every delegate when the owning agent disconnects", () => {
    const registry = new AgentRegistry();
    const primary = connection();
    const delegateA = connection();
    const delegateB = connection();
    const descriptor = { agentId: "local:worker-a", side: "codex" as const };
    const delegationToken = "d".repeat(64);

    registry.register(descriptor, primary, delegationToken);
    registry.registerDelegate(descriptor.agentId, delegationToken, delegateA);
    registry.registerDelegate(descriptor.agentId, delegationToken, delegateB);

    expect(registry.unregister(primary)?.delegates).toEqual([delegateA, delegateB]);
    expect(registry.get(descriptor.agentId)).toBeUndefined();
    expect(registry.getByConnection(delegateA)).toBeUndefined();
    expect(registry.getByConnection(delegateB)).toBeUndefined();
  });
});
