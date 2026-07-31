import { describe, expect, test } from "bun:test";
import { runtimeAgent } from "../src/runtime-agent";

describe("runtimeAgent", () => {
  test("adds explicit public activity without deriving provider content", () => {
    expect(
      runtimeAgent("local:default", "claude", {
        GATEWAY_AGENT_ID: "local:reviewer",
        GATEWAY_AGENT_ACTIVITY: "reviewing tests",
      }),
    ).toEqual({
      agentId: "local:reviewer",
      side: "claude",
      activity: "reviewing tests",
    });
  });

  test("keeps activity optional and rejects unsafe metadata", () => {
    expect(runtimeAgent("local:default", "codex", {})).toEqual({
      agentId: "local:default",
      side: "codex",
    });
    expect(() =>
      runtimeAgent("local:default", "codex", {
        GATEWAY_AGENT_ACTIVITY: "unsafe\nvalue",
      }),
    ).toThrow("Invalid GATEWAY_AGENT_ACTIVITY");
  });
});
