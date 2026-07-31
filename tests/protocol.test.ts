import { describe, expect, test } from "bun:test";
import {
  DEFAULT_REQUEST_TIMEOUT_MS,
  MAX_REQUEST_TIMEOUT_MS,
  normalizeTimeoutMs,
  parseClientMessage,
  parseServerMessage,
} from "../src/protocol";

describe("parseClientMessage", () => {
  test("accepts a neutral registration", () => {
    expect(
      parseClientMessage(
        JSON.stringify({
          type: "register",
          protocolVersion: 1,
          agent: { agentId: "local:reviewer", side: "claude" },
        }),
      ),
    ).toEqual({
      type: "register",
      protocolVersion: 1,
      agent: { agentId: "local:reviewer", side: "claude" },
    });
  });

  test("accepts public activity metadata but rejects client-supplied status", () => {
    expect(
      parseClientMessage(
        JSON.stringify({
          type: "register",
          protocolVersion: 1,
          agent: {
            agentId: "local:reviewer",
            side: "claude",
            activity: "reviewing tests",
          },
        }),
      ),
    ).toMatchObject({
      agent: { activity: "reviewing tests" },
    });
    expect(
      parseClientMessage(
        JSON.stringify({
          type: "register",
          protocolVersion: 1,
          agent: { agentId: "local:reviewer", side: "claude", status: "busy" },
        }),
      ),
    ).toBeNull();
    expect(
      parseClientMessage(
        JSON.stringify({
          type: "register",
          protocolVersion: 1,
          agent: { agentId: "local:reviewer", side: "claude", activity: "unsafe\nvalue" },
        }),
      ),
    ).toBeNull();
  });

  test("accepts an agent-scoped delegate registration", () => {
    const delegationToken = "d".repeat(64);
    expect(
      parseClientMessage(
        JSON.stringify({
          type: "register_delegate",
          protocolVersion: 1,
          agentId: "local:worker-a",
          delegationToken,
        }),
      ),
    ).toEqual({
      type: "register_delegate",
      protocolVersion: 1,
      agentId: "local:worker-a",
      delegationToken,
    });
    expect(
      parseClientMessage(
        JSON.stringify({
          type: "register_delegate",
          protocolVersion: 1,
          agentId: "local:worker-a",
          delegationToken: "too-short",
        }),
      ),
    ).toBeNull();
  });

  test("rejects invalid identifiers", () => {
    expect(
      parseClientMessage(
        JSON.stringify({
          type: "register",
          protocolVersion: 1,
          agent: { agentId: "../../secret", side: "generic" },
        }),
      ),
    ).toBeNull();
  });

  test("rejects empty request content", () => {
    expect(
      parseClientMessage(
        JSON.stringify({ type: "send", requestId: "request-1", to: "local:reviewer", content: "" }),
      ),
    ).toBeNull();
  });
});

describe("parseServerMessage", () => {
  test("accepts a correlated result and rejects malformed router data", () => {
    expect(
      parseServerMessage(
        JSON.stringify({
          type: "result",
          requestId: "request-1",
          from: "local:worker-a",
          ok: true,
          content: "done",
        }),
      ),
    ).toEqual({
      type: "result",
      requestId: "request-1",
      from: "local:worker-a",
      ok: true,
      content: "done",
    });
    expect(parseServerMessage(JSON.stringify({ type: "result", requestId: "request-1" }))).toBeNull();
    expect(parseServerMessage(new Uint8Array())).toBeNull();
    expect(parseServerMessage("not-json")).toBeNull();
  });

  test("accepts router-derived agent presence", () => {
    expect(
      parseServerMessage(
        JSON.stringify({
          type: "agents",
          requestId: "list-1",
          agents: [
            {
              agentId: "local:reviewer",
              side: "claude",
              activity: "reviewing tests",
              status: "busy",
            },
          ],
        }),
      ),
    ).toMatchObject({
      agents: [{ activity: "reviewing tests", status: "busy" }],
    });
  });
});

describe("normalizeTimeoutMs", () => {
  test("uses a safe default", () => {
    expect(normalizeTimeoutMs(undefined)).toBe(DEFAULT_REQUEST_TIMEOUT_MS);
  });

  test("clamps values to the supported range", () => {
    expect(normalizeTimeoutMs(0)).toBe(1);
    expect(normalizeTimeoutMs(Number.MAX_SAFE_INTEGER)).toBe(MAX_REQUEST_TIMEOUT_MS);
  });
});
