import { describe, expect, test } from "bun:test";
import {
  DEFAULT_REQUEST_TIMEOUT_MS,
  MAX_REQUEST_TIMEOUT_MS,
  normalizeTimeoutMs,
  parseClientMessage,
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

describe("normalizeTimeoutMs", () => {
  test("uses a safe default", () => {
    expect(normalizeTimeoutMs(undefined)).toBe(DEFAULT_REQUEST_TIMEOUT_MS);
  });

  test("clamps values to the supported range", () => {
    expect(normalizeTimeoutMs(0)).toBe(1);
    expect(normalizeTimeoutMs(Number.MAX_SAFE_INTEGER)).toBe(MAX_REQUEST_TIMEOUT_MS);
  });
});

