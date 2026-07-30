import type { SessionAdapter, SessionRequest, SessionResult } from "./session-adapter";

export type MockSessionHandler = (
  request: SessionRequest,
) => SessionResult | Promise<SessionResult>;

/** In-memory adapter for protocol and routing tests. It never writes message content to logs. */
export class MockSessionAdapter implements SessionAdapter {
  readonly requests: SessionRequest[] = [];

  constructor(private readonly handler: MockSessionHandler) {}

  async handle(request: SessionRequest): Promise<SessionResult> {
    this.requests.push(request);
    return this.handler(request);
  }
}
