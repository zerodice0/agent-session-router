export interface SessionRequest {
  requestId: string;
  from: string;
  content: string;
  timeoutMs: number;
}

export type SessionResult =
  | { ok: true; content: string }
  | { ok: false; error: string };

/**
 * Provider-specific integration boundary. Implementations may talk to a live
 * session, but must return only the result for the supplied request.
 */
export interface SessionAdapter {
  handle(request: SessionRequest): Promise<SessionResult>;
}
