import { NETWORK_FULFILLMENT_API_BASE } from "./config";

/** RFC 7807 problem+json body every error response from network-fulfillment
 *  returns (see apis/openapi.yaml's Problem schema). */
export interface ProblemDetails {
  type: string;
  title: string;
  status: number;
  detail: string;
  instance?: string;
}

export class ApiError extends Error {
  problem: ProblemDetails | null;
  status: number;

  constructor(status: number, problem: ProblemDetails | null, fallbackMessage: string) {
    super(problem?.detail || problem?.title || fallbackMessage);
    this.status = status;
    this.problem = problem;
  }
}

/**
 * GET-only client. network-fulfillment's REST surface is deliberately
 * read-only (apis/openapi.yaml's own description: demand enters this
 * context ONLY by polling the external network, never via HTTP intake),
 * so unlike every CRUD sibling remote this file has no apiPost/apiPut/
 * apiDelete -- there is nothing here to mutate.
 */
export async function apiGet<TResponse>(path: string): Promise<TResponse> {
  const res = await fetch(`${NETWORK_FULFILLMENT_API_BASE}${path}`);
  if (!res.ok) {
    let problem: ProblemDetails | null = null;
    try {
      problem = (await res.json()) as ProblemDetails;
    } catch {
      // non-JSON error body -- fall through with problem = null
    }
    throw new ApiError(res.status, problem, `${res.status} ${res.statusText}`);
  }
  return (await res.json()) as TResponse;
}
