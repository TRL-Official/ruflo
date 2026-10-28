/**
 * http_fetch MCP tool — ADR-164 §5.1.8.
 *
 * The Operations business pod (templates/ops.json) references `http_fetch` as
 * an allowed MCP tool for the synthetic-endpoint availability bench (probe
 * 200/500 from a configured URL, escalate to #ops on 500-rate spikes).
 * Phase 3 left this as a TODO; Phase 4 ships the tool with secure-by-default
 * gating per the §5.1.8 contract: URL allowlist (no file://, ftp://, no
 * RFC-1918 / loopback unless explicitly enabled), header sanitization (no
 * auth pass-through unless explicitly enabled), hard timeout via
 * AbortController, response truncation, default User-Agent.
 *
 * Architectural constraints (load-bearing):
 *   - DEFAULT-REFUSES private addresses + loopback (CLAUDE_FLOW_HTTP_FETCH_ALLOW_PRIVATE=1 opt-in)
 *   - DEFAULT-REFUSES Authorization / Cookie / X-Auth-* (CLAUDE_FLOW_HTTP_FETCH_ALLOW_AUTH=1 opt-in)
 *   - hard timeout 30s default, 60s ceiling
 *   - response truncated to 256KB default, 1MB ceiling
 *   - redirects followed manually, max 10, every hop re-validated by the same allowlist
 *
 * @module @claude-flow/cli/mcp-tools/http-fetch
 */

import type { MCPTool } from './types.js';

const DEFAULT_TIMEOUT_MS = 30_000;
const MAX_TIMEOUT_MS = 60_000;
const DEFAULT_MAX_RESPONSE_BYTES = 256 * 1024; // 256 KB
const MAX_RESPONSE_BYTES = 1024 * 1024; // 1 MB
const MAX_REDIRECTS = 10;
const REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);
const DEFAULT_USER_AGENT = 'ruflo-http-fetch/1.0';

const FORBIDDEN_HEADER_PREFIXES = ['x-auth-', 'x-api-key'] as const;
const FORBIDDEN_HEADERS_EXACT = ['authorization', 'cookie', 'set-cookie', 'proxy-authorization'] as const;

export class HttpFetchValidationError extends Error {
  constructor(message: string, public readonly code: string) {
    super(message);
    this.name = 'HttpFetchValidationError';
  }
}

/**
 * Decide whether the URL is permitted under the default secure-by-default
 * allowlist. Block file://, ftp://, RFC-1918 private addresses, loopback,
 * link-local — unless CLAUDE_FLOW_HTTP_FETCH_ALLOW_PRIVATE=1 is set.
 */
export function validateUrl(rawUrl: string): URL {
  let parsed: URL;
  try {
    parsed = new URL(rawUrl);
  } catch {
    throw new HttpFetchValidationError(`invalid URL: ${rawUrl}`, 'INVALID_URL');
  }
  const proto = parsed.protocol.toLowerCase();
  if (proto !== 'http:' && proto !== 'https:') {
    throw new HttpFetchValidationError(
      `protocol ${parsed.protocol} not allowed (only http: and https:)`,
      'FORBIDDEN_PROTOCOL',
    );
  }
  const host = parsed.hostname.toLowerCase();
  const allowPrivate = process.env.CLAUDE_FLOW_HTTP_FETCH_ALLOW_PRIVATE === '1';
  if (!allowPrivate && isPrivateOrLoopback(host)) {
    throw new HttpFetchValidationError(
      `host ${host} is loopback/private/link-local; set CLAUDE_FLOW_HTTP_FETCH_ALLOW_PRIVATE=1 to override`,
      'PRIVATE_ADDRESS',
    );
  }
  return parsed;
}

function isPrivateOrLoopback(host: string): boolean {
  if (host === 'localhost' || host === 'localhost.localdomain') return true;

  // Node's URL parser keeps brackets in `.hostname` for every IPv6 literal
  // (e.g. "[::1]", "[fc00::1]", "[::ffff:7f00:1]"). Strip them once so every
  // check below operates on the bare address — without this, the fc00::/7
  // and fe80::/10 checks further down never match anything (they test
  // `startsWith('fc'/'fe80:')` against a string starting with "["), and an
  // IPv4-mapped-IPv6 literal (`::ffff:a.b.c.d`, which Node normalizes to a
  // canonical `::ffff:xxxx:yyyy` hex form) sails through both the IPv4 and
  // IPv6 branches untested — a live SSRF bypass (e.g. `::ffff:127.0.0.1` or
  // `::ffff:169.254.169.254` reaching loopback / cloud-metadata targets).
  const bare = host.startsWith('[') && host.endsWith(']') ? host.slice(1, -1) : host;

  if (bare === '::1') return true; // IPv6 loopback

  // IPv4-mapped IPv6 — decode back to dotted-decimal and recurse into the
  // IPv4 check below so it inherits every existing private/loopback/
  // link-local/CGNAT rule instead of needing its own copy.
  const mappedHex = bare.match(/^::ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/);
  if (mappedHex) {
    const hi = parseInt(mappedHex[1], 16);
    const lo = parseInt(mappedHex[2], 16);
    const decoded = `${(hi >> 8) & 0xff}.${hi & 0xff}.${(lo >> 8) & 0xff}.${lo & 0xff}`;
    return isPrivateOrLoopback(decoded);
  }
  const mappedDecimal = bare.match(/^::ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/);
  if (mappedDecimal) return isPrivateOrLoopback(mappedDecimal[1]);

  // IPv4 numeric checks
  const m = bare.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
  if (m) {
    const a = Number(m[1]);
    const b = Number(m[2]);
    if (a === 0) return true;          // 0.0.0.0/8
    if (a === 127) return true;        // loopback
    if (a === 10) return true;         // RFC1918
    if (a === 172 && b >= 16 && b <= 31) return true; // RFC1918
    if (a === 192 && b === 168) return true;          // RFC1918
    if (a === 169 && b === 254) return true;          // link-local
    if (a === 100 && b >= 64 && b <= 127) return true; // CGNAT
    return false;
  }
  // IPv6 special forms — be conservative and accept only public-looking
  // literals. Reject obvious private/local forms.
  if (bare.startsWith('fc') || bare.startsWith('fd')) return true;  // fc00::/7 ULA
  if (bare.startsWith('fe80:')) return true;                        // link-local
  // KNOWN LIMITATION (pre-existing, not introduced by the bracket-stripping /
  // IPv4-mapped-IPv6 fix above; flagged by an independent adversarial review):
  // `::` (IPv6 unspecified/"any", the analog of 0.0.0.0, which the IPv4 branch
  // above does block), IPv6 multicast (ff00::/8), and NAT64 (64:ff9b::/96) are
  // not covered. Left as a follow-up hardening item rather than scope-creeping
  // this patch.
  return false;
}

export function validateHeaders(headers: Record<string, string>): Record<string, string> {
  const allowAuth = process.env.CLAUDE_FLOW_HTTP_FETCH_ALLOW_AUTH === '1';
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    const lower = key.toLowerCase();
    if (!allowAuth) {
      if ((FORBIDDEN_HEADERS_EXACT as readonly string[]).includes(lower)) {
        throw new HttpFetchValidationError(
          `header "${key}" is not allowed without CLAUDE_FLOW_HTTP_FETCH_ALLOW_AUTH=1`,
          'FORBIDDEN_HEADER',
        );
      }
      if (FORBIDDEN_HEADER_PREFIXES.some((p) => lower.startsWith(p))) {
        throw new HttpFetchValidationError(
          `header "${key}" is not allowed without CLAUDE_FLOW_HTTP_FETCH_ALLOW_AUTH=1`,
          'FORBIDDEN_HEADER',
        );
      }
    }
    if (typeof value !== 'string') {
      throw new HttpFetchValidationError(
        `header "${key}" must be a string`,
        'INVALID_HEADER_VALUE',
      );
    }
    out[key] = value;
  }
  return out;
}

function clampNumber(raw: unknown, defaultValue: number, max: number): number {
  if (raw === undefined || raw === null) return defaultValue;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return defaultValue;
  return Math.min(Math.floor(n), max);
}

export interface HttpFetchResult {
  success: boolean;
  status: number;
  statusText: string;
  headers: Record<string, string>;
  body: string;
  bodyTruncated: boolean;
  bytesRead: number;
  durationMs: number;
  url: string;
  method: string;
  error?: string;
  errorCode?: string;
}

/**
 * Pure execution path so tests can call it without going through the MCP
 * dispatcher. Returns a result object (does not throw on validation failure
 * — it returns success: false with an errorCode).
 */
export async function httpFetchExecute(input: Record<string, unknown>): Promise<HttpFetchResult> {
  const startedAt = Date.now();
  const url = String(input.url ?? '');
  const method = String(input.method ?? 'GET').toUpperCase();
  if (!['GET', 'POST', 'HEAD'].includes(method)) {
    return {
      success: false,
      status: 0,
      statusText: '',
      headers: {},
      body: '',
      bodyTruncated: false,
      bytesRead: 0,
      durationMs: Date.now() - startedAt,
      url,
      method,
      error: `method ${method} not allowed (GET, POST, HEAD only)`,
      errorCode: 'INVALID_METHOD',
    };
  }

  let parsed: URL;
  try {
    parsed = validateUrl(url);
  } catch (e) {
    const err = e as HttpFetchValidationError;
    return {
      success: false,
      status: 0,
      statusText: '',
      headers: {},
      body: '',
      bodyTruncated: false,
      bytesRead: 0,
      durationMs: Date.now() - startedAt,
      url,
      method,
      error: err.message,
      errorCode: err.code ?? 'VALIDATION_ERROR',
    };
  }

  let headers: Record<string, string>;
  try {
    const raw = (input.headers ?? {}) as Record<string, string>;
    if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
      throw new HttpFetchValidationError('headers must be an object', 'INVALID_HEADERS');
    }
    headers = validateHeaders(raw);
  } catch (e) {
    const err = e as HttpFetchValidationError;
    return {
      success: false,
      status: 0,
      statusText: '',
      headers: {},
      body: '',
      bodyTruncated: false,
      bytesRead: 0,
      durationMs: Date.now() - startedAt,
      url,
      method,
      error: err.message,
      errorCode: err.code ?? 'VALIDATION_ERROR',
    };
  }

  // Default UA if not set
  const hasUA = Object.keys(headers).some((k) => k.toLowerCase() === 'user-agent');
  if (!hasUA) headers['User-Agent'] = DEFAULT_USER_AGENT;

  const timeoutMs = clampNumber(input.timeoutMs, DEFAULT_TIMEOUT_MS, MAX_TIMEOUT_MS);
  const maxResponseBytes = clampNumber(
    input.maxResponseBytes,
    DEFAULT_MAX_RESPONSE_BYTES,
    MAX_RESPONSE_BYTES,
  );
  const body = input.body !== undefined ? String(input.body) : undefined;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    let currentUrl = parsed;
    let currentMethod = method;
    let currentBody = body;
    let response: Response;
    let redirects = 0;

    // Do not let fetch follow redirects on its own: only the initial URL was
    // validated, so an automatic follow could reach loopback/RFC1918 or switch
    // protocols after passing the allowlist. Every hop is validated with the
    // same rules; sensitive headers are dropped when the origin changes.
    while (true) {
      response = await fetch(currentUrl.toString(), {
        method: currentMethod,
        headers,
        body: currentMethod === 'GET' || currentMethod === 'HEAD' ? undefined : currentBody,
        signal: controller.signal,
        redirect: 'manual',
      });

      if (!REDIRECT_STATUSES.has(response.status)) break;

      const location = response.headers.get('location');
      if (!location) break;

      if (redirects++ >= MAX_REDIRECTS) {
        try { await response.body?.cancel(); } catch { /* ignore */ }
        return {
          success: false,
          status: response.status,
          statusText: response.statusText,
          headers: {},
          body: '',
          bodyTruncated: false,
          bytesRead: 0,
          durationMs: Date.now() - startedAt,
          url: currentUrl.toString(),
          method,
          error: `too many redirects (max ${MAX_REDIRECTS})`,
          errorCode: 'REDIRECT_LIMIT',
        };
      }

      let nextUrl: URL;
      try {
        nextUrl = validateUrl(new URL(location, currentUrl).toString());
      } catch (e) {
        try { await response.body?.cancel(); } catch { /* ignore */ }
        const err = e as HttpFetchValidationError;
        return {
          success: false,
          status: response.status,
          statusText: response.statusText,
          headers: {},
          body: '',
          bodyTruncated: false,
          bytesRead: 0,
          durationMs: Date.now() - startedAt,
          url: currentUrl.toString(),
          method,
          error: `redirect blocked: ${err.message}`,
          errorCode: err.code ?? 'REDIRECT_BLOCKED',
        };
      }

      // The redirect response body is not returned; release the connection
      // before issuing the next request. A body-less 3xx is also fine.
      try { await response.body?.cancel(); } catch { /* ignore */ }

      if (nextUrl.origin !== currentUrl.origin) {
        for (const key of Object.keys(headers)) {
          const lower = key.toLowerCase();
          if (lower === 'authorization' || lower === 'cookie' || lower === 'proxy-authorization') {
            delete headers[key];
          }
        }
      }

      // Match fetch's redirect method rules: 303 becomes GET except for
      // GET/HEAD, while 301/302 only rewrite POST to GET. 307/308 preserve
      // both method and body.
      if (
        (response.status === 303 && currentMethod !== 'GET' && currentMethod !== 'HEAD') ||
        ((response.status === 301 || response.status === 302) && currentMethod === 'POST')
      ) {
        currentMethod = 'GET';
        currentBody = undefined;
      }

      currentUrl = nextUrl;
    }

    // Drain the body with byte budget. Use array-buffer first to bound size
    // deterministically before decoding to UTF-8.
    const reader = response.body?.getReader();
    let bytesRead = 0;
    let bodyTruncated = false;
    const chunks: Uint8Array[] = [];
    if (reader) {
      // eslint-disable-next-line no-constant-condition
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        if (value) {
          if (bytesRead + value.byteLength > maxResponseBytes) {
            const remaining = Math.max(0, maxResponseBytes - bytesRead);
            if (remaining > 0) chunks.push(value.slice(0, remaining));
            bytesRead += remaining;
            bodyTruncated = true;
            try { await reader.cancel('body-truncated'); } catch { /* ignore */ }
            break;
          }
          chunks.push(value);
          bytesRead += value.byteLength;
        }
      }
    }
    const combined = new Uint8Array(bytesRead);
    let offset = 0;
    for (const c of chunks) {
      combined.set(c, offset);
      offset += c.byteLength;
    }
    const decoder = new TextDecoder('utf-8', { fatal: false });
    const bodyText = decoder.decode(combined);

    const outHeaders: Record<string, string> = {};
    response.headers.forEach((v, k) => { outHeaders[k] = v; });

    return {
      success: true,
      status: response.status,
      statusText: response.statusText,
      headers: outHeaders,
      body: bodyText,
      bodyTruncated,
      bytesRead,
      durationMs: Date.now() - startedAt,
      url: currentUrl.toString(),
      method,
    };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    const aborted = msg.includes('aborted') || (e as { name?: string } | undefined)?.name === 'AbortError';
    return {
      success: false,
      status: 0,
      statusText: '',
      headers: {},
      body: '',
      bodyTruncated: false,
      bytesRead: 0,
      durationMs: Date.now() - startedAt,
      url: parsed.toString(),
      method,
      error: aborted ? `request aborted after ${timeoutMs}ms timeout` : msg,
      errorCode: aborted ? 'TIMEOUT' : 'FETCH_ERROR',
    };
  } finally {
    clearTimeout(timer);
  }
}

export const httpFetchTools: MCPTool[] = [
  {
    name: 'http_fetch',
    description: 'ADR-164 §5.1.8 — HTTP probe primitive for business-pod ops benches (synthetic 200/500 endpoint checks, third-party status pages). Default-secure: blocks file://, ftp://, RFC-1918 / loopback / link-local hosts unless CLAUDE_FLOW_HTTP_FETCH_ALLOW_PRIVATE=1, and rejects Authorization / Cookie / X-Auth-* headers unless CLAUDE_FLOW_HTTP_FETCH_ALLOW_AUTH=1. Hard 30s timeout (60s ceiling), response truncated to 256 KB (1 MB ceiling), default User-Agent ruflo-http-fetch/1.0. Use when a pod or smoke contract needs a guarded HTTP probe — calling Node fetch() directly is wrong because it skips the URL allowlist and header sanitization that ADR-164 mandates for autopilot mode. Pair with the ops-pod bench in plugins/ruflo-business-pods/templates/ops.json (the §4.4 synthetic-endpoint test).',
    category: 'business-pods',
    tags: ['business-pods', 'http', 'fetch', 'adr-164', 'security', 'ops-pod'],
    inputSchema: {
      type: 'object',
      properties: {
        url: {
          type: 'string',
          description: 'Absolute http:// or https:// URL. file://, ftp://, RFC-1918 / loopback / link-local blocked by default.',
        },
        method: {
          type: 'string',
          enum: ['GET', 'POST', 'HEAD'],
          description: 'HTTP method. Defaults to GET.',
        },
        timeoutMs: {
          type: 'number',
          description: 'Hard timeout in milliseconds. Default 30000, max 60000.',
        },
        maxResponseBytes: {
          type: 'number',
          description: 'Max body bytes to read before truncation. Default 262144 (256 KB), max 1048576 (1 MB).',
        },
        headers: {
          type: 'object',
          description: 'Extra request headers. Authorization / Cookie / X-Auth-* blocked unless CLAUDE_FLOW_HTTP_FETCH_ALLOW_AUTH=1.',
        },
        body: {
          type: 'string',
          description: 'Request body for POST. Ignored for GET / HEAD.',
        },
      },
      required: ['url'],
    },
    handler: async (input) => {
      return await httpFetchExecute(input);
    },
  },
];
