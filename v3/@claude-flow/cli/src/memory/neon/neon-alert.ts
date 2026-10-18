/**
 * Remember that Neon failed, so the next prompt can say so (ported from digital-twin's neon-alert.cjs, 2026-10-18).
 * Every neon-direct call that cannot reach Neon throws (nothing is written locally instead), but hooks run with
 * their errors discarded, so record() appends one line per outage to <git dir>/.neon-alert.json (connection strings
 * removed). Never throws. Env: NEON_ALERT_FILE, REPO_ROOT / DIGITAL_TWIN_ROOT (else the checkout around the cwd).
 */
import * as fs from 'node:fs';
import * as path from 'node:path';

/* eslint-disable @typescript-eslint/no-explicit-any */
function gitDir(start: string): string | null {
  let d = start;
  for (let i = 0; i < 40; i++) {
    const g = path.join(d, '.git');
    try {
      const st = fs.statSync(g);
      if (st.isDirectory()) return g;
      const m = fs.readFileSync(g, 'utf8').match(/^gitdir:\s*(.+)$/m); // a worktree
      if (m) return path.resolve(d, m[1].trim());
    } catch { /* not here */ }
    const up = path.dirname(d);
    if (up === d) return null;
    d = up;
  }
  return null;
}
export function file(): string | null {
  if (process.env.NEON_ALERT_FILE) return process.env.NEON_ALERT_FILE;
  const g = gitDir(process.env.REPO_ROOT || process.env.DIGITAL_TWIN_ROOT || process.cwd());
  return g ? path.join(g, '.neon-alert.json') : null;
}
// connection strings and passwords never reach the note
export const scrub = (s: unknown): string => String(s).replace(/postgres(?:ql)?:\/\/[^\s'"`]+/gi, '<connection-string>')
  .replace(/(password|passwd|pwd)\s*[=:]\s*[^\s,;'"]+/gi, '$1=<hidden>').replace(/\s+/g, ' ').slice(0, 300);

// An outage, not an ordinary SQL error: no SQLSTATE at all (transport), or a class that means the
// server or the connection is the problem — 08 connection, 28 auth, 3D no database, 53 resources,
// 57 shut down / cancelled, 58 system, XX internal.
const OUTAGE_TEXT = /did not answer|timed? ?out|fetch failed|ECONN|ENOTFOUND|EAI_AGAIN|ETIMEDOUT|EHOSTUNREACH|socket|connect|password authentication|authentication failed|too many connections|compute|endpoint|quota|HTTP status [45]\d\d|server error|Unexpected token|not valid JSON|Failed to parse URL/i;
export function isOutage(err: any): boolean {
  // pgCode: the SQLSTATE a facade attached (undefined there = a transport failure); else the error's own code
  const code = err && (Object.prototype.hasOwnProperty.call(err, 'pgCode') ? err.pgCode : err.code);
  if (typeof code === 'string' && /^[0-9A-Z]{5}$/.test(code)) return /^(08|28|3D|53|57|58|XX)/.test(code);
  return OUTAGE_TEXT.test(`${err && err.message || err} ${code || ''}`) || !code;
}
// One line per failure, appended (2026-10-18): concurrent writers — a hook, the holder draining the
// queue, the sentinel — each append a short line; read-modify-write of one JSON object lost entries
// when two failed at the same moment (seen in neon-failure-test F3). read() adds the lines up.
const CAP = 1 << 20; // past 1 MB of notes (thousands of failures) the count stops growing; the alert stands
export function read(): any {
  let text: string; try { text = fs.readFileSync(file() as string, 'utf8'); } catch { return null; }
  const a: any = { count: 0, sources: {} };
  for (const line of text.split('\n')) {
    if (!line) continue;
    let x: any; try { x = JSON.parse(line); } catch { continue; } // a torn last line is skipped
    a.first ??= x.t; a.last = x.t; a.count++; a.error = x.error;
    a.sources[x.source] = (a.sources[x.source] || 0) + 1;
  }
  return a.count ? a : null;
}
export function record(source: string, err: any): void {
  try {
    const f = file(); if (!f || !isOutage(err)) return;
    try { if (fs.statSync(f).size > CAP) return; } catch { /* first failure */ }
    const line = JSON.stringify({ t: new Date().toISOString(), source, error: scrub(err && err.message || err) }) + '\n';
    fs.appendFileSync(f, line, { mode: 0o600 });
  } catch { /* never throw from here */ }
}
export function clear(): any {
  const f = file(); if (!f) return null;
  // rename first: a failure appended after this starts a new note instead of being deleted unread
  const moved = `${f}.${process.pid}.cleared`;
  try { fs.renameSync(f, moved); } catch { return null; }
  let a = null; try { const prev = process.env.NEON_ALERT_FILE; process.env.NEON_ALERT_FILE = moved; a = read(); if (prev == null) delete process.env.NEON_ALERT_FILE; else process.env.NEON_ALERT_FILE = prev; } catch { /* unreadable */ }
  try { fs.unlinkSync(moved); } catch { /* gone */ }
  return a;
}
