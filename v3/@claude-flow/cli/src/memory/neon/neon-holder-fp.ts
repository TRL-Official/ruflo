/**
 * Which database a connection holder serves, so a client never uses the wrong one (ported from digital-twin's
 * neon-holder-fp.cjs, 2026-10-18). The holder writes a fingerprint of its URL beside its socket (<sock>.fp: 16
 * hex chars of sha256, never the URL itself); a client uses the holder only when it matches its own URL, and
 * otherwise connects directly. Before, a process asking for another database was served by the holder's.
 */
import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';

export const fp = (url: string): string => createHash('sha256').update(String(url)).digest('hex').slice(0, 16);

/** The holder socket to use for `url`, or null when there is none for this database. */
export function holderFor(url: string | undefined, sock: string | undefined = process.env.NEON_HOLDER_SOCK): string | null {
  if (!url || !sock) return null;
  try { return existsSync(sock) && readFileSync(`${sock}.fp`, 'utf8').trim() === fp(url) ? sock : null; }
  catch { return null; }
}
