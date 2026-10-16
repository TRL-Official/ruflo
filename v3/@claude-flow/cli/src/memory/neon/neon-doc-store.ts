/**
 * Persist ruflo state DIRECTLY in Neon (Postgres + pgvector), one row per record, from synchronous code.
 *
 * Why rows, not one JSON blob: a blob is last-writer-wins, exactly like the file it replaces — two
 * hook processes (or two Claude Code sessions) that each add a record and save lose one of them.
 * Here writes are per record, and save() is a three-way diff against what THIS process loaded:
 *   - a record that is new or changed here          → upserted
 *   - a record this process loaded and then removed → deleted
 *   - a record some other process wrote meanwhile   → untouched (this process never saw it)
 *
 * Why synchronous: LocalReasoningBank's API (store/findSimilar/get/flushToDisk) is synchronous and
 * called from many sites. Each call blocks in Atomics.wait while a worker thread does the HTTP round
 * trip (neon-doc-worker) — measured ~119 ms, the same as an async query.
 *
 * `lazy` collections are never loaded whole (at real scale a whole store passes Neon's 64 MB HTTP
 * response limit). Their rows carry a pgvector column, and similarity search runs in Neon.
 * Writes are chunked at ~8 MB per transaction for the same limit.
 *
 * It never falls back to a file: without Neon it throws, and the caller decides.
 *
 * Env: RVN_URL (else PGSYNC_URL), RVN_SCHEMA (default rvdirect), PGSYNC_NEON_DRIVER (default
 * '@neondatabase/serverless'), RVN_TIMEOUT_MS (20000), RVN_DIM (384), RVN_WINDOW (0), RVN_CHUNK_BYTES.
 * The same table layout as the ruvector fork's bin/neon-doc-store.cjs, so the two can share a schema.
 */
import { Worker, MessageChannel, receiveMessageOnPort } from 'node:worker_threads';
import { createHash } from 'node:crypto';

/* eslint-disable @typescript-eslint/no-explicit-any */
type Row = any;
interface Stmt { text: string; params?: unknown[] }
type Doc = Record<string, any>;

function makeCaller(): (stmts: Stmt[]) => Row[][] {
  const url = process.env.RVN_URL || process.env.PGSYNC_URL;
  if (!url) throw new Error('neon-doc-store: RVN_URL / PGSYNC_URL is not set — refusing to fall back to a file');
  const driver = process.env.PGSYNC_NEON_DRIVER || '@neondatabase/serverless';
  const timeout = Number(process.env.RVN_TIMEOUT_MS) || 20000;
  // The worker inherits the host's execArgv; --input-type (node -e hosts) is refused for a file worker.
  const execArgv = process.execArgv.filter((a) => !a.startsWith('--input-type'));
  const worker = new Worker(new URL('./neon-doc-worker.js', import.meta.url), { workerData: { url, driver }, execArgv });
  worker.unref(); // never keep a CLI process alive
  return function call(stmts: Stmt[]): Row[][] {
    const sab = new SharedArrayBuffer(4);
    const { port1, port2 } = new MessageChannel();
    worker.postMessage({ stmts, sab, port: port2 }, [port2]);
    const r = Atomics.wait(new Int32Array(sab), 0, 0, timeout);
    const msg = receiveMessageOnPort(port1);
    port1.close();
    if (r === 'timed-out' || !msg) throw new Error(`neon-doc-store: Neon did not answer within ${timeout} ms`);
    if (msg.message.error) throw new Error(`neon-doc-store: ${msg.message.error}`);
    return msg.message.rows;
  };
}

export interface DocStoreOptions { store: string; keyed?: string[]; lazy?: string[]; window?: number }

export function openDocStore({ store, keyed = [], lazy = [], window = 0 }: DocStoreOptions) {
  const schema = process.env.RVN_SCHEMA || 'rvdirect';
  if (!/^[a-z_][a-z0-9_]*$/.test(schema)) throw new Error(`neon-doc-store: bad schema name ${schema}`);
  const T = `${schema}.rec`;
  const KEYED = new Set(keyed);
  const LAZY = [...lazy];
  const DIM = Number(process.env.RVN_DIM) || 384;
  const WINDOW = Number(process.env.RVN_WINDOW ?? window) || 0;
  const call = makeCaller();
  let snapshot = new Map<string, string>(); // `${coll}\0${rid}` → JSON of the body as loaded/saved
  const stats = { loads: 0, saves: 0, upserts: 0, deletes: 0, ms: 0 };

  function toRows(doc: Doc) {
    const rows: { coll: string; rid: string; body: any }[] = [];
    for (const [key, val] of Object.entries(doc)) {
      if (val === undefined) continue;
      if (Array.isArray(val)) {
        const seen = new Map<string, number>();
        for (const el of val) {
          // Real stores repeat ids, so a repeat gets #n: both copies are kept.
          const base = el && typeof el === 'object' && (typeof el.id === 'string' || typeof el.id === 'number')
            ? `id:${el.id}` : `h:${createHash('sha1').update(JSON.stringify(el)).digest('hex').slice(0, 20)}`;
          const n = (seen.get(base) || 0) + 1; seen.set(base, n);
          rows.push({ coll: key, rid: n === 1 ? base : `${base}#${n}`, body: el });
        }
        if (val.length === 0) rows.push({ coll: '_doc', rid: `[]${key}`, body: [] }); // keep empty arrays
      } else if (KEYED.has(key) && val && typeof val === 'object') {
        for (const [k, v] of Object.entries(val)) if (v !== undefined) rows.push({ coll: key, rid: `k:${k}`, body: v });
        if (Object.keys(val).length === 0) rows.push({ coll: '_doc', rid: `{}${key}`, body: {} });
      } else {
        rows.push({ coll: '_doc', rid: key, body: val });
      }
    }
    return rows;
  }

  const DDL: Stmt[] = [
    { text: `CREATE SCHEMA IF NOT EXISTS ${schema}` },
    { text: `CREATE TABLE IF NOT EXISTS ${T} (store text NOT NULL, coll text NOT NULL, rid text NOT NULL, body jsonb NOT NULL,
               seq bigserial, ver int NOT NULL DEFAULT 1, updated_at timestamptz NOT NULL DEFAULT now(),
               vec vector(${DIM}), PRIMARY KEY (store, coll, rid))` },
    { text: `CREATE INDEX IF NOT EXISTS rec_vec_hnsw ON ${T} USING hnsw (vec vector_cosine_ops)` },
  ];
  // DDL only when the table is missing: CREATE INDEX IF NOT EXISTS still locks the table.
  function withTable<R>(fn: () => R): R {
    try { return fn(); }
    catch (e) {
      if (!/does not exist/.test((e as Error).message)) throw e;
      try { call(DDL); } catch { /* a concurrent first-time creator won the race */ }
      return fn();
    }
  }

  function load(): Doc | null {
    const t = Date.now();
    const [rows] = withTable(() => call([{ text: `SELECT coll, rid, body FROM (
               SELECT coll, rid, body, seq FROM ${T} WHERE store = $1 AND NOT (coll = ANY($2::text[]))
               UNION ALL
               SELECT coll, rid, body, seq FROM (SELECT coll, rid, body, seq FROM ${T}
                 WHERE store = $1 AND coll = ANY($2::text[]) ORDER BY seq DESC LIMIT $3) w
             ) d ORDER BY seq`, params: [store, LAZY, WINDOW] }]));
    stats.loads++; stats.ms += Date.now() - t;
    snapshot = new Map();
    if (!rows.length) return null;
    const doc: Doc = {};
    for (const { coll, rid, body } of rows) {
      snapshot.set(`${coll}\u0000${rid}`, JSON.stringify(body));
      if (coll === '_doc') {
        if (rid.startsWith('[]')) { doc[rid.slice(2)] ??= []; continue; }
        if (rid.startsWith('{}')) { doc[rid.slice(2)] ??= {}; continue; }
        doc[rid] = body;
      } else if (rid.startsWith('k:')) {
        (doc[coll] ??= {})[rid.slice(2)] = body;
      } else {
        (doc[coll] ??= []).push(body);
      }
    }
    return doc;
  }

  /** Upsert `up` [{coll, rid, body}] and delete `del` [{coll, rid}], in ~8 MB transactions. */
  function writeRows(up: { coll: string; rid: string; body: any }[], del: { coll: string; rid: string }[] = []): void {
    if (!up.length && !del.length) return;
    const items = up.map((u, i) => ({ ord: i, coll: u.coll, rid: u.rid, body: u.body }));
    const CHUNK = Number(process.env.RVN_CHUNK_BYTES) || 8 * 1024 * 1024;
    const chunks: typeof items[] = [];
    let cur: typeof items = [], size = 0;
    for (const u of items) {
      const n = JSON.stringify(u.body).length + 64;
      if (cur.length && size + n > CHUNK) { chunks.push(cur); cur = []; size = 0; }
      cur.push(u); size += n;
    }
    if (cur.length) chunks.push(cur);
    const upsert = (rows: typeof items): Stmt => ({
      // A wrong-dimension vector stays NULL (not searchable) rather than failing the save.
      text: `INSERT INTO ${T} (store, coll, rid, body, vec)
             SELECT $1, x.coll, x.rid, x.body,
                    CASE WHEN x.coll = ANY($3::text[]) AND jsonb_typeof(x.body->'embedding') = 'array'
                          AND jsonb_array_length(x.body->'embedding') = ${DIM}
                         THEN (x.body->>'embedding')::vector END
             FROM jsonb_to_recordset($2::jsonb) AS x(ord int, coll text, rid text, body jsonb) ORDER BY x.ord
             ON CONFLICT (store, coll, rid) DO UPDATE SET body = excluded.body, vec = excluded.vec,
                    ver = ${T}.ver + 1, updated_at = now()
             WHERE ${T}.body IS DISTINCT FROM excluded.body`,
      params: [store, JSON.stringify(rows), LAZY],
    });
    const delStmt: Stmt | null = del.length ? {
      text: `DELETE FROM ${T} t USING jsonb_to_recordset($2::jsonb) AS x(coll text, rid text)
             WHERE t.store = $1 AND t.coll = x.coll AND t.rid = x.rid`,
      params: [store, JSON.stringify(del)],
    } : null;
    if (!chunks.length && delStmt) withTable(() => call([delStmt]));
    chunks.forEach((c, i) => withTable(() => call(i === chunks.length - 1 && delStmt ? [upsert(c), delStmt] : [upsert(c)])));
  }

  function save(doc: Doc) {
    const t = Date.now();
    const now = new Map<string, string>();
    const up: { coll: string; rid: string; body: any }[] = [];
    for (const r of toRows(doc)) {
      const k = `${r.coll}\u0000${r.rid}`;
      const j = JSON.stringify(r.body);
      now.set(k, j);
      if (snapshot.get(k) !== j) up.push(r);
    }
    const del: { coll: string; rid: string }[] = [];
    for (const k of snapshot.keys()) if (!now.has(k)) { const [coll, rid] = k.split('\u0000'); del.push({ coll, rid }); }
    if (!up.length && !del.length) return { upserts: 0, deletes: 0 };
    writeRows(up, del);
    snapshot = now;
    stats.saves++; stats.upserts += up.length; stats.deletes += del.length; stats.ms += Date.now() - t;
    return { upserts: up.length, deletes: del.length };
  }

  /** Nearest rows of a lazy collection by cosine similarity, best first: [{ ...body, score }]. */
  function search(coll: string, vector: ArrayLike<number>, k = 5, opts: { eq?: Record<string, unknown>; threshold?: number } = {}): any[] {
    if (!vector || vector.length !== DIM) return []; // e.g. a hash-fallback vector: nothing comparable stored
    const t = Date.now();
    const params: unknown[] = [store, `[${Array.from(vector).join(',')}]`, coll, k];
    let where = '';
    for (const [f, v] of Object.entries(opts.eq || {})) { params.push(f, String(v)); where += ` AND body->>$${params.length - 1} = $${params.length}`; }
    if (opts.threshold != null) { params.push(opts.threshold); where += ` AND 1 - (vec <=> $2::vector) >= $${params.length}`; }
    const [, rows] = withTable(() => call([
      { text: `SET LOCAL hnsw.ef_search = ${Math.max(40, k * 4)}` },
      { text: `SELECT body, 1 - (vec <=> $2::vector) AS score FROM ${T}
               WHERE store = $1 AND coll = $3 AND vec IS NOT NULL${where} ORDER BY vec <=> $2::vector LIMIT $4`, params },
    ]));
    stats.ms += Date.now() - t;
    return rows.map((r: Row) => ({ ...r.body, score: Number(r.score) }));
  }

  /** Row-level access for stores that are never loaded as a document (the ReasoningBank). */
  const rows = {
    get: (coll: string, rids: string[]): Row[] => rids.length ? withTable(() => call([{ text: `SELECT rid, body FROM ${T} WHERE store = $1 AND coll = $2 AND rid = ANY($3::text[])`, params: [store, coll, rids] }]))[0] : [],
    findBy: (coll: string, field: string, value: unknown): Row[] => withTable(() => call([{ text: `SELECT rid, body FROM ${T} WHERE store = $1 AND coll = $2 AND body->>$3 = $4 ORDER BY seq LIMIT 1`, params: [store, coll, field, String(value)] }]))[0],
    count: (coll: string): number => Number(withTable(() => call([{ text: `SELECT count(*) AS n FROM ${T} WHERE store = $1 AND coll = $2`, params: [store, coll] }]))[0][0].n),
    /** Every row, oldest first, in pages — one response is capped at 64 MB. */
    all: (coll: string, eq: Record<string, unknown> = {}): any[] => {
      const out: any[] = []; let after = 0;
      const [f, v] = Object.entries(eq)[0] || [];
      for (;;) {
        const page = withTable(() => call([{ text: `SELECT seq, body FROM ${T} WHERE store = $1 AND coll = $2 AND seq > $3
          ${f ? 'AND body->>$5 = $6' : ''} ORDER BY seq LIMIT $4`, params: f ? [store, coll, after, 2000, f, String(v)] : [store, coll, after, 2000] }]))[0];
        for (const r of page) out.push(r.body);
        if (page.length < 2000) return out;
        after = page[page.length - 1].seq;
      }
    },
    write: (coll: string, upserts: { rid: string; body: any }[], deletes: string[] = []) =>
      writeRows(upserts.map((u) => ({ coll, ...u })), deletes.map((rid) => ({ coll, rid }))),
    /** Retention, only when a cap is explicitly asked for: keep the newest `keep` rows. */
    retain: (coll: string, keep: number) => withTable(() => call([{ text: `DELETE FROM ${T} WHERE store = $1 AND coll = $2 AND seq <= (
          SELECT seq FROM ${T} WHERE store = $1 AND coll = $2 ORDER BY seq DESC OFFSET $3 LIMIT 1)`, params: [store, coll, keep] }])),
    clear: (coll: string) => withTable(() => call([{ text: `DELETE FROM ${T} WHERE store = $1 AND coll = $2`, params: [store, coll] }])),
  };

  return { load, save, search, rows, stats, query: (text: string, params?: unknown[]) => withTable(() => call([{ text, params }])[0]) };
}

export type DocStore = ReturnType<typeof openDocStore>;
