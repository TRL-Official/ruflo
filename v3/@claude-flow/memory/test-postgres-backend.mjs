/**
 * Functional test for PostgresBackend against a real Postgres/pgvector.
 *
 * Uses an isolated schema so it can never touch anything else in the database,
 * and drops it at the end. Run:
 *   PGSYNC_URL=... PGSYNC_NEON_DRIVER=... node test-postgres-backend.mjs
 */
import { PostgresBackend } from './dist/postgres-backend.js';

const SCHEMA = 'cf_memory_test';
const DIM = 384;
let pass = 0, fail = 0;
const ok = (m) => { pass++; console.log(`  ok   ${m}`); };
const bad = (m, extra) => { fail++; console.log(`  FAIL ${m}${extra ? ' — ' + extra : ''}`); };
const want = (m, expected, actual) =>
  JSON.stringify(expected) === JSON.stringify(actual)
    ? ok(m)
    : bad(m, `expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);

const vec = (seed) => {
  const v = new Float32Array(DIM);
  for (let i = 0; i < DIM; i++) v[i] = Math.sin(seed * 0.7 + i * 0.013);
  // normalise, so cosine distance behaves predictably
  let n = 0; for (let i = 0; i < DIM; i++) n += v[i] * v[i];
  n = Math.sqrt(n); for (let i = 0; i < DIM; i++) v[i] /= n;
  return v;
};
const entry = (id, over = {}) => ({
  id, key: `key-${id}`, content: `content of ${id}`,
  type: 'semantic', namespace: 'test-ns', tags: ['alpha'], metadata: { from: id },
  accessLevel: 'private', createdAt: Date.now(), updatedAt: Date.now(),
  version: 1, references: [], accessCount: 0, lastAccessedAt: Date.now(), ...over,
});

const cfg = { schema: SCHEMA, dimensions: DIM, defaultNamespace: 'test-ns', verbose: false };
const b = new PostgresBackend(cfg);

try {
  await b.initialize();
  ok('initialize() creates the schema and indexes');

  // clean slate if a previous run died
  await b.clearNamespace('test-ns');
  await b.clearNamespace('other-ns');

  // ---- store / get / getByKey
  await b.store(entry('a1', { embedding: vec(1) }));
  const got = await b.get('a1');
  want('store() then get() round-trips the content', 'content of a1', got?.content);
  want('…and the tags', ['alpha'], got?.tags);
  want('…and the metadata', { from: 'a1' }, got?.metadata);
  ok(`…and the embedding came back ${got?.embedding?.length}-dim`);
  let bitExact = got?.embedding?.length === DIM;
  if (bitExact) {
    const src = vec(1);
    for (let i = 0; i < DIM; i++) if (Math.fround(src[i]) !== got.embedding[i]) { bitExact = false; break; }
  }
  want('…and the embedding is bit-exact', true, bitExact);
  const byKey = await b.getByKey('test-ns', 'key-a1');
  want('getByKey() finds the same row', 'a1', byKey?.id);
  want('get() on a missing id returns null', null, await b.get('nope'));

  // ---- update
  const upd = await b.update('a1', { content: 'rewritten', metadata: { extra: 1 } });
  want('update() changes content', 'rewritten', upd?.content);
  want('…merges metadata rather than replacing it', { from: 'a1', extra: 1 }, upd?.metadata);
  want('…and bumps the version', 2, upd?.version);

  // ---- idempotence: storing the same id twice must not duplicate
  await b.store(entry('a1', { embedding: vec(1), content: 'rewritten again' }));
  want('store() on an existing id upserts, not duplicates', 1, await b.count('test-ns'));

  // ---- bulkInsert / query
  await b.bulkInsert([
    entry('b1', { embedding: vec(2), tags: ['beta'] }),
    entry('b2', { embedding: vec(3), tags: ['beta', 'alpha'] }),
    entry('c1', { embedding: vec(4), namespace: 'other-ns' }),
  ]);
  want('bulkInsert() inserted into the right namespaces', 3, await b.count('test-ns'));
  want('…and the other namespace has its own row', 1, await b.count('other-ns'));
  const byTag = await b.query({ namespace: 'test-ns', tags: ['beta'] });
  want('query() filters by tag', ['b1', 'b2'], byTag.map((e) => e.id).sort());
  const byPrefix = await b.query({ namespace: 'test-ns', keyPrefix: 'key-b' });
  want('query() filters by key prefix', 2, byPrefix.length);
  want('listNamespaces() sees both', ['other-ns', 'test-ns'], await b.listNamespaces());

  // ---- search: the thing SQLiteBackend cannot do
  const hits = await b.search(vec(2), { k: 3, filters: { namespace: 'test-ns' } });
  want('search() returns results (SQLite returns none)', true, hits.length > 0);
  want('…and the nearest hit is the exact vector stored', 'b1', hits[0]?.entry.id);
  want('…with a distance of ~0', true, hits[0]?.distance < 1e-6);
  want('…and results are ordered by distance', true,
    hits.every((h, i) => i === 0 || hits[i - 1].distance <= h.distance));
  const filtered = await b.search(vec(2), { k: 5, filters: { namespace: 'other-ns' } });
  want('search() honours the namespace filter', ['c1'], filtered.map((h) => h.entry.id));

  // ---- the dimension rule: never pad, never truncate
  let refused = false;
  try { await b.store(entry('bad', { embedding: new Float32Array(10) })); }
  catch { refused = true; }
  want('a wrong-dimension embedding is REFUSED, not reshaped', true, refused);

  // ---- stats / health
  const stats = await b.getStats();
  want('getStats() counts every row', 4, stats.totalEntries);
  want('…and breaks down by namespace', 3, stats.entriesByNamespace['test-ns']);
  const health = await b.healthCheck();
  want('healthCheck() reports healthy', 'healthy', health.status);

  // ---- MULTI-SESSION: two independent backends against one database
  const b2 = new PostgresBackend(cfg);
  await b2.initialize();
  await Promise.all([
    b.store(entry('s-A', { embedding: vec(10), content: 'written by session A' })),
    b2.store(entry('s-B', { embedding: vec(11), content: 'written by session B' })),
  ]);
  const seenByA = await b.get('s-B');
  const seenByB = await b2.get('s-A');
  want("session A sees session B's write immediately", 'written by session B', seenByA?.content);
  want("session B sees session A's write immediately", 'written by session A', seenByB?.content);
  want('neither overwrote the other', 5, await b.count('test-ns'));
  await b2.shutdown();

  // ---- delete paths
  want('delete() removes a row', true, await b.delete('s-A'));
  want('delete() on a missing row is false', false, await b.delete('s-A'));
  want('bulkDelete() returns the number removed', 2, await b.bulkDelete(['b1', 'b2']));
  want('clearNamespace() empties just that namespace', 1, await b.clearNamespace('other-ns'));
  want('…leaving the other namespace alone', 2, await b.count('test-ns'));
} catch (err) {
  bad('unexpected exception', err?.stack || String(err));
} finally {
  try {
    const pool = b.pool;
    if (pool) await pool.query(`DROP SCHEMA IF EXISTS "${SCHEMA}" CASCADE`);
  } catch { /* best effort */ }
  await b.shutdown().catch(() => undefined);
}

console.log(`\n=== ${pass} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
