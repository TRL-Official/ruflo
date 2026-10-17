/**
 * NeonReasoningBank — a drop-in for LocalReasoningBank (../intelligence.ts) that keeps the patterns
 * in Neon instead of .claude-flow/neural/patterns.json, with no 5,000-pattern cap.
 *
 * Selected by RUFLO_STORE=neon. LocalReasoningBank loads the whole file in every process (37 MB
 * compact at 4,300 patterns; a whole-store load would pass Neon's 64 MB response limit at ~7,500)
 * and evicts the oldest pattern at maxSize. This one loads nothing up front:
 *   findSimilar → a pgvector cosine search in Neon (type filter and threshold in SQL)
 *   get / store → fetched by id; content de-duplication asks Neon, as the original asks its list
 *   writes      → every object handed out is remembered with the JSON it had; a flush upserts the
 *                 ones that changed (distillLearning mutates objects from get() and relies on a
 *                 later flush) and deletes what was deleted, so concurrent sessions merge per pattern
 *                 instead of the last whole-file write winning
 *   cap         → none unless maxSize / RUFLO_MAX_PATTERNS is finite; then it is retention in Neon
 *                 (newest N kept), not an in-process shift() that cannot see other sessions' rows
 *
 * Gates: knowledge/digital-twin-stack/neon-direct/ in TRL-Official/digital-twin.
 */
import { openDocStore, type DocStore } from './neon-doc-store.js';

/* eslint-disable @typescript-eslint/no-explicit-any */
type Pattern = any;

const COLL = 'patterns';

/** An Array (Array.isArray is true) whose contents are fetched on first access. */
function lazyList<T>(fetch: () => T[]): T[] {
  let arr: T[] | null = null;
  const load = () => (arr ??= fetch());
  return new Proxy([] as T[], {
    get(_, p) { const a = load(); const v = Reflect.get(a, p, a); return typeof v === 'function' ? v.bind(a) : v; },
    set(_, p, v) { return Reflect.set(load(), p, v); },
    has(_, p) { return Reflect.has(load(), p); },
    ownKeys() { return Reflect.ownKeys(load()); },
    getOwnPropertyDescriptor(_, p) { const d = Reflect.getOwnPropertyDescriptor(load(), p); if (d && p !== 'length') d.configurable = true; return d; },
    deleteProperty(_, p) { return Reflect.deleteProperty(load(), p); },
  });
}
const rid = (id: string) => `id:${id}`;

export class NeonReasoningBank {
  private maxSize: number;
  private persistenceEnabled: boolean;
  private db: DocStore;
  private live = new Map<string, Pattern>();   // id → the object handed out (identity kept)
  private orig = new Map<string, string>();    // id → its JSON when adopted / last flushed; absent = new
  private deleted = new Set<string>();
  private saveTimeout: ReturnType<typeof setTimeout> | null = null;

  constructor(options: { maxSize: number; persistence?: boolean }) {
    this.maxSize = Number.isFinite(options.maxSize) ? options.maxSize : Infinity;
    this.persistenceEnabled = options.persistence !== false;
    this.db = openDocStore({ store: process.env.RUFLO_NEON_STORE || 'ruflo-patterns', lazy: [COLL] });
  }

  private adopt(body: Pattern): Pattern {
    const have = this.live.get(body.id);
    if (have) return have;
    this.live.set(body.id, body);
    this.orig.set(body.id, JSON.stringify(body));
    return body;
  }

  private saveToDisk(): void {
    if (!this.persistenceEnabled) return;
    if (this.saveTimeout) clearTimeout(this.saveTimeout);
    this.saveTimeout = setTimeout(() => this.flushToDisk(), 100);
  }

  flushToDisk(): void {
    if (this.saveTimeout) { clearTimeout(this.saveTimeout); this.saveTimeout = null; }
    if (!this.persistenceEnabled) return;
    const up: { rid: string; body: Pattern; j: string; id: string }[] = [];
    for (const [id, obj] of this.live) {
      const j = JSON.stringify(obj);
      if (this.orig.get(id) !== j) up.push({ rid: rid(id), body: obj, j, id });
    }
    if (!up.length && !this.deleted.size) return;
    try {
      this.db.rows.write(COLL, up.map((u) => ({ rid: u.rid, body: u.body })), [...this.deleted].map(rid));
      for (const u of up) this.orig.set(u.id, u.j);
      this.deleted.clear();
      if (Number.isFinite(this.maxSize)) this.db.rows.retain(COLL, this.maxSize);
    } catch (error) {
      // As the original: persistence failures must not break training. Say so, and keep the
      // changes pending so the next flush retries them.
      console.error('Failed to persist patterns to Neon:', (error as Error).message);
    }
  }

  get(id: string): Pattern | undefined {
    if (this.deleted.has(id)) return undefined;
    if (this.live.has(id)) return this.live.get(id);
    const [row] = this.db.rows.get(COLL, [rid(id)]);
    return row ? this.adopt(row.body) : undefined;
  }

  private findByContent(content: string): Pattern | undefined {
    for (const p of this.live.values()) if (p.content === content && !this.deleted.has(p.id)) return p;
    const [row] = this.db.rows.findBy(COLL, 'content', content);
    return row && !this.deleted.has(row.body.id) ? this.adopt(row.body) : undefined;
  }

  store(pattern: Pattern): boolean {
    const now = Date.now();
    const stored = { ...pattern, usageCount: pattern.usageCount ?? 0, createdAt: pattern.createdAt ?? now, lastUsedAt: pattern.lastUsedAt ?? now };
    const existing = this.get(pattern.id);
    if (existing) {
      stored.usageCount = existing.usageCount + 1;
      stored.createdAt = existing.createdAt;
      this.live.set(pattern.id, stored);
      this.saveToDisk();
      return false;
    }
    const dupe = this.findByContent(pattern.content);
    if (dupe) {
      dupe.usageCount++;
      dupe.lastUsedAt = now;
      if (stored.confidence > dupe.confidence) dupe.confidence = stored.confidence;
      this.saveToDisk();
      return false;
    }
    this.deleted.delete(pattern.id);
    this.live.set(pattern.id, stored); // no orig entry → new → upserted at flush
    this.saveToDisk();
    return true;
  }

  findSimilar(queryEmbedding: number[], options: { k?: number; threshold?: number; type?: string }): Pattern[] {
    const { k = 5, threshold = 0.5, type } = options;
    this.flushToDisk(); // patterns stored in this process must be searchable too
    const hits = this.db.search(COLL, queryEmbedding, k, { eq: type ? { type } : {}, threshold });
    return hits.filter((h) => !this.deleted.has(h.id)).map(({ score, ...body }) => {
      const p = this.adopt(body);
      p.usageCount++;
      p.lastUsedAt = Date.now();
      this.saveToDisk();
      return { ...p, similarity: score };
    });
  }

  stats(): { size: number; patternCount: number } {
    this.flushToDisk();
    const n = this.db.rows.count(COLL);
    return { size: n, patternCount: n };
  }

  // The whole bank is ~37 MB at 4,300 patterns (embeddings included) — ~20 s over the network. ruflo's
  // endTrajectory() calls getAll() and never uses the result, on every post-task. So the list is LAZY:
  // an array that fetches on first real use (length, an index, map, iteration) and is otherwise free.
  getAll(): Pattern[] {
    this.flushToDisk();
    return lazyList(() => this.db.rows.all(COLL).map((b) => this.adopt(b)));
  }

  getByType(type: string): Pattern[] {
    this.flushToDisk();
    return lazyList(() => this.db.rows.all(COLL, { type }).map((b) => this.adopt(b)));
  }

  delete(id: string): boolean {
    if (!this.get(id)) return false;
    this.live.delete(id);
    this.orig.delete(id);
    this.deleted.add(id);
    this.saveToDisk();
    return true;
  }

  clear(): void {
    this.db.rows.clear(COLL);
    this.live.clear(); this.orig.clear(); this.deleted.clear();
  }
}
