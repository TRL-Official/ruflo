/**
 * PostgreSQL / pgvector Memory Backend
 *
 * Stores memory entries in PostgreSQL so that several sessions — each in its own
 * container, with its own checkout — share ONE memory rather than each keeping a
 * private SQLite file that git cannot merge.
 *
 * Why this exists, and how it differs from the SQLite backend:
 *
 *   - SHARED BY CONSTRUCTION. Every write is an upsert on a primary key, so two
 *     sessions writing at once converge instead of overwriting each other.
 *   - REAL VECTOR SEARCH. `SQLiteBackend.search()` returns an empty array and
 *     tells you to use HybridBackend. Here `search()` is a pgvector HNSW query,
 *     so semantic recall works from the one backend.
 *   - NO LOCAL FILE. Nothing to commit, nothing to conflict.
 *
 * Transport: any node-postgres-compatible driver. `@neondatabase/serverless`
 * exposes the same Pool API over a WebSocket to port 443, which matters where
 * TCP 5432 is blocked — set PGSYNC_NEON_DRIVER to its index.mjs. Plain `pg`
 * works unchanged.
 *
 * @module v3/memory/postgres-backend
 */

import { EventEmitter } from 'node:events';
import {
  IMemoryBackend,
  MemoryEntry,
  MemoryEntryUpdate,
  MemoryQuery,
  SearchOptions,
  SearchResult,
  BackendStats,
  HealthCheckResult,
  ComponentHealth,
  MemoryType,
  EmbeddingGenerator,
} from './types.js';

/** The slice of node-postgres this backend uses. Structural, so any compatible driver fits. */
interface PgQueryResult {
  rows: any[];
  rowCount: number | null;
}
interface PgClientLike {
  query(text: string, params?: any[]): Promise<PgQueryResult>;
  release?(): void;
}
interface PgPoolLike {
  query(text: string, params?: any[]): Promise<PgQueryResult>;
  connect(): Promise<PgClientLike>;
  end(): Promise<void>;
}

export interface PostgresBackendConfig {
  /** postgresql://user:pass@host/db — defaults to PGSYNC_URL, then DATABASE_URL */
  connectionString: string;

  /**
   * Schema to own. Kept separate from the public schema on purpose: a Neon
   * project may already hold the `pg-sync-neon` mirror (mem / traj /
   * state_records), and these tables must not collide with it.
   */
  schema: string;

  /** Embedding width. MUST match the generator; a mismatch is refused, never padded. */
  dimensions: number;

  /** Default namespace */
  defaultNamespace: string;

  /** Optional generator, for parity with the other backends */
  embeddingGenerator?: EmbeddingGenerator;

  /**
   * HNSW search breadth, applied per query with SET LOCAL.
   *
   * It is set per transaction and NOT as a database default, because on a
   * pooled endpoint `ALTER DATABASE … SET hnsw.ef_search` is stored and then
   * never applied to a session — it reads back as the compiled-in 40. Measured
   * against Neon 2026-10-03: relying on the database default cost 1 of 100
   * exact-parity probes; setting it per query restored 100/100.
   */
  efSearch: number;

  /** Module to load the driver from. Lets a caller point at the Neon driver. */
  driverModule?: string;

  /** Enable verbose logging */
  verbose: boolean;
}

const DEFAULT_CONFIG: PostgresBackendConfig = {
  connectionString: '',
  schema: 'claude_flow_memory',
  dimensions: 384,
  defaultNamespace: 'default',
  efSearch: 400,
  verbose: false,
};

/** Postgres identifiers are not parameterisable, so a schema name is validated, never interpolated blind. */
const VALID_IDENT = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

export class PostgresBackend extends EventEmitter implements IMemoryBackend {
  private config: PostgresBackendConfig;
  private pool: PgPoolLike | null = null;
  private initialized = false;
  private schema: string;

  private stats = {
    queryCount: 0,
    totalQueryTime: 0,
    searchCount: 0,
    totalSearchTime: 0,
    writeCount: 0,
    totalWriteTime: 0,
  };

  constructor(config: Partial<PostgresBackendConfig> = {}) {
    super();
    this.config = {
      ...DEFAULT_CONFIG,
      connectionString:
        config.connectionString || process.env.PGSYNC_URL || process.env.DATABASE_URL || '',
      ...config,
    };
    if (!VALID_IDENT.test(this.config.schema) || this.config.schema.length > 63) {
      throw new Error(
        `PostgresBackend: invalid schema name ${JSON.stringify(this.config.schema)}. ` +
          'Letters, digits and underscores only, starting with a letter or underscore.'
      );
    }
    this.schema = this.config.schema;
  }

  async initialize(): Promise<void> {
    if (this.initialized) return;
    if (!this.config.connectionString) {
      throw new Error(
        '@claude-flow/memory: PostgresBackend needs a connection string. Set PGSYNC_URL ' +
          '(or DATABASE_URL), or pass connectionString.'
      );
    }

    const mod = this.config.driverModule || process.env.PGSYNC_NEON_DRIVER || 'pg';
    let driver: any;
    try {
      driver = await import(mod);
    } catch (err) {
      throw new Error(
        `@claude-flow/memory: PostgresBackend could not load its driver from '${mod}'. ` +
          "Install 'pg', or point PGSYNC_NEON_DRIVER at @neondatabase/serverless's index.mjs. " +
          'Original error: ' +
          (err instanceof Error ? err.message : String(err))
      );
    }
    const Pool = driver.Pool || driver.default?.Pool;
    if (!Pool) throw new Error(`@claude-flow/memory: '${mod}' exports no Pool`);

    // The Neon driver needs a WebSocket constructor in Node; harmless for plain pg.
    const neonConfig = driver.neonConfig || driver.default?.neonConfig;
    if (neonConfig && !neonConfig.webSocketConstructor) {
      neonConfig.webSocketConstructor = (globalThis as any).WebSocket;
    }

    this.pool = new Pool({ connectionString: this.config.connectionString }) as PgPoolLike;
    await this.createSchema();

    this.initialized = true;
    this.emit('initialized');
  }

  async shutdown(): Promise<void> {
    if (!this.initialized || !this.pool) return;
    await this.pool.end();
    this.pool = null;
    this.initialized = false;
    this.emit('shutdown');
  }

  private ensureInitialized(): void {
    if (!this.initialized || !this.pool) {
      throw new Error('PostgresBackend: not initialized — call initialize() first');
    }
  }

  private get t(): string {
    return `"${this.schema}".memory_entries`;
  }

  private async createSchema(): Promise<void> {
    const dim = this.config.dimensions;
    const s = `"${this.schema}"`;
    // One statement per call: some HTTP/pooled endpoints run one statement per
    // request, and a multi-statement string fails there.
    const statements = [
      `CREATE EXTENSION IF NOT EXISTS vector`,
      `CREATE SCHEMA IF NOT EXISTS ${s}`,
      `CREATE TABLE IF NOT EXISTS ${s}.memory_entries (
         id               text PRIMARY KEY,
         key              text NOT NULL,
         content          text NOT NULL,
         embedding        vector(${dim}),
         type             text NOT NULL,
         namespace        text NOT NULL,
         tags             jsonb NOT NULL DEFAULT '[]'::jsonb,
         metadata         jsonb NOT NULL DEFAULT '{}'::jsonb,
         owner_id         text,
         access_level     text NOT NULL,
         created_at       bigint NOT NULL,
         updated_at       bigint NOT NULL,
         expires_at       bigint,
         version          integer NOT NULL DEFAULT 1,
         "references"     jsonb NOT NULL DEFAULT '[]'::jsonb,
         access_count     integer NOT NULL DEFAULT 0,
         last_accessed_at bigint NOT NULL,
         CONSTRAINT memory_entries_dims
           CHECK (embedding IS NULL OR vector_dims(embedding) = ${dim}))`,
      `CREATE INDEX IF NOT EXISTS memory_entries_namespace ON ${s}.memory_entries (namespace)`,
      `CREATE INDEX IF NOT EXISTS memory_entries_ns_key ON ${s}.memory_entries (namespace, key)`,
      `CREATE INDEX IF NOT EXISTS memory_entries_type ON ${s}.memory_entries (type)`,
      `CREATE INDEX IF NOT EXISTS memory_entries_owner ON ${s}.memory_entries (owner_id)`,
      `CREATE INDEX IF NOT EXISTS memory_entries_updated ON ${s}.memory_entries (updated_at)`,
      `CREATE INDEX IF NOT EXISTS memory_entries_expires ON ${s}.memory_entries (expires_at)`,
      `CREATE INDEX IF NOT EXISTS memory_entries_tags ON ${s}.memory_entries USING gin (tags)`,
      // Built on an empty (or already small) table so each insert maintains it
      // incrementally — no single long CREATE INDEX that could outlive a
      // request timeout.
      `CREATE INDEX IF NOT EXISTS memory_entries_hnsw
         ON ${s}.memory_entries USING hnsw (embedding vector_cosine_ops)`,
      `CREATE INDEX IF NOT EXISTS memory_entries_content_fts
         ON ${s}.memory_entries USING gin (to_tsvector('english', content))`,
    ];
    for (const sql of statements) await this.pool!.query(sql);
  }

  /* ───────────────────────────── writes ──────────────────────────────── */

  private vecLiteral(v: Float32Array): string {
    // pgvector's text form. Math.fround keeps this bit-identical to what a
    // float4 column stores, so a round trip does not drift.
    const parts = new Array(v.length);
    for (let i = 0; i < v.length; i++) parts[i] = String(Math.fround(v[i]));
    return `[${parts.join(',')}]`;
  }

  private entryParams(entry: MemoryEntry): any[] {
    if (entry.embedding && entry.embedding.length !== this.config.dimensions) {
      // Rule: never pad, never truncate. A wrong-width vector is a bug upstream;
      // silently reshaping it corrupts every later search.
      throw new Error(
        `PostgresBackend: entry ${entry.id} has a ${entry.embedding.length}-dim embedding, ` +
          `expected ${this.config.dimensions}`
      );
    }
    return [
      entry.id,
      entry.key,
      entry.content,
      entry.embedding ? this.vecLiteral(entry.embedding) : null,
      entry.type,
      entry.namespace,
      JSON.stringify(entry.tags ?? []),
      JSON.stringify(entry.metadata ?? {}),
      entry.ownerId ?? null,
      entry.accessLevel,
      entry.createdAt,
      entry.updatedAt,
      entry.expiresAt ?? null,
      entry.version,
      JSON.stringify(entry.references ?? []),
      entry.accessCount,
      entry.lastAccessedAt,
    ];
  }

  private readonly UPSERT = (schema: string, dim: number) => `
    INSERT INTO "${schema}".memory_entries (
      id, key, content, embedding, type, namespace, tags, metadata, owner_id,
      access_level, created_at, updated_at, expires_at, version, "references",
      access_count, last_accessed_at
    ) VALUES ($1,$2,$3,$4::vector(${dim}),$5,$6,$7::jsonb,$8::jsonb,$9,$10,$11,$12,$13,$14,$15::jsonb,$16,$17)
    ON CONFLICT (id) DO UPDATE SET
      key = EXCLUDED.key, content = EXCLUDED.content, embedding = EXCLUDED.embedding,
      type = EXCLUDED.type, namespace = EXCLUDED.namespace, tags = EXCLUDED.tags,
      metadata = EXCLUDED.metadata, owner_id = EXCLUDED.owner_id,
      access_level = EXCLUDED.access_level, updated_at = EXCLUDED.updated_at,
      expires_at = EXCLUDED.expires_at, version = EXCLUDED.version,
      "references" = EXCLUDED."references", access_count = EXCLUDED.access_count,
      last_accessed_at = EXCLUDED.last_accessed_at`;

  async store(entry: MemoryEntry): Promise<void> {
    this.ensureInitialized();
    const t0 = performance.now();
    await this.pool!.query(this.UPSERT(this.schema, this.config.dimensions), this.entryParams(entry));
    this.stats.writeCount++;
    this.stats.totalWriteTime += performance.now() - t0;
    this.emit('entry:stored', { id: entry.id });
  }

  async bulkInsert(entries: MemoryEntry[]): Promise<void> {
    this.ensureInitialized();
    if (entries.length === 0) return;
    const client = await this.pool!.connect();
    try {
      await client.query('BEGIN');
      const sql = this.UPSERT(this.schema, this.config.dimensions);
      // Sorted by id so concurrent sessions take row locks in the same order
      // and overlapping batches queue instead of deadlocking.
      for (const e of [...entries].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))) {
        await client.query(sql, this.entryParams(e));
      }
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release?.();
    }
    this.stats.writeCount += entries.length;
    this.emit('bulk:inserted', { count: entries.length });
  }

  async update(id: string, update: MemoryEntryUpdate): Promise<MemoryEntry | null> {
    this.ensureInitialized();
    const entry = await this.get(id);
    if (!entry) return null;

    if (update.content !== undefined) entry.content = update.content;
    if (update.tags !== undefined) entry.tags = update.tags;
    if (update.metadata !== undefined) entry.metadata = { ...entry.metadata, ...update.metadata };
    if (update.accessLevel !== undefined) entry.accessLevel = update.accessLevel;
    if (update.expiresAt !== undefined) entry.expiresAt = update.expiresAt;
    if (update.references !== undefined) entry.references = update.references;

    entry.updatedAt = Date.now();
    entry.version++;

    await this.store(entry);
    this.emit('entry:updated', { id });
    return entry;
  }

  async delete(id: string): Promise<boolean> {
    this.ensureInitialized();
    const r = await this.pool!.query(`DELETE FROM ${this.t} WHERE id = $1`, [id]);
    const gone = (r.rowCount ?? 0) > 0;
    if (gone) this.emit('entry:deleted', { id });
    return gone;
  }

  async bulkDelete(ids: string[]): Promise<number> {
    this.ensureInitialized();
    if (ids.length === 0) return 0;
    const r = await this.pool!.query(`DELETE FROM ${this.t} WHERE id = ANY($1)`, [ids]);
    const n = r.rowCount ?? 0;
    this.emit('bulk:deleted', { count: n });
    return n;
  }

  /* ───────────────────────────── reads ───────────────────────────────── */

  private rowToEntry(row: any): MemoryEntry {
    let embedding: Float32Array | undefined;
    if (row.embedding) {
      // pgvector returns its text form '[a,b,…]' unless a type parser is registered.
      const raw =
        typeof row.embedding === 'string'
          ? row.embedding.replace(/^\[|\]$/g, '').split(',')
          : row.embedding;
      if (raw.length) embedding = Float32Array.from(raw, (x: any) => Number(x));
    }
    const asArray = (v: any): any[] => (Array.isArray(v) ? v : typeof v === 'string' ? JSON.parse(v || '[]') : []);
    const asObject = (v: any): Record<string, unknown> =>
      v && typeof v === 'object' ? v : typeof v === 'string' ? JSON.parse(v || '{}') : {};

    return {
      id: row.id,
      key: row.key,
      content: row.content,
      embedding,
      type: row.type as MemoryType,
      namespace: row.namespace,
      tags: asArray(row.tags),
      metadata: asObject(row.metadata),
      ownerId: row.owner_id ?? undefined,
      accessLevel: row.access_level,
      createdAt: Number(row.created_at),
      updatedAt: Number(row.updated_at),
      expiresAt: row.expires_at == null ? undefined : Number(row.expires_at),
      version: Number(row.version),
      references: asArray(row.references),
      accessCount: Number(row.access_count),
      lastAccessedAt: Number(row.last_accessed_at),
    } as MemoryEntry;
  }

  async get(id: string): Promise<MemoryEntry | null> {
    this.ensureInitialized();
    const r = await this.pool!.query(
      `SELECT *, embedding::text AS embedding FROM ${this.t} WHERE id = $1`,
      [id]
    );
    return r.rows[0] ? this.rowToEntry(r.rows[0]) : null;
  }

  async getByKey(namespace: string, key: string): Promise<MemoryEntry | null> {
    this.ensureInitialized();
    const r = await this.pool!.query(
      `SELECT *, embedding::text AS embedding FROM ${this.t} WHERE namespace = $1 AND key = $2`,
      [namespace, key]
    );
    return r.rows[0] ? this.rowToEntry(r.rows[0]) : null;
  }

  async query(query: MemoryQuery): Promise<MemoryEntry[]> {
    this.ensureInitialized();
    const t0 = performance.now();
    const where: string[] = [];
    const params: any[] = [];
    const p = (v: any) => `$${params.push(v)}`;

    if (query.namespace) where.push(`namespace = ${p(query.namespace)}`);
    if (query.key) where.push(`key = ${p(query.key)}`);
    if (query.keyPrefix) where.push(`key LIKE ${p(query.keyPrefix + '%')}`);
    if (query.memoryType) where.push(`type = ${p(query.memoryType)}`);
    if (query.accessLevel) where.push(`access_level = ${p(query.accessLevel)}`);
    if (query.ownerId) where.push(`owner_id = ${p(query.ownerId)}`);
    if (query.createdAfter) where.push(`created_at >= ${p(query.createdAfter)}`);
    if (query.createdBefore) where.push(`created_at <= ${p(query.createdBefore)}`);
    if (query.updatedAfter) where.push(`updated_at >= ${p(query.updatedAfter)}`);
    if (query.updatedBefore) where.push(`updated_at <= ${p(query.updatedBefore)}`);
    if (!query.includeExpired) where.push(`(expires_at IS NULL OR expires_at > ${p(Date.now())})`);

    if (query.tags && query.tags.length > 0) {
      for (const tag of query.tags) {
        if (typeof tag !== 'string' || !/^[a-zA-Z0-9_\-.:]+$/.test(tag)) {
          throw new Error(`Invalid tag format: ${tag}`);
        }
      }
      // ?| is "has any of these keys"; the cast keeps it parameterised.
      where.push(`tags ?| ${p(query.tags)}::text[]`);
    }

    let sql = `SELECT *, embedding::text AS embedding FROM ${this.t}`;
    if (where.length) sql += ` WHERE ${where.join(' AND ')}`;
    sql += ` ORDER BY updated_at DESC`;
    if (typeof (query as any).limit === 'number') sql += ` LIMIT ${p((query as any).limit)}`;
    if (typeof (query as any).offset === 'number') sql += ` OFFSET ${p((query as any).offset)}`;

    const r = await this.pool!.query(sql, params);
    this.stats.queryCount++;
    this.stats.totalQueryTime += performance.now() - t0;
    return r.rows.map((row) => this.rowToEntry(row));
  }

  /**
   * Semantic search over pgvector's HNSW index.
   *
   * Unlike SQLiteBackend.search(), which returns nothing, this is a real
   * nearest-neighbour query. ef_search is applied with SET LOCAL inside the
   * transaction — see PostgresBackendConfig.efSearch for why a database
   * default cannot be relied on.
   */
  async search(embedding: Float32Array, options: SearchOptions): Promise<SearchResult[]> {
    this.ensureInitialized();
    if (embedding.length !== this.config.dimensions) {
      throw new Error(
        `PostgresBackend.search: query vector is ${embedding.length}-dim, ` +
          `expected ${this.config.dimensions}`
      );
    }
    const t0 = performance.now();
    const k = Math.max(1, options.k ?? 10);
    const ef = Math.max(k, options.ef ?? this.config.efSearch);

    const where: string[] = ['embedding IS NOT NULL'];
    const params: any[] = [this.vecLiteral(embedding)];
    const p = (v: any) => `$${params.push(v)}`;
    const f = options.filters;
    if (f?.namespace) where.push(`namespace = ${p(f.namespace)}`);
    if (f?.memoryType) where.push(`type = ${p(f.memoryType)}`);
    if (f?.ownerId) where.push(`owner_id = ${p(f.ownerId)}`);
    if (!f?.includeExpired) where.push(`(expires_at IS NULL OR expires_at > ${p(Date.now())})`);

    const sql =
      `SELECT *, embedding::text AS embedding, ` +
      `embedding <=> $1::vector(${this.config.dimensions}) AS distance ` +
      `FROM ${this.t} WHERE ${where.join(' AND ')} ` +
      `ORDER BY distance LIMIT ${p(k)}`;

    const client = await this.pool!.connect();
    let rows: any[];
    try {
      await client.query('BEGIN');
      await client.query(`SET LOCAL hnsw.ef_search = ${Math.floor(ef)}`);
      rows = (await client.query(sql, params)).rows;
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release?.();
    }

    this.stats.searchCount++;
    this.stats.totalSearchTime += performance.now() - t0;

    const threshold = options.threshold;
    return rows
      .map((row) => {
        const distance = Number(row.distance);
        return { entry: this.rowToEntry(row), score: 1 - distance, distance };
      })
      .filter((r) => (threshold === undefined ? true : r.score >= threshold));
  }

  async count(namespace?: string): Promise<number> {
    this.ensureInitialized();
    const r = namespace
      ? await this.pool!.query(`SELECT count(*)::bigint AS n FROM ${this.t} WHERE namespace = $1`, [namespace])
      : await this.pool!.query(`SELECT count(*)::bigint AS n FROM ${this.t}`);
    return Number(r.rows[0]?.n ?? 0);
  }

  async listNamespaces(): Promise<string[]> {
    this.ensureInitialized();
    const r = await this.pool!.query(`SELECT DISTINCT namespace FROM ${this.t} ORDER BY 1`);
    return r.rows.map((row) => row.namespace);
  }

  async clearNamespace(namespace: string): Promise<number> {
    this.ensureInitialized();
    const r = await this.pool!.query(`DELETE FROM ${this.t} WHERE namespace = $1`, [namespace]);
    const n = r.rowCount ?? 0;
    this.emit('namespace:cleared', { namespace, count: n });
    return n;
  }

  async getStats(): Promise<BackendStats> {
    this.ensureInitialized();
    const [total, byNs, byType, size] = await Promise.all([
      this.pool!.query(`SELECT count(*)::bigint AS n FROM ${this.t}`),
      this.pool!.query(`SELECT namespace, count(*)::bigint AS n FROM ${this.t} GROUP BY 1`),
      this.pool!.query(`SELECT type, count(*)::bigint AS n FROM ${this.t} GROUP BY 1`),
      this.pool!.query(`SELECT pg_total_relation_size('${this.schema}.memory_entries')::bigint AS b`),
    ]);
    const entriesByNamespace: Record<string, number> = {};
    for (const r of byNs.rows) entriesByNamespace[r.namespace] = Number(r.n);
    const entriesByType = {} as Record<MemoryType, number>;
    for (const r of byType.rows) (entriesByType as any)[r.type] = Number(r.n);

    return {
      totalEntries: Number(total.rows[0]?.n ?? 0),
      entriesByNamespace,
      entriesByType,
      memoryUsage: Number(size.rows[0]?.b ?? 0),
      avgQueryTime: this.stats.queryCount ? this.stats.totalQueryTime / this.stats.queryCount : 0,
      avgSearchTime: this.stats.searchCount ? this.stats.totalSearchTime / this.stats.searchCount : 0,
    };
  }

  async healthCheck(): Promise<HealthCheckResult> {
    const issues: string[] = [];
    const recommendations: string[] = [];

    const probe = async (sql: string): Promise<ComponentHealth> => {
      const t0 = performance.now();
      try {
        await this.pool!.query(sql);
        return { status: 'healthy', latency: performance.now() - t0 };
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        issues.push(message);
        return { status: 'unhealthy', latency: performance.now() - t0, message };
      }
    };

    if (!this.initialized || !this.pool) {
      return {
        status: 'unhealthy',
        components: {
          storage: { status: 'unhealthy', latency: 0, message: 'not initialized' },
          index: { status: 'unhealthy', latency: 0, message: 'not initialized' },
          cache: { status: 'healthy', latency: 0, message: 'not used by this backend' },
        },
        timestamp: Date.now(),
        issues: ['PostgresBackend is not initialized'],
        recommendations: ['Call initialize() before use'],
      };
    }

    const storage = await probe(`SELECT 1 FROM ${this.t} LIMIT 1`);
    const index = await probe(
      `SELECT 1 FROM pg_indexes WHERE schemaname = '${this.schema}' AND indexname = 'memory_entries_hnsw'`
    );
    if (index.status === 'healthy') {
      const r = await this.pool!.query(
        `SELECT count(*)::int AS n FROM pg_indexes WHERE schemaname = '${this.schema}' AND indexname = 'memory_entries_hnsw'`
      );
      if (Number(r.rows[0]?.n ?? 0) === 0) {
        index.status = 'degraded';
        index.message = 'HNSW index missing — semantic search will fall back to a sequential scan';
        issues.push(index.message);
        recommendations.push('Re-run initialize() to recreate the index');
      }
    }

    const status =
      storage.status === 'unhealthy' || index.status === 'unhealthy'
        ? 'unhealthy'
        : index.status === 'degraded'
          ? 'degraded'
          : 'healthy';

    return {
      status,
      components: {
        storage,
        index,
        cache: { status: 'healthy', latency: 0, message: 'not used by this backend' },
      },
      timestamp: Date.now(),
      issues,
      recommendations,
    };
  }
}
