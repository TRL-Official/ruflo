/**
 * The async half of neon-doc-store: runs in a worker thread.
 *
 * The main thread posts {stmts, sab, port}; this runs the statements against Neon in one HTTP round
 * trip (a transaction when there is more than one), posts the rows or the error back on `port`, then
 * wakes the main thread, which is blocked in Atomics.wait. That is what lets the synchronous
 * ReasoningBank talk to Postgres. Messages posted before the listener exists are queued by Node,
 * so no "ready" handshake is needed.
 */
import { parentPort, workerData } from 'node:worker_threads';

interface Stmt { text: string; params?: unknown[] }
interface Job { stmts: Stmt[]; sab: SharedArrayBuffer; port: import('node:worker_threads').MessagePort }

const { neon } = await import(workerData.driver);
const sql = neon(workerData.url);

parentPort!.on('message', async ({ stmts, sab, port }: Job) => {
  let reply: { rows?: unknown[]; error?: string };
  try {
    const rows = stmts.length === 1
      ? [await sql.query(stmts[0].text, stmts[0].params || [])]
      : await sql.transaction(stmts.map((s) => sql.query(s.text, s.params || [])));
    reply = { rows };
  } catch (e) {
    reply = { error: String((e as Error)?.message || e) };
  }
  port.postMessage(reply);
  port.close();
  const flag = new Int32Array(sab);
  Atomics.store(flag, 0, 1);
  Atomics.notify(flag, 0);
});
