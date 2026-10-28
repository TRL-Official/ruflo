import { describe, it, expect, afterEach, vi } from 'vitest';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { WorkerDaemon } from '../src/services/worker-daemon.js';

const dirs: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true }); });

describe('daemon-queue entries stay behind the supervisor gate', () => {
  it('a committed queue file runs the worker as a scheduled run, never a manual trigger', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'ruflo-queue-')); dirs.push(dir);
    mkdirSync(join(dir, '.claude-flow', 'daemon-queue'), { recursive: true });
    writeFileSync(join(dir, '.claude-flow', 'daemon-queue', 'planted.json'), JSON.stringify({ trigger: 'audit', workerId: 'x' }));
    const daemon = new WorkerDaemon(dir, { aiWorkersEnabled: false });
    const execute = vi.spyOn(daemon as never, 'executeWorker' as never).mockResolvedValue({} as never);
    (daemon as unknown as { running: boolean }).running = true;
    await (daemon as unknown as { processDispatchQueue(): Promise<void> }).processDispatchQueue();
    expect(execute).toHaveBeenCalledOnce();
    expect((execute.mock.calls[0] as unknown[])[1]).toEqual({ manualTrigger: false });
  });

  it('an operator\'s own triggerWorker call is still manual', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'ruflo-queue-')); dirs.push(dir);
    const daemon = new WorkerDaemon(dir, { aiWorkersEnabled: false });
    const execute = vi.spyOn(daemon as never, 'executeWorker' as never).mockResolvedValue({} as never);
    await daemon.triggerWorker('audit');
    expect((execute.mock.calls[0] as unknown[])[1]).toEqual({ manualTrigger: true });
  });
});
