/**
 * Codemod intent lists (ADR-143) — no dependency on the TypeScript compiler.
 *
 * [neon-direct] Split out of engine.ts (2026-10-20): the model router needs only to KNOW which intents are
 * deterministic, and importing engine.ts for that pulled in `typescript`, which is not a dependency of the
 * published @claude-flow/cli. In any install without it, the router's import failed and `hooks pre-task` /
 * `hooks_route` silently dropped the whole model recommendation ([TASK_MODEL_RECOMMENDATION]). engine.ts
 * re-exports these, so existing imports keep working.
 *
 * @module ruvector/codemods/intents
 */

export type CodemodIntent = 'var-to-const' | 'remove-console' | 'add-logging';

/** Intents the codemod engine can apply deterministically with $0 cost. */
export const DETERMINISTIC_CODEMOD_INTENTS: readonly CodemodIntent[] = [
  'var-to-const',
  'remove-console',
  'add-logging',
] as const;

/** Intents recognised by the router but NOT safe as deterministic codemods. */
export const MODEL_ROUTED_INTENTS: readonly string[] = [
  'add-types',
  'add-error-handling',
  'async-await',
] as const;

export function isDeterministicCodemod(intent: string): intent is CodemodIntent {
  return (DETERMINISTIC_CODEMOD_INTENTS as readonly string[]).includes(intent);
}
