// Host kernel limits that QuestDB depends on but cannot raise from inside its
// container.
//
// QuestDB memory-maps every column file of every partition it touches, plus
// every pending WAL segment. `vm.max_map_count` caps how many mappings one
// process may hold; when the cap is hit, mmap fails with ENOMEM (errno=12) —
// queries error out and the WAL apply job can suspend a table, even though
// plenty of RAM is free. The stock Debian/Ubuntu/RHEL value (65530) is fine
// for a fresh database but months of daily partitions × three tables — plus
// a segment backlog once a table IS suspended — can exhaust it. QuestDB's own
// web console warns below 1048576; we mirror exactly that check so both
// surfaces agree.
//
// The sysctl is kernel-global (not namespaced), so reading it from the Signal
// K process gives the value that governs the QuestDB container whenever both
// share the kernel — i.e. in managed-container mode, regardless of whether
// Signal K itself runs bare-metal or containerized. In external mode QuestDB
// may live on another machine, so the local value proves nothing and the
// probe should not be used.

import { readFile } from "fs/promises";

export const RECOMMENDED_MAX_MAP_COUNT = 1048576;

const MAX_MAP_COUNT_PATH = "/proc/sys/vm/max_map_count";

export interface MaxMapCountStatus {
  current: number;
  recommended: number;
  tooLow: boolean;
}

// The proc file holds a single decimal number (plus a trailing newline), but
// parse defensively: a garbled read must degrade to "unknown", never to a
// bogus warning.
export function parseMaxMapCount(content: string): number | null {
  const trimmed = content.trim();
  if (!/^\d+$/.test(trimmed)) return null;
  const value = Number(trimmed);
  return Number.isSafeInteger(value) ? value : null;
}

export function evaluateMaxMapCount(current: number): MaxMapCountStatus {
  return {
    current,
    recommended: RECOMMENDED_MAX_MAP_COUNT,
    tooLow: current < RECOMMENDED_MAX_MAP_COUNT,
  };
}

/** The nofile limits a container actually runs with, as reported live. */
export interface NofileLimits {
  soft: number;
  hard: number;
}

// A recorded ulimit-clamp advisory is a snapshot from the moment the
// container was created; the container may since have been recreated with
// the full limit (by signalk-container's regrant, or out-of-band by the
// operator). The advisory is stale — and must be cleared — once the live
// limits satisfy the request. Soft AND hard must both satisfy it: the soft
// limit is what actually bounds the process's fd allocation.
export function nofileClampSatisfied(
  requested: number,
  live: NofileLimits | null,
): boolean {
  return live !== null && live.soft >= requested && live.hard >= requested;
}

// Null on non-Linux, unreadable proc, or unparseable content — callers treat
// null as "unknown, say nothing" rather than warning on speculation.
export async function readMaxMapCount(
  path: string = MAX_MAP_COUNT_PATH,
): Promise<MaxMapCountStatus | null> {
  try {
    const content = await readFile(path, "utf8");
    const value = parseMaxMapCount(content);
    return value === null ? null : evaluateMaxMapCount(value);
  } catch {
    return null;
  }
}
