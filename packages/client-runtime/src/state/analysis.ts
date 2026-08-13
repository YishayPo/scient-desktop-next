import { WS_METHODS } from "@t3tools/contracts";
import type { AnalysisRunSnapshot, AnalysisRunStreamEvent } from "@t3tools/contracts";
import * as Stream from "effect/Stream";
import { Atom } from "effect/unstable/reactivity";

import type { EnvironmentRegistry } from "../connection/registry.ts";
import {
  createAtomCommandScheduler,
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
  createEnvironmentRpcSubscriptionAtomFamily,
} from "./runtime.ts";

const MAXIMUM_TERMINAL_SUBSCRIPTION_RUNS = 32;

function isTerminalRun(run: AnalysisRunSnapshot): boolean {
  return ["succeeded", "failed", "cancelled", "lost"].includes(run.receipt.status);
}

function setBoundedRun(
  runs: ReadonlyMap<string, AnalysisRunSnapshot>,
  run: AnalysisRunSnapshot,
): ReadonlyMap<string, AnalysisRunSnapshot> {
  const next = new Map(runs);
  next.set(run.receipt.runId, run);
  const terminal = [...next.values()]
    .filter(isTerminalRun)
    .toSorted((left, right) => right.receipt.startedAt.localeCompare(left.receipt.startedAt));
  for (const expired of terminal.slice(MAXIMUM_TERMINAL_SUBSCRIPTION_RUNS)) {
    next.delete(expired.receipt.runId);
  }
  return next;
}

export function applyAnalysisRunStreamEvent(
  runs: ReadonlyMap<string, AnalysisRunSnapshot>,
  event: AnalysisRunStreamEvent,
): ReadonlyMap<string, AnalysisRunSnapshot> {
  if (event._tag === "run-snapshot") {
    return setBoundedRun(runs, event.run);
  }
  if (event._tag === "run-updated") {
    const current = runs.get(event.run.receipt.runId);
    return setBoundedRun(runs, {
      ...event.run,
      receipt: {
        ...event.run.receipt,
        output: current?.receipt.output ?? [],
      },
    });
  }
  const current = runs.get(event.runId);
  if (!current) return runs;
  const currentOutput = current.receipt.output;
  const lastSequence = currentOutput.at(-1)?.sequence ?? -1;
  const appendOnly = event.chunks.every(
    (chunk, index) => chunk.sequence > (event.chunks[index - 1]?.sequence ?? lastSequence),
  );
  let output: AnalysisRunSnapshot["receipt"]["output"];
  if (appendOnly) {
    output = event.chunks.length === 0 ? currentOutput : [...currentOutput, ...event.chunks];
  } else {
    const knownSequences = new Set(currentOutput.map((chunk) => chunk.sequence));
    const newChunks = event.chunks.flatMap((chunk) => {
      if (knownSequences.has(chunk.sequence)) return [];
      knownSequences.add(chunk.sequence);
      return [chunk];
    });
    output =
      newChunks.length === 0
        ? currentOutput
        : [...currentOutput, ...newChunks].toSorted(
            (left, right) => left.sequence - right.sequence,
          );
  }
  if (
    output === currentOutput &&
    current.receipt.outputTruncated === event.outputTruncated &&
    current.receipt.outputByteLength === event.outputByteLength
  ) {
    return runs;
  }
  return setBoundedRun(runs, {
    ...current,
    receipt: {
      ...current.receipt,
      output,
      outputTruncated: event.outputTruncated,
      outputByteLength: event.outputByteLength,
    },
  });
}

export function createAnalysisEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  const runScheduler = createAtomCommandScheduler();
  const runtimeScheduler = createAtomCommandScheduler();
  const cleanupScheduler = createAtomCommandScheduler();
  return {
    runtimes: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:analysis:runtimes",
      tag: WS_METHODS.analysisInspectRuntimes,
      staleTimeMs: 15_000,
    }),
    runs: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:analysis:runs",
      tag: WS_METHODS.analysisListRuns,
      // A live subscription owns in-panel updates, but it is intentionally
      // discarded on unmount. Always revalidate when a file panel remounts so
      // an older pre-completion history snapshot cannot hide persisted artifacts.
      staleTimeMs: 0,
    }),
    run: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:analysis:run",
      tag: WS_METHODS.analysisGetRun,
      staleTimeMs: 30_000,
      idleTtlMs: 60_000,
    }),
    storage: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:analysis:storage",
      tag: WS_METHODS.analysisStorageSummary,
      staleTimeMs: 0,
    }),
    runEvents: createEnvironmentRpcSubscriptionAtomFamily(runtime, {
      label: "environment-data:analysis:run-events",
      tag: WS_METHODS.subscribeAnalysisRuns,
      idleTtlMs: 0,
      transform: (stream) =>
        stream.pipe(
          Stream.scan(new Map<string, AnalysisRunSnapshot>(), applyAnalysisRunStreamEvent),
          Stream.map((runs) =>
            [...runs.values()].toSorted((left, right) =>
              right.receipt.startedAt.localeCompare(left.receipt.startedAt),
            ),
          ),
        ),
    }),
    configureRuntime: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:analysis:configure-runtime",
      tag: WS_METHODS.analysisConfigureRuntime,
      scheduler: runtimeScheduler,
      concurrency: {
        mode: "latest",
        key: ({ environmentId, input }) =>
          JSON.stringify([environmentId, input.cwd, input.runtimeKind]),
      },
    }),
    verifyRuntime: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:analysis:verify-runtime",
      tag: WS_METHODS.analysisVerifyRuntime,
      scheduler: runtimeScheduler,
      concurrency: {
        mode: "singleFlight",
        key: ({ environmentId, input }) =>
          JSON.stringify([environmentId, input.cwd, input.runtimeId]),
      },
    }),
    startRun: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:analysis:start-run",
      tag: WS_METHODS.analysisStartRun,
      scheduler: runScheduler,
      concurrency: {
        mode: "singleFlight",
        key: ({ environmentId, input }) =>
          JSON.stringify([environmentId, input.cwd, input.relativePath]),
      },
    }),
    cancelRun: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:analysis:cancel-run",
      tag: WS_METHODS.analysisCancelRun,
      scheduler: runScheduler,
      concurrency: {
        mode: "singleFlight",
        key: ({ environmentId, input }) => JSON.stringify([environmentId, input.runId]),
      },
    }),
    cleanupRun: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:analysis:cleanup-run",
      tag: WS_METHODS.analysisCleanupRun,
      scheduler: cleanupScheduler,
      concurrency: {
        mode: "singleFlight",
        key: ({ environmentId, input }) => JSON.stringify([environmentId, input.runId]),
      },
    }),
    cleanupProject: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:analysis:cleanup-project",
      tag: WS_METHODS.analysisCleanupProject,
      scheduler: cleanupScheduler,
      concurrency: {
        mode: "singleFlight",
        key: ({ environmentId, input }) => JSON.stringify([environmentId, input.cwd]),
      },
    }),
  };
}
