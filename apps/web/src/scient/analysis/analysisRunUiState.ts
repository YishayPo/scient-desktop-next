import type { AnalysisRunSummary } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/unstable/reactivity";

const TERMINAL_ANALYSIS_RUN_STATUSES = new Set<AnalysisRunSummary["receipt"]["status"]>([
  "succeeded",
  "failed",
  "cancelled",
  "lost",
]);

export function isTerminalAnalysisRunStatus(
  status: AnalysisRunSummary["receipt"]["status"],
): boolean {
  return TERMINAL_ANALYSIS_RUN_STATUSES.has(status);
}

export function emptyAnalysisRunOutputLabel(
  status: AnalysisRunSummary["receipt"]["status"],
): string {
  return isTerminalAnalysisRunStatus(status) ? "No console output." : "Waiting for output…";
}

export function analysisRunIdToAutoExpand(
  runs: ReadonlyArray<{
    readonly receipt: {
      readonly runId: string;
      readonly status: AnalysisRunSummary["receipt"]["status"];
    };
  }>,
  previouslyObserved: ReadonlySet<string> | null,
): string | null {
  if (previouslyObserved === null) {
    return (
      runs.find((run) => !isTerminalAnalysisRunStatus(run.receipt.status))?.receipt.runId ?? null
    );
  }
  return runs.find((run) => !previouslyObserved.has(run.receipt.runId))?.receipt.runId ?? null;
}

export function analysisOperationReason(
  result: AsyncResult.AsyncResult<unknown, unknown>,
): string | null {
  if (result._tag !== "Failure") return null;
  const error = Cause.squash(result.cause);
  if (typeof error !== "object" || error === null) return null;
  const candidate = error as { readonly _tag?: unknown; readonly reason?: unknown };
  return candidate._tag === "AnalysisOperationError" && typeof candidate.reason === "string"
    ? candidate.reason
    : null;
}
