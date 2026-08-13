import { AnalysisOperationError } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/unstable/reactivity";

import {
  analysisOperationReason,
  analysisRunIdToAutoExpand,
  emptyAnalysisRunOutputLabel,
  isTerminalAnalysisRunStatus,
} from "./analysisRunUiState";

describe("analysis run UI state", () => {
  it("recognizes an uninitialized project without conflating unrelated failures", () => {
    const uninitialized = AsyncResult.failure(
      Cause.fail(
        new AnalysisOperationError({
          operation: "list",
          reason: "project-not-initialized",
          message: "Initialize this folder.",
        }),
      ),
    );
    const unrelated = AsyncResult.failure(Cause.fail(new Error("connection closed")));

    expect(analysisOperationReason(uninitialized)).toBe("project-not-initialized");
    expect(analysisOperationReason(unrelated)).toBeNull();
    expect(analysisOperationReason(AsyncResult.success({}))).toBeNull();
  });

  it("refreshes persisted history only after a live run becomes terminal", () => {
    expect(isTerminalAnalysisRunStatus("queued")).toBe(false);
    expect(isTerminalAnalysisRunStatus("starting")).toBe(false);
    expect(isTerminalAnalysisRunStatus("running")).toBe(false);
    expect(isTerminalAnalysisRunStatus("succeeded")).toBe(true);
    expect(isTerminalAnalysisRunStatus("failed")).toBe(true);
    expect(isTerminalAnalysisRunStatus("cancelled")).toBe(true);
    expect(isTerminalAnalysisRunStatus("lost")).toBe(true);
  });

  it("does not describe a completed silent run as still waiting", () => {
    expect(emptyAnalysisRunOutputLabel("queued")).toBe("Waiting for output…");
    expect(emptyAnalysisRunOutputLabel("running")).toBe("Waiting for output…");
    expect(emptyAnalysisRunOutputLabel("succeeded")).toBe("No console output.");
    expect(emptyAnalysisRunOutputLabel("failed")).toBe("No console output.");
  });

  it("opens new or active work without reopening old terminal output", () => {
    const terminal = { receipt: { runId: "old", status: "succeeded" as const } };
    const active = { receipt: { runId: "active", status: "running" as const } };
    const next = { receipt: { runId: "next", status: "succeeded" as const } };

    expect(analysisRunIdToAutoExpand([terminal], null)).toBeNull();
    expect(analysisRunIdToAutoExpand([active, terminal], null)).toBe("active");
    expect(analysisRunIdToAutoExpand([next, terminal], new Set(["old"]))).toBe("next");
    expect(analysisRunIdToAutoExpand([terminal], new Set(["old"]))).toBeNull();
  });
});
