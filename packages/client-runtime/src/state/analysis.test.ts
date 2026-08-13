import type { AnalysisRunSnapshot, AnalysisRunStreamEvent } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";

import { applyAnalysisRunStreamEvent } from "./analysis.ts";

const run = {
  contractVersion: 1,
  projectId: "project-1",
  action: "run-file",
  runtime: {
    id: "matlab:local" as never,
    kind: "matlab",
    label: "MATLAB",
    availability: "available",
    source: "path",
    executablePath: "/opt/matlab/bin/matlab",
    version: null,
    detail: null,
    capabilities: ["run-file", "stream-output", "cancel-process-tree"],
    inspectedAt: "2026-08-12T00:00:00.000Z",
    verification: null,
  },
  source: {
    cwd: "/project",
    relativePath: "analysis.m",
    revision: "sha256:source" as never,
  },
  phase: "running",
  queuePosition: null,
  diagnostics: [],
  artifacts: [],
  artifactReceipt: { status: "pending", failureMessage: null },
  localStorage: {
    status: "retained",
    outputBytes: 0,
    artifactBytes: 0,
    totalBytes: 0,
    removedAt: null,
  },
  receipt: {
    runId: "run-1" as never,
    status: "running",
    startedAt: "2026-08-12T00:00:00.000Z",
    finishedAt: null,
    exitCode: null,
    failureMessage: null,
    cancellationRequested: false,
    outputTruncated: false,
    outputByteLength: 6,
    outputContentHash: null,
    output: [
      {
        sequence: 0,
        stream: "stdout",
        text: "first\n",
        observedAt: "2026-08-12T00:00:01.000Z",
      },
    ],
  },
} satisfies AnalysisRunSnapshot;

describe("analysis run stream folding", () => {
  it("folds bounded output deltas without duplicating replayed chunks", () => {
    const snapshot = applyAnalysisRunStreamEvent(new Map(), {
      _tag: "run-snapshot",
      eventSequence: 1,
      run,
    });
    const outputEvent = {
      _tag: "run-output",
      eventSequence: 2,
      projectId: run.projectId,
      runId: run.receipt.runId,
      source: run.source,
      chunks: [
        run.receipt.output[0]!,
        {
          sequence: 1,
          stream: "stderr",
          text: "second\n",
          observedAt: "2026-08-12T00:00:02.000Z",
        },
      ],
      outputTruncated: true,
      outputByteLength: 13,
    } satisfies AnalysisRunStreamEvent;

    const folded = applyAnalysisRunStreamEvent(snapshot, outputEvent);
    expect(folded.get(run.receipt.runId)?.receipt.output).toHaveLength(2);
    expect(folded.get(run.receipt.runId)?.receipt.output[1]?.text).toBe("second\n");
    expect(folded.get(run.receipt.runId)?.receipt.outputTruncated).toBe(true);
    expect(folded.get(run.receipt.runId)?.receipt.outputByteLength).toBe(13);
  });

  it("applies summary updates without resending or discarding live output", () => {
    const snapshot = applyAnalysisRunStreamEvent(new Map(), {
      _tag: "run-snapshot",
      eventSequence: 1,
      run,
    });
    const { output: _output, ...summaryReceipt } = run.receipt;
    const folded = applyAnalysisRunStreamEvent(snapshot, {
      _tag: "run-updated",
      eventSequence: 2,
      run: {
        ...run,
        receipt: { ...summaryReceipt, status: "succeeded", finishedAt: "2026-08-12T00:00:03.000Z" },
      },
    });
    expect(folded.get(run.receipt.runId)?.receipt.status).toBe("succeeded");
    expect(folded.get(run.receipt.runId)?.receipt.output).toEqual(run.receipt.output);
  });

  it("ignores an output delta until its snapshot is known", () => {
    const current = new Map<string, AnalysisRunSnapshot>();
    const folded = applyAnalysisRunStreamEvent(current, {
      _tag: "run-output",
      eventSequence: 1,
      projectId: run.projectId,
      runId: run.receipt.runId,
      source: run.source,
      chunks: [],
      outputTruncated: false,
      outputByteLength: 0,
    });
    expect(folded.size).toBe(0);
    expect(folded).toBe(current);
  });

  it("appends ordered output without rebuilding the existing transcript", () => {
    const snapshot = applyAnalysisRunStreamEvent(new Map(), {
      _tag: "run-snapshot",
      eventSequence: 1,
      run,
    });
    const originalOutput = snapshot.get(run.receipt.runId)!.receipt.output;
    const folded = applyAnalysisRunStreamEvent(snapshot, {
      _tag: "run-output",
      eventSequence: 2,
      projectId: run.projectId,
      runId: run.receipt.runId,
      source: run.source,
      chunks: [
        {
          sequence: 1,
          stream: "stdout",
          text: "second\n",
          observedAt: "2026-08-12T00:00:02.000Z",
        },
      ],
      outputTruncated: false,
      outputByteLength: 13,
    });

    const output = folded.get(run.receipt.runId)!.receipt.output;
    expect(output.slice(0, originalOutput.length)).toEqual(originalOutput);
    expect(output.map((chunk) => chunk.sequence)).toEqual([0, 1]);
  });

  it("merges a rare out-of-order delta and ignores a pure replay", () => {
    const withGap = {
      ...run,
      receipt: {
        ...run.receipt,
        output: [
          run.receipt.output[0]!,
          { ...run.receipt.output[0]!, sequence: 2, text: "third\n" },
        ],
      },
    } satisfies AnalysisRunSnapshot;
    const snapshot = applyAnalysisRunStreamEvent(new Map(), {
      _tag: "run-snapshot",
      eventSequence: 1,
      run: withGap,
    });
    const folded = applyAnalysisRunStreamEvent(snapshot, {
      _tag: "run-output",
      eventSequence: 2,
      projectId: run.projectId,
      runId: run.receipt.runId,
      source: run.source,
      chunks: [{ ...run.receipt.output[0]!, sequence: 1, text: "second\n" }],
      outputTruncated: false,
      outputByteLength: 19,
    });
    expect(folded.get(run.receipt.runId)?.receipt.output.map((chunk) => chunk.sequence)).toEqual([
      0, 1, 2,
    ]);

    const replayed = applyAnalysisRunStreamEvent(folded, {
      _tag: "run-output",
      eventSequence: 3,
      projectId: run.projectId,
      runId: run.receipt.runId,
      source: run.source,
      chunks: folded.get(run.receipt.runId)!.receipt.output,
      outputTruncated: false,
      outputByteLength: 19,
    });
    expect(replayed).toBe(folded);
  });

  it("bounds completed live projections while preserving active runs", () => {
    let folded: ReadonlyMap<string, AnalysisRunSnapshot> = new Map();
    for (let index = 0; index < 40; index += 1) {
      const terminal = {
        ...run,
        receipt: {
          ...run.receipt,
          runId: `run-${String(index).padStart(2, "0")}` as never,
          status: "succeeded" as const,
          startedAt: `2026-08-12T00:00:${String(index).padStart(2, "0")}.000Z`,
          finishedAt: "2026-08-12T00:01:00.000Z",
          exitCode: 0,
        },
      } satisfies AnalysisRunSnapshot;
      folded = applyAnalysisRunStreamEvent(folded, {
        _tag: "run-snapshot",
        eventSequence: index,
        run: terminal,
      });
    }
    folded = applyAnalysisRunStreamEvent(folded, {
      _tag: "run-snapshot",
      eventSequence: 41,
      run: { ...run, receipt: { ...run.receipt, runId: "active" as never } },
    });

    expect(folded.size).toBe(33);
    expect(folded.has("active")).toBe(true);
    expect(folded.has("run-00")).toBe(false);
    expect(folded.has("run-39")).toBe(true);
  });
});
