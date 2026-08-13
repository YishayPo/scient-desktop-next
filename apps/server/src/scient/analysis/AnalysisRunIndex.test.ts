import {
  AnalysisRuntimeId,
  AnalysisSourceRevision,
  type AnalysisRunSummary,
} from "@scientfactory/analysis";
import { ExecutionRunId } from "@scientfactory/execution";
import { describe, expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { AnalysisRunIndex, layer } from "./AnalysisRunIndex.ts";

function summary(index: number, relativePath = "analysis.m"): AnalysisRunSummary {
  const timestamp = DateTime.formatIso(
    DateTime.add(DateTime.makeUnsafe("2026-08-12T00:00:00.000Z"), { milliseconds: index }),
  );
  return {
    contractVersion: 1,
    projectId: "project-1",
    action: "run-file",
    runtime: {
      id: AnalysisRuntimeId.make("matlab:local"),
      kind: "matlab",
      label: "MATLAB",
      availability: "available",
      source: "custom",
      executablePath: "/opt/matlab/bin/matlab",
      version: "R2026a",
      detail: null,
      capabilities: ["run-file", "stream-output", "cancel-process-tree"],
      inspectedAt: timestamp,
      verification: null,
    },
    source: {
      cwd: "/project",
      relativePath,
      revision: AnalysisSourceRevision.make(`sha256:${index}`),
    },
    phase: "finished",
    queuePosition: null,
    diagnostics: [],
    artifacts: [],
    artifactReceipt: { status: "not-requested", failureMessage: null },
    localStorage: {
      status: "retained",
      outputBytes: 0,
      artifactBytes: 0,
      totalBytes: 0,
      removedAt: null,
    },
    receipt: {
      runId: ExecutionRunId.make(`run-${String(index).padStart(5, "0")}`),
      status: "succeeded",
      startedAt: timestamp,
      finishedAt: timestamp,
      exitCode: 0,
      failureMessage: null,
      cancellationRequested: false,
      outputTruncated: false,
      outputByteLength: 0,
      outputContentHash: null,
    },
  };
}

const indexLayer = layer.pipe(Layer.provide(SqlitePersistenceMemory));

describe("AnalysisRunIndex", () => {
  it.effect("rebuilds a 10,000-run projection and pages by a stable opaque cursor", () =>
    Effect.gen(function* () {
      const index = yield* AnalysisRunIndex;
      const runs = Array.from({ length: 10_000 }, (_, runIndex) =>
        summary(runIndex, runIndex % 2 === 0 ? "analysis.m" : "other.m"),
      );
      const revision = yield* index.beginMutation("project-1");
      expect(yield* index.replaceProject("project-1", runs, revision)).toBe(true);
      expect(yield* index.needsRebuild("project-1")).toBe(false);

      const first = yield* index.list({
        projectId: "project-1",
        relativePath: "analysis.m",
        limit: 100,
      });
      expect(first.runs).toHaveLength(100);
      expect(first.hasMore).toBe(true);
      expect(first.nextCursor).not.toBeNull();
      expect(first.runs[0]?.receipt.runId).toBe("run-09998");

      const second = yield* index.list({
        projectId: "project-1",
        relativePath: "analysis.m",
        limit: 100,
        cursor: first.nextCursor!,
      });
      expect(second.runs).toHaveLength(100);
      expect(second.runs[0]?.receipt.runId).toBe("run-09798");
      expect(new Set([...first.runs, ...second.runs].map((run) => run.receipt.runId)).size).toBe(
        200,
      );
    }).pipe(Effect.provide(indexLayer)),
  );

  it.effect("replaces stale projection rows and rejects malformed cursors", () =>
    Effect.gen(function* () {
      const index = yield* AnalysisRunIndex;
      const firstRevision = yield* index.beginMutation("project-1");
      yield* index.replaceProject("project-1", [summary(1), summary(2)], firstRevision);
      const secondRevision = yield* index.beginMutation("project-1");
      yield* index.replaceProject("project-1", [summary(3)], secondRevision);
      const page = yield* index.list({ projectId: "project-1", limit: 20 });
      expect(page.runs.map((run) => run.receipt.runId)).toEqual(["run-00003"]);

      const malformed = yield* index
        .list({ projectId: "project-1", limit: 20, cursor: "not-a-cursor" })
        .pipe(Effect.result);
      expect(malformed._tag).toBe("Failure");
      if (malformed._tag === "Failure") expect(malformed.failure.operation).toBe("decode-cursor");
    }).pipe(Effect.provide(indexLayer)),
  );

  it.effect("never marks an obsolete or incrementally incomplete projection generation clean", () =>
    Effect.gen(function* () {
      const index = yield* AnalysisRunIndex;
      const initial = yield* index.beginMutation("project-1");
      yield* index.replaceProject("project-1", [summary(0)], initial);
      const obsolete = yield* index.beginMutation("project-1");
      const current = yield* index.beginMutation("project-1");

      expect(yield* index.replaceProject("project-1", [summary(1)], obsolete)).toBe(false);
      expect(yield* index.needsRebuild("project-1")).toBe(true);
      yield* index.upsert(summary(2), current);
      expect(yield* index.needsRebuild("project-1")).toBe(true);
      const recovery = yield* index.beginMutation("project-1");
      expect(yield* index.replaceProject("project-1", [summary(0), summary(2)], recovery)).toBe(
        true,
      );
      expect(yield* index.needsRebuild("project-1")).toBe(false);
    }).pipe(Effect.provide(indexLayer)),
  );

  it.effect("accounts retained bytes without treating metadata-only receipts as disposable", () =>
    Effect.gen(function* () {
      const index = yield* AnalysisRunIndex;
      const retained = {
        ...summary(1),
        localStorage: {
          status: "retained" as const,
          outputBytes: 128,
          artifactBytes: 256,
          totalBytes: 384,
          removedAt: null,
        },
      };
      const metadataOnly = {
        ...summary(2),
        localStorage: {
          status: "metadata-only" as const,
          outputBytes: 0,
          artifactBytes: 0,
          totalBytes: 0,
          removedAt: "2026-08-12T00:00:03.000Z",
        },
      };
      const revision = yield* index.beginMutation("project-1");
      yield* index.replaceProject("project-1", [retained, metadataOnly], revision);

      expect(yield* index.storageSummary({ projectId: "project-1" })).toEqual({
        retainedRunCount: 1,
        metadataOnlyRunCount: 1,
        outputBytes: 128,
        artifactBytes: 256,
        totalBytes: 384,
      });
      expect(
        (yield* index.listRetainedTerminal("project-1")).map((run) => run.receipt.runId),
      ).toEqual([retained.receipt.runId]);
    }).pipe(Effect.provide(indexLayer)),
  );
});
