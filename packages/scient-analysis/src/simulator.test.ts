import { describe, expect, it } from "@effect/vitest";
import { ExecutionRunId } from "@scientfactory/execution";
import {
  AnalysisRuntimeId,
  AnalysisSourceRevision,
  createSimulatedAnalysisAdapter,
  summarizeAnalysisRun,
  type AnalysisRunSnapshot,
} from "./index.ts";

describe("analysis runtime adapter", () => {
  it("prepares a runtime-neutral file execution command", async () => {
    const profile = {
      id: AnalysisRuntimeId.make("simulator"),
      kind: "simulator",
      label: "Simulator",
      availability: "available",
      source: "custom",
      executablePath: "/tmp/simulator",
      version: "1",
      detail: null,
      capabilities: ["run-file", "stream-output", "cancel-process-tree"] as const,
      inspectedAt: "2026-08-12T00:00:00.000Z",
      verification: null,
    } as const;
    const adapter = createSimulatedAnalysisAdapter({ profile, command: ["sim", "--run"] });

    await expect(
      adapter.inspect({
        inspectedAt: "2026-08-12T00:00:00.000Z",
        platform: "linux",
        environment: {},
      }),
    ).resolves.toEqual(profile);
    expect(
      adapter.prepare({
        runId: "run-1" as never,
        projectId: "project-1",
        runtime: profile,
        source: { cwd: "/project", relativePath: "analysis.m", revision: "sha256:1" as never },
        absoluteSourcePath: "/project/analysis.m",
        artifactStagingDirectory: "/state/analysis/run-1/artifact-staging",
      }),
    ).toEqual({ executable: "sim", args: ["--run"], cwd: "/project", environment: {} });
  });

  it("keeps full runtime provenance canonical without repeating toolbox inventories in summaries", () => {
    const run = {
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
        capabilities: ["run-file"],
        inspectedAt: "2026-08-13T00:00:00.000Z",
        verification: {
          status: "ready",
          verifiedAt: "2026-08-13T00:00:00.000Z",
          durationMs: 1,
          executableIdentity: "matlab-identity",
          release: "R2026a",
          version: "26.1",
          architecture: "maca64",
          installationRoot: "/Applications/MATLAB_R2026a.app",
          javaAvailable: true,
          javaVersion: "Java 21",
          toolboxes: [{ name: "MATLAB", version: "26.1" }],
          detail: "MATLAB is ready.",
        },
      },
      source: {
        cwd: "/project",
        relativePath: "analysis.m",
        revision: AnalysisSourceRevision.make("sha256:source"),
      },
      phase: "finished",
      queuePosition: null,
      diagnostics: [],
      artifacts: [],
      artifactReceipt: { status: "succeeded", failureMessage: null },
      localStorage: {
        status: "retained",
        outputBytes: 0,
        artifactBytes: 0,
        totalBytes: 0,
        removedAt: null,
      },
      receipt: {
        runId: ExecutionRunId.make("run-1"),
        status: "succeeded",
        startedAt: "2026-08-13T00:00:00.000Z",
        finishedAt: "2026-08-13T00:00:01.000Z",
        exitCode: 0,
        failureMessage: null,
        cancellationRequested: false,
        outputTruncated: false,
        outputByteLength: 0,
        outputContentHash: null,
        output: [],
      },
    } satisfies AnalysisRunSnapshot;

    expect(summarizeAnalysisRun(run).runtime.verification?.toolboxes).toEqual([]);
    expect(run.runtime.verification.toolboxes).toEqual([{ name: "MATLAB", version: "26.1" }]);
  });
});
