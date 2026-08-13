import {
  AnalysisArtifactContentHash,
  AnalysisArtifactFileName,
  AnalysisArtifactId,
  AnalysisArtifactRepresentationId,
  AnalysisRuntimeId,
  AnalysisSourceRevision,
  type AnalysisArtifact,
  type AnalysisArtifactMediaType,
  type AnalysisArtifactPresentation,
  type AnalysisRunSummary,
  type PreviewSessionSnapshot,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";

import {
  analysisArtifactResource,
  artifactDisplayStatus,
  canFollowArtifactInTab,
  floatingArtifactPositionForDrop,
  interactiveArtifactRepresentation,
  isImageArtifactRepresentation,
  nativeArtifactRepresentation,
  preferredArtifactPreview,
  preferredArtifactThumbnail,
  runForArtifactDisplay,
} from "./analysisArtifactPresentation";

function representation(input: {
  id: string;
  mediaType: AnalysisArtifactMediaType;
  presentation: AnalysisArtifactPresentation;
}) {
  return {
    representationId: AnalysisArtifactRepresentationId.make(input.id),
    fileName: AnalysisArtifactFileName.make(`figure-001.${input.id}`),
    mediaType: input.mediaType,
    presentation: input.presentation,
    requiresNetworkForFullExperience: input.presentation === "interactive",
    contentHash: AnalysisArtifactContentHash.make(`sha256:${"0".repeat(64)}`),
    byteLength: 10,
  };
}

const artifact = {
  artifactId: AnalysisArtifactId.make("figure-001"),
  kind: "figure",
  label: "Figure 1",
  createdAt: "2026-08-12T00:00:02.000Z",
  representations: [
    representation({ id: "static-png", mediaType: "image/png", presentation: "static" }),
    representation({ id: "static-svg", mediaType: "image/svg+xml", presentation: "static" }),
    representation({ id: "interactive-html", mediaType: "text/html", presentation: "interactive" }),
    representation({
      id: "matlab-figure",
      mediaType: "application/vnd.mathworks.matlab.figure",
      presentation: "native",
    }),
  ],
} satisfies AnalysisArtifact;

function run(input: {
  id: string;
  status: AnalysisRunSummary["receipt"]["status"];
  revision?: string;
  artifacts?: ReadonlyArray<AnalysisArtifact>;
  startedAt?: string;
}): AnalysisRunSummary {
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
      executablePath: "/Applications/MATLAB.app/bin/matlab",
      version: "R2026a",
      detail: null,
      capabilities: ["run-file", "stream-output", "cancel-process-tree", "capture-artifacts"],
      inspectedAt: "2026-08-12T00:00:00.000Z",
      verification: null,
    },
    source: {
      cwd: "/project",
      relativePath: "analysis.m",
      revision: AnalysisSourceRevision.make(input.revision ?? "sha256:current"),
    },
    phase: "finished",
    queuePosition: null,
    diagnostics: [],
    artifacts: input.artifacts ?? [],
    artifactReceipt: { status: "succeeded", failureMessage: null },
    localStorage: {
      status: "retained",
      outputBytes: 0,
      artifactBytes: 0,
      totalBytes: 0,
      removedAt: null,
    },
    receipt: {
      runId: input.id as AnalysisRunSummary["receipt"]["runId"],
      status: input.status,
      startedAt: input.startedAt ?? "2026-08-12T00:00:00.000Z",
      finishedAt: input.status === "running" ? null : "2026-08-12T00:00:03.000Z",
      exitCode: input.status === "succeeded" ? 0 : null,
      failureMessage: input.status === "failed" ? "failed" : null,
      cancellationRequested: false,
      outputTruncated: false,
      outputByteLength: 0,
      outputContentHash: null,
    },
  };
}

describe("analysis artifact presentation", () => {
  it("prefers a reliable static view and keeps richer formats explicit", () => {
    expect(preferredArtifactPreview(artifact)?.representationId).toBe("static-png");
    expect(preferredArtifactThumbnail(artifact)?.representationId).toBe("static-svg");
    expect(interactiveArtifactRepresentation(artifact)?.representationId).toBe("interactive-html");
    expect(nativeArtifactRepresentation(artifact)?.representationId).toBe("matlab-figure");
    expect(isImageArtifactRepresentation(artifact.representations[0]!)).toBe(true);
    expect(isImageArtifactRepresentation(artifact.representations[1]!)).toBe(true);
    expect(isImageArtifactRepresentation(artifact.representations[2]!)).toBe(false);
  });

  it("creates a stable producer-owned asset reference", () => {
    expect(
      analysisArtifactResource(
        run({ id: "run-1", status: "succeeded", artifacts: [artifact] }),
        artifact,
        artifact.representations[0]!,
      ),
    ).toEqual({
      _tag: "analysis-artifact",
      projectId: "project-1",
      runId: "run-1",
      artifactId: "figure-001",
      representationId: "static-png",
    });
  });

  it("keeps the last good figure visible while a new run is active or fails", () => {
    const lastGood = run({ id: "run-good", status: "succeeded", artifacts: [artifact] });
    const running = run({ id: "run-active", status: "running" });
    const failed = run({ id: "run-failed", status: "failed" });

    expect(runForArtifactDisplay([running, lastGood], running)).toBe(lastGood);
    expect(
      artifactDisplayStatus({
        artifactRun: lastGood,
        selectedRun: running,
        activeRun: running,
        sourceRevision: "sha256:current",
      }),
    ).toBe("updating");
    expect(runForArtifactDisplay([failed, lastGood], failed)).toBe(lastGood);
    expect(
      artifactDisplayStatus({
        artifactRun: lastGood,
        selectedRun: failed,
        activeRun: null,
        sourceRevision: "sha256:current",
      }),
    ).toBe("failed-latest");
  });

  it("distinguishes stale and partial artifact receipts", () => {
    const stale = run({
      id: "run-stale",
      status: "succeeded",
      revision: "sha256:old",
      artifacts: [artifact],
    });
    expect(
      artifactDisplayStatus({
        artifactRun: stale,
        selectedRun: stale,
        activeRun: null,
        sourceRevision: "sha256:current",
      }),
    ).toBe("stale");

    const partial = run({ id: "run-partial", status: "failed", artifacts: [artifact] });
    expect(runForArtifactDisplay([partial], partial)).toBe(partial);
    expect(
      artifactDisplayStatus({
        artifactRun: partial,
        selectedRun: partial,
        activeRun: null,
        sourceRevision: "sha256:current",
      }),
    ).toBe("partial");
  });

  it("follows a figure tab only while it remains on the URL Scient opened", () => {
    const lastArtifactUrl = "http://127.0.0.1/api/assets/token/figure-001.html";
    const snapshot = {
      threadId: "thread-1",
      tabId: "tab-1",
      navStatus: { _tag: "Success", url: lastArtifactUrl, title: "Figure 1" },
      canGoBack: false,
      canGoForward: false,
      updatedAt: "2026-08-12T00:00:00.000Z",
    } as PreviewSessionSnapshot;

    expect(canFollowArtifactInTab(snapshot, lastArtifactUrl)).toBe(true);
    expect(
      canFollowArtifactInTab(
        {
          ...snapshot,
          navStatus: { _tag: "Success", url: "https://example.com", title: "Elsewhere" },
        },
        lastArtifactUrl,
      ),
    ).toBe(false);
    expect(canFollowArtifactInTab(undefined, lastArtifactUrl)).toBe(false);
  });

  it("centers a floated figure on its drop point in the app window", () => {
    expect(
      floatingArtifactPositionForDrop({
        clientPoint: { x: 620, y: 390 },
        playerSize: { width: 320, height: 200 },
      }),
    ).toEqual({ x: 460, y: 290 });
  });
});
