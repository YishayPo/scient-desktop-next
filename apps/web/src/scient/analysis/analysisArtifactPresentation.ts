import type {
  AnalysisArtifact,
  AnalysisArtifactRepresentation,
  AnalysisRunSnapshot,
  AnalysisRunSummary,
  AssetResource,
  PreviewSessionSnapshot,
} from "@t3tools/contracts";

type AnalysisRun = AnalysisRunSnapshot | AnalysisRunSummary;

const PREVIEW_ORDER = ["static-png", "static-svg", "static-pdf"];
const THUMBNAIL_ORDER = ["static-svg", "static-png"];

function representationByPreference(
  artifact: AnalysisArtifact,
  preference: ReadonlyArray<string>,
): AnalysisArtifactRepresentation | null {
  for (const representationId of preference) {
    const representation = artifact.representations.find(
      (candidate) => candidate.representationId === representationId,
    );
    if (representation) return representation;
  }
  return null;
}

export function preferredArtifactPreview(
  artifact: AnalysisArtifact,
): AnalysisArtifactRepresentation | null {
  return representationByPreference(artifact, PREVIEW_ORDER);
}

export function preferredArtifactThumbnail(
  artifact: AnalysisArtifact,
): AnalysisArtifactRepresentation | null {
  return representationByPreference(artifact, THUMBNAIL_ORDER);
}

export function interactiveArtifactRepresentation(
  artifact: AnalysisArtifact,
): AnalysisArtifactRepresentation | null {
  return (
    artifact.representations.find((candidate) => candidate.presentation === "interactive") ?? null
  );
}

export function nativeArtifactRepresentation(
  artifact: AnalysisArtifact,
): AnalysisArtifactRepresentation | null {
  return artifact.representations.find((candidate) => candidate.presentation === "native") ?? null;
}

export function isImageArtifactRepresentation(
  representation: AnalysisArtifactRepresentation,
): boolean {
  return representation.mediaType === "image/png" || representation.mediaType === "image/svg+xml";
}

export function analysisArtifactResource(
  run: AnalysisRun,
  artifact: AnalysisArtifact,
  representation: AnalysisArtifactRepresentation,
): AssetResource {
  return {
    _tag: "analysis-artifact",
    projectId: run.projectId,
    runId: run.receipt.runId,
    artifactId: artifact.artifactId,
    representationId: representation.representationId,
  };
}

/** Keep the selected run honest while retaining the latest successful visual during a rerun. */
export function runForArtifactDisplay(
  runs: ReadonlyArray<AnalysisRun>,
  selectedRun: AnalysisRun | null,
): AnalysisRun | null {
  if (selectedRun && selectedRun.artifacts.length > 0) return selectedRun;
  return runs.find((run) => run.receipt.status === "succeeded" && run.artifacts.length > 0) ?? null;
}

export function artifactDisplayStatus(input: {
  readonly artifactRun: AnalysisRun;
  readonly selectedRun: AnalysisRun | null;
  readonly activeRun: AnalysisRun | null;
  readonly sourceRevision: string;
}): "current" | "updating" | "stale" | "partial" | "failed-latest" {
  if (input.activeRun) return "updating";
  if (input.artifactRun.source.revision !== input.sourceRevision) return "stale";
  if (input.artifactRun.receipt.status === "failed") return "partial";
  if (
    input.selectedRun?.receipt.status === "failed" &&
    input.selectedRun.receipt.runId !== input.artifactRun.receipt.runId
  ) {
    return "failed-latest";
  }
  return "current";
}

/** Stop following as soon as the tab closes or the user navigates it away from our artifact. */
export function canFollowArtifactInTab(
  snapshot: PreviewSessionSnapshot | undefined,
  lastArtifactUrl: string,
): boolean {
  return (
    snapshot !== undefined &&
    snapshot.navStatus._tag !== "Idle" &&
    snapshot.navStatus.url === lastArtifactUrl
  );
}

/** Center a newly floated figure on the point where the user dropped its thumbnail. */
export function floatingArtifactPositionForDrop(input: {
  readonly clientPoint: { readonly x: number; readonly y: number };
  readonly playerSize: { readonly width: number; readonly height: number };
}): { readonly x: number; readonly y: number } {
  return {
    x: Math.round(input.clientPoint.x - input.playerSize.width / 2),
    y: Math.round(input.clientPoint.y - input.playerSize.height / 2),
  };
}
