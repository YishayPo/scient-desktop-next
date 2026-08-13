import type { EnvironmentId, ScopedThreadRef } from "@t3tools/contracts";

import { AnalysisRunFilePanel } from "~/scient/analysis/AnalysisRunFilePanel";

interface ScientFileAuxiliarySurfaceProps {
  readonly environmentId: EnvironmentId;
  readonly threadRef: ScopedThreadRef;
  readonly cwd: string;
  readonly relativePath: string | null;
  readonly sourceRevision: string | null;
  readonly sourcePending: boolean;
  readonly truncated: boolean;
}

/**
 * Stable Scient-owned extension point beneath the inherited source viewer.
 * Format-specific surfaces belong here so upstream viewer updates stay isolated.
 */
export function ScientFileAuxiliarySurface(props: ScientFileAuxiliarySurfaceProps) {
  if (
    props.relativePath === null ||
    props.sourceRevision === null ||
    props.truncated ||
    !props.relativePath.toLowerCase().endsWith(".m")
  ) {
    return null;
  }

  return (
    <AnalysisRunFilePanel
      key={`${props.environmentId}:${props.cwd}:${props.relativePath}`}
      environmentId={props.environmentId}
      threadRef={props.threadRef}
      cwd={props.cwd}
      relativePath={props.relativePath}
      sourceRevision={props.sourceRevision}
      sourcePending={props.sourcePending}
      runtimeKind="matlab"
      runtimeLabel="MATLAB"
    />
  );
}
