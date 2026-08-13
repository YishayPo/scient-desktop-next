// @effect-diagnostics nodeBuiltinImport:off -- static audit for the inherited preview seam.
import * as NodeFS from "node:fs";

import { describe, expect, it } from "@effect/vitest";

const readSource = (relativePath: string) =>
  NodeFS.readFileSync(new URL(relativePath, import.meta.url), "utf8");

describe("analysis preview mini-player seam", () => {
  it("keeps the shared floating preview generic while preserving its reusable chrome", () => {
    const componentSource = readSource("../../components/preview/ThreadPreviewMiniPlayer.tsx");
    const layoutSource = readSource("../../components/preview/previewMiniPlayerLayout.ts");
    const storeSource = readSource("../../previewMiniPlayerStore.ts");
    const sharedSources = [componentSource, layoutSource, storeSource].join("\n");

    expect(sharedSources).not.toMatch(
      /~\/scient\/|@scientfactory\/analysis|matlab|analysis-artifact|AnalysisArtifact|figure-\d/iu,
    );
    expect(componentSource.match(/<BrowserSurfaceSlot/gu)).toHaveLength(1);
    expect(componentSource).toContain("RESIZE_HANDLES.map");
    expect(componentSource).toContain("setRect(threadRef, tabId");
    expect(layoutSource).toContain("PreviewMiniPlayerResizeDirection");
  });
});
