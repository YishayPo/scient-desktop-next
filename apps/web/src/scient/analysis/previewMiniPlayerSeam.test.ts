// @effect-diagnostics nodeBuiltinImport:off -- static audit for the inherited preview seam.
import * as NodeFS from "node:fs";

import { describe, expect, it } from "@effect/vitest";

const readSource = (relativePath: string) =>
  NodeFS.readFileSync(new URL(relativePath, import.meta.url), "utf8");

describe("analysis preview mini-player seam", () => {
  it("keeps the shared floating preview generic while preserving its reusable chrome", () => {
    const componentSource = readSource("../../components/preview/ThreadPreviewMiniPlayer.tsx");
    const imageSurfaceSource = readSource("../../components/preview/PreviewImageSurface.tsx");
    const layoutSource = readSource("../../components/preview/previewMiniPlayerLayout.ts");
    const hostStyleSource = readSource("../../browser/hostedBrowserWebviewStyle.ts");
    const storeSource = readSource("../../previewMiniPlayerStore.ts");
    const imageStoreSource = readSource("../../previewImageSurfaceStore.ts");
    const sharedSources = [
      componentSource,
      imageSurfaceSource,
      layoutSource,
      storeSource,
      imageStoreSource,
    ].join("\n");

    expect(sharedSources).not.toMatch(
      /~\/scient\/|@scientfactory\/analysis|matlab|analysis-artifact|AnalysisArtifact|figure-\d/iu,
    );
    expect(componentSource.match(/<BrowserSurfaceSlot/gu)).toHaveLength(1);
    expect(componentSource).toContain("createPortal(");
    expect(componentSource).toContain('className="pointer-events-none fixed z-[29]');
    expect(componentSource).toContain("window.requestAnimationFrame");
    expect(imageSurfaceSource).toContain('addEventListener("wheel"');
    expect(imageSurfaceSource).toContain("overflow-auto overscroll-contain bg-background");
    expect(imageSurfaceSource).toContain("Loading figure…");
    expect(imageSurfaceSource).toContain("window.setTimeout");
    expect(imageSurfaceSource).not.toContain('title="Pinch with two fingers to zoom"');
    expect(imageSurfaceSource.match(/Pinch with two fingers to zoom/gu)).toHaveLength(1);
    expect(hostStyleSource).toContain("zIndex: 30");
    expect(storeSource).toContain("PreviewMiniPlayerImageSource");
    expect(componentSource).not.toContain("bottomInset");
    expect(componentSource).toContain("RESIZE_HANDLES.map");
    expect(componentSource).toContain("setRect(threadRef, tabId");
    expect(componentSource).toContain("!imageSource ? (");
    expect(layoutSource).toContain("PreviewMiniPlayerResizeDirection");
  });
});
