// @effect-diagnostics nodeBuiltinImport:off -- static audit for the inherited viewer seam.
import * as NodeFS from "node:fs";

import { describe, expect, it } from "@effect/vitest";

describe("Scient file surface seams", () => {
  it("keeps additive Scient behavior mounted without leaking runtime logic into the viewer", () => {
    const source = NodeFS.readFileSync(
      new URL("../../components/files/FilePreviewPanel.tsx", import.meta.url),
      "utf8",
    );
    expect(source).toContain("ScientFileAuxiliarySurface");
    expect(source.match(/<ScientFileAuxiliarySurface/gu)).toHaveLength(1);
    expect(source.match(/useWorkspaceFileRefresh\(/gu)).toHaveLength(1);
    expect(source).toContain("ScientFileReloadButton");
    expect(source).toContain("ScientFileFreshnessNotices");
    expect(source).toContain("viewerRefreshKey");
    expect(source).not.toMatch(/matlab|-batch|AnalysisRunFilePanel/iu);
  });
});
