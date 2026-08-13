import type { ProjectReadFileResult } from "@t3tools/contracts";
import { EnvironmentId } from "@t3tools/contracts";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import {
  clearProjectFileQueryData,
  confirmProjectFileQueryData,
  getOptimisticProjectFileQueryData,
  resolveProjectFileQueryData,
  setProjectFileQueryData,
} from "./projectFilesQueryState";

const environmentId = EnvironmentId.make("environment-project-files-query-test");

describe("project files queries", () => {
  afterEach(() => {
    clearProjectFileQueryData(environmentId, "/repo", "convex.json");
    vi.unstubAllGlobals();
  });

  it("keeps the latest optimistic draft when an older write finishes", () => {
    vi.stubGlobal("window", {});
    const initial = {
      relativePath: "convex.json",
      contents: '{"nodeVersion":"20"}',
      byteLength: 20,
      truncated: false,
      revision: "revision-1",
    } satisfies ProjectReadFileResult;
    setProjectFileQueryData(
      environmentId,
      "/repo",
      "convex.json",
      '{"nodeVersion":"220"}',
      initial.revision,
    );
    setProjectFileQueryData(environmentId, "/repo", "convex.json", '{"nodeVersion":"22"}');

    expect(getOptimisticProjectFileQueryData(environmentId, "/repo", "convex.json")?.contents).toBe(
      '{"nodeVersion":"22"}',
    );

    expect(
      confirmProjectFileQueryData(
        environmentId,
        "/repo",
        "convex.json",
        '{"nodeVersion":"220"}',
        "revision-2",
      ),
    ).toBe(false);

    expect(resolveProjectFileQueryData(environmentId, "/repo", "convex.json", initial)).toEqual({
      relativePath: "convex.json",
      contents: '{"nodeVersion":"22"}',
      byteLength: 20,
      truncated: false,
      revision: "revision-1",
    });

    expect(
      confirmProjectFileQueryData(
        environmentId,
        "/repo",
        "convex.json",
        '{"nodeVersion":"22"}',
        "revision-2",
      ),
    ).toBe(true);
  });

  it("reveals authoritative contents after a local draft is discarded", () => {
    const authoritative = {
      relativePath: "convex.json",
      contents: '{"nodeVersion":"22"}',
      byteLength: 20,
      truncated: false,
      revision: "revision-agent",
    } satisfies ProjectReadFileResult;
    setProjectFileQueryData(
      environmentId,
      "/repo",
      "convex.json",
      '{"nodeVersion":"local"}',
      "revision-before",
    );

    expect(
      resolveProjectFileQueryData(environmentId, "/repo", "convex.json", authoritative),
    ).not.toEqual(authoritative);

    clearProjectFileQueryData(environmentId, "/repo", "convex.json");

    expect(
      resolveProjectFileQueryData(environmentId, "/repo", "convex.json", authoritative),
    ).toEqual(authoritative);
  });
});
