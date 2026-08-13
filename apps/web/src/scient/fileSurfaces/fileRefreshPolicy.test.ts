import type { ProjectReadFileResult } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { hasExternalFileConflict } from "./fileRefreshPolicy";

function file(contents: string, revision: string): ProjectReadFileResult {
  return {
    relativePath: "analysis.m",
    contents,
    byteLength: contents.length,
    truncated: false,
    revision,
  };
}

describe("hasExternalFileConflict", () => {
  it("flags an external revision without discarding the pending local buffer", () => {
    expect(
      hasExternalFileConflict({
        authoritative: file("agent edit", "revision-2"),
        optimistic: file("local edit", "revision-1"),
        pending: true,
      }),
    ).toBe(true);
  });

  it("ignores Scient's own watched save when the contents agree", () => {
    expect(
      hasExternalFileConflict({
        authoritative: file("local edit", "revision-2"),
        optimistic: file("local edit", "revision-1"),
        pending: true,
      }),
    ).toBe(false);
  });

  it("ignores an earlier own save while a newer local edit is pending", () => {
    expect(
      hasExternalFileConflict({
        authoritative: file("first local edit", "revision-2"),
        optimistic: file("newer local edit", "revision-1"),
        lastConfirmedSave: { contents: "first local edit", revision: "revision-2" },
        pending: true,
      }),
    ).toBe(false);
  });

  it("still flags a later external edit after an own save", () => {
    expect(
      hasExternalFileConflict({
        authoritative: file("agent edit", "revision-3"),
        optimistic: file("newer local edit", "revision-1"),
        lastConfirmedSave: { contents: "first local edit", revision: "revision-2" },
        pending: true,
      }),
    ).toBe(true);
  });

  it("never reports a conflict without a pending local edit", () => {
    expect(
      hasExternalFileConflict({
        authoritative: file("agent edit", "revision-2"),
        optimistic: file("old contents", "revision-1"),
        pending: false,
      }),
    ).toBe(false);
  });
});
