import * as NodeServices from "@effect/platform-node/NodeServices";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { it, describe, expect } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../config.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as WorkspaceEntries from "./WorkspaceEntries.ts";
import * as WorkspaceFileSystem from "./WorkspaceFileSystem.ts";
import * as WorkspacePaths from "./WorkspacePaths.ts";

const ProjectLayer = WorkspaceFileSystem.layer.pipe(
  Layer.provide(WorkspacePaths.layer),
  Layer.provide(WorkspaceEntries.layer.pipe(Layer.provide(WorkspacePaths.layer))),
);

const TestLayer = Layer.empty.pipe(
  Layer.provideMerge(ProjectLayer),
  Layer.provideMerge(WorkspaceEntries.layer.pipe(Layer.provide(WorkspacePaths.layer))),
  Layer.provideMerge(WorkspacePaths.layer),
  Layer.provideMerge(VcsDriverRegistry.layer.pipe(Layer.provide(VcsProcess.layer))),
  Layer.provide(
    ServerConfig.ServerConfig.layerTest(process.cwd(), {
      prefix: "t3-workspace-files-test-",
    }),
  ),
  Layer.provideMerge(NodeServices.layer),
);

const makeTempDir = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  return yield* fileSystem.makeTempDirectoryScoped({
    prefix: "t3code-workspace-files-",
  });
});

const writeTextFile = Effect.fn("writeTextFile")(function* (
  cwd: string,
  relativePath: string,
  contents = "",
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const absolutePath = path.join(cwd, relativePath);
  yield* fileSystem
    .makeDirectory(path.dirname(absolutePath), { recursive: true })
    .pipe(Effect.orDie);
  yield* fileSystem.writeFileString(absolutePath, contents).pipe(Effect.orDie);
});

it.layer(TestLayer, { excludeTestServices: true })("WorkspaceFileSystemLive", (it) => {
  describe("readFile", () => {
    it.effect("reads UTF-8 files relative to the workspace root", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const cwd = yield* makeTempDir;
        yield* writeTextFile(cwd, "src/index.ts", "export const answer = 42;\n");

        const result = yield* workspaceFileSystem.readFile({
          cwd,
          relativePath: "src/index.ts",
        });

        expect(result).toEqual({
          relativePath: "src/index.ts",
          contents: "export const answer = 42;\n",
          byteLength: 26,
          truncated: false,
          revision: expect.stringMatching(/^sha256:[0-9a-f]{64}$/u),
        });
      }),
    );

    it.effect("rejects reads outside the workspace root", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const cwd = yield* makeTempDir;

        const error = yield* workspaceFileSystem
          .readFile({ cwd, relativePath: "../escape.md" })
          .pipe(Effect.flip);

        expect(error.message).toContain(
          "Workspace file path must be relative to the project root: ../escape.md",
        );
      }),
    );

    it.effect("rejects symlinks that resolve outside the workspace root", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const cwd = yield* makeTempDir;
        const outsideDir = yield* makeTempDir;
        yield* writeTextFile(outsideDir, "secret.txt", "outside\n");
        yield* fileSystem.symlink(
          path.join(outsideDir, "secret.txt"),
          path.join(cwd, "linked-secret.txt"),
        );

        const error = yield* workspaceFileSystem
          .readFile({ cwd, relativePath: "linked-secret.txt" })
          .pipe(Effect.flip);
        const resolvedWorkspaceRoot = yield* fileSystem.realPath(cwd);
        const resolvedPath = yield* fileSystem.realPath(path.join(outsideDir, "secret.txt"));

        expect(error).toBeInstanceOf(WorkspaceFileSystem.WorkspaceFilePathEscapeError);
        expect(error).toMatchObject({
          workspaceRoot: cwd,
          relativePath: "linked-secret.txt",
          resolvedWorkspaceRoot,
          resolvedPath,
        });
        expect("cause" in error).toBe(false);
      }),
    );

    it.effect("rejects directories without manufacturing an I/O cause", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const cwd = yield* makeTempDir;
        yield* fileSystem.makeDirectory(path.join(cwd, "src"));

        const error = yield* workspaceFileSystem
          .readFile({ cwd, relativePath: "src" })
          .pipe(Effect.flip);
        const resolvedPath = yield* fileSystem.realPath(path.join(cwd, "src"));

        expect(error).toBeInstanceOf(WorkspaceFileSystem.WorkspacePathNotFileError);
        expect(error).toMatchObject({
          workspaceRoot: cwd,
          relativePath: "src",
          resolvedPath,
        });
        expect("cause" in error).toBe(false);
      }),
    );

    it.effect("rejects binary files without leaking their contents into the error", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const cwd = yield* makeTempDir;
        const absolutePath = path.join(cwd, "asset.bin");
        yield* fileSystem.writeFile(absolutePath, Uint8Array.from([0x61, 0, 0x62]));

        const error = yield* workspaceFileSystem
          .readFile({ cwd, relativePath: "asset.bin" })
          .pipe(Effect.flip);
        const resolvedPath = yield* fileSystem.realPath(absolutePath);

        expect(error).toBeInstanceOf(WorkspaceFileSystem.WorkspaceBinaryFileError);
        expect(error).toMatchObject({
          workspaceRoot: cwd,
          relativePath: "asset.bin",
          resolvedPath,
        });
        expect("cause" in error).toBe(false);
        expect("contents" in error).toBe(false);
      }),
    );

    it.effect("keeps PDF bytes out of the text readFile path", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const cwd = yield* makeTempDir;
        yield* fileSystem.writeFile(
          path.join(cwd, "paper.pdf"),
          Uint8Array.from([0x25, 0x50, 0x44, 0x46, 0x2d, 0x31, 0x2e, 0x37, 0x0a, 0]),
        );

        const error = yield* workspaceFileSystem
          .readFile({ cwd, relativePath: "paper.pdf" })
          .pipe(Effect.flip);

        expect(error).toBeInstanceOf(WorkspaceFileSystem.WorkspaceBinaryFileError);
        expect("contents" in error).toBe(false);
      }),
    );

    it.effect("preserves the real cause and path for I/O failures", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const path = yield* Path.Path;
        const cwd = yield* makeTempDir;
        const resolvedPath = path.join(cwd, "missing.txt");

        const error = yield* workspaceFileSystem
          .readFile({ cwd, relativePath: "missing.txt" })
          .pipe(Effect.flip);

        expect(error).toBeInstanceOf(WorkspaceFileSystem.WorkspaceFileSystemOperationError);
        expect(error).toMatchObject({
          workspaceRoot: cwd,
          relativePath: "missing.txt",
          resolvedPath,
          operationPath: resolvedPath,
          operation: "realpath-target",
        });
        expect(error.cause).toBeInstanceOf(Error);
        expect((error.cause as NodeJS.ErrnoException).code).toBe("ENOENT");
      }),
    );
  });

  describe("watchFile", () => {
    it.effect("emits a coalesced hint only when the selected file changes", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const cwd = yield* makeTempDir;
        yield* writeTextFile(cwd, "analysis.m", "answer = 1;\n");
        yield* writeTextFile(cwd, "notes.txt", "unchanged\n");
        const opened = yield* workspaceFileSystem.readFile({
          cwd,
          relativePath: "analysis.m",
        });

        const event = yield* workspaceFileSystem
          .watchFile({ cwd, relativePath: "analysis.m" })
          .pipe(
            Stream.tap((event) =>
              event._tag === "watch-ready"
                ? Effect.gen(function* () {
                    yield* writeTextFile(cwd, "notes.txt", "unrelated\n");
                    yield* workspaceFileSystem.writeFile({
                      cwd,
                      relativePath: "analysis.m",
                      contents: "answer = 2;\n",
                      expectedRevision: opened.revision,
                    });
                  })
                : Effect.void,
            ),
            Stream.filter((event) => event._tag === "file-changed"),
            Stream.runHead,
          );

        expect(event).toEqual(Option.some({ _tag: "file-changed", relativePath: "analysis.m" }));
      }),
    );

    it.effect("emits a hint when the selected file is removed", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const cwd = yield* makeTempDir;
        yield* writeTextFile(cwd, "analysis.m", "answer = 1;\n");

        const event = yield* workspaceFileSystem
          .watchFile({ cwd, relativePath: "analysis.m" })
          .pipe(
            Stream.tap((event) =>
              event._tag === "watch-ready"
                ? fileSystem.remove(path.join(cwd, "analysis.m"))
                : Effect.void,
            ),
            Stream.filter((event) => event._tag === "file-changed"),
            Stream.runHead,
          );

        expect(event).toEqual(Option.some({ _tag: "file-changed", relativePath: "analysis.m" }));
      }),
    );

    it.effect("keeps watching a missing file so recreation can recover the viewer", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const cwd = yield* makeTempDir;

        const event = yield* workspaceFileSystem
          .watchFile({ cwd, relativePath: "analysis.m" })
          .pipe(
            Stream.tap((event) =>
              event._tag === "watch-ready"
                ? writeTextFile(cwd, "analysis.m", "answer = 2;\n")
                : Effect.void,
            ),
            Stream.filter((event) => event._tag === "file-changed"),
            Stream.runHead,
          );

        expect(event).toEqual(Option.some({ _tag: "file-changed", relativePath: "analysis.m" }));
      }),
    );

    it.effect("continues watching across deletion and recreation", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const cwd = yield* makeTempDir;
        const absolutePath = path.join(cwd, "analysis.m");
        yield* writeTextFile(cwd, "analysis.m", "answer = 1;\n");
        let observedChanges = 0;

        const events = yield* workspaceFileSystem
          .watchFile({ cwd, relativePath: "analysis.m" })
          .pipe(
            Stream.tap((event) => {
              if (event._tag === "watch-ready") return fileSystem.remove(absolutePath);
              observedChanges += 1;
              return observedChanges === 1
                ? writeTextFile(cwd, "analysis.m", "answer = 2;\n")
                : Effect.void;
            }),
            Stream.filter((event) => event._tag === "file-changed"),
            Stream.take(2),
            Stream.runCollect,
          );

        expect([...events]).toEqual([
          { _tag: "file-changed", relativePath: "analysis.m" },
          { _tag: "file-changed", relativePath: "analysis.m" },
        ]);
        expect(yield* fileSystem.readFileString(absolutePath)).toBe("answer = 2;\n");
      }),
    );

    it.effect("follows in-workspace file symlinks to their real target", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const cwd = yield* makeTempDir;
        yield* writeTextFile(cwd, "sources/analysis.m", "answer = 1;\n");
        yield* fileSystem.symlink(
          path.join(cwd, "sources/analysis.m"),
          path.join(cwd, "analysis.m"),
        );

        const event = yield* workspaceFileSystem
          .watchFile({ cwd, relativePath: "analysis.m" })
          .pipe(
            Stream.tap((event) =>
              event._tag === "watch-ready"
                ? writeTextFile(cwd, "sources/analysis.m", "answer = 2;\n")
                : Effect.void,
            ),
            Stream.filter((event) => event._tag === "file-changed"),
            Stream.runHead,
          );

        expect(event).toEqual(Option.some({ _tag: "file-changed", relativePath: "analysis.m" }));
      }),
    );

    it.effect("rejects watch paths and symlink targets outside the workspace", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const cwd = yield* makeTempDir;
        const outsideDir = yield* makeTempDir;
        yield* writeTextFile(outsideDir, "analysis.m", "answer = 1;\n");
        yield* fileSystem.symlink(
          path.join(outsideDir, "analysis.m"),
          path.join(cwd, "analysis.m"),
        );

        const lexicalEscape = yield* workspaceFileSystem
          .watchFile({ cwd, relativePath: "../analysis.m" })
          .pipe(Stream.runHead, Effect.flip);
        const symlinkEscape = yield* workspaceFileSystem
          .watchFile({ cwd, relativePath: "analysis.m" })
          .pipe(Stream.runHead, Effect.flip);

        expect(lexicalEscape.message).toContain(
          "Workspace file path must be relative to the project root: ../analysis.m",
        );
        expect(symlinkEscape).toBeInstanceOf(WorkspaceFileSystem.WorkspaceFilePathEscapeError);
      }),
    );
  });

  describe("writeFile", () => {
    it.effect("writes files relative to the workspace root", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const cwd = yield* makeTempDir;
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const result = yield* workspaceFileSystem.writeFile({
          cwd,
          relativePath: "plans/effect-rpc.md",
          contents: "# Plan\n",
        });
        const saved = yield* fileSystem
          .readFileString(path.join(cwd, "plans/effect-rpc.md"))
          .pipe(Effect.orDie);

        expect(result).toEqual({
          relativePath: "plans/effect-rpc.md",
          revision: expect.stringMatching(/^sha256:[0-9a-f]{64}$/u),
        });
        expect(saved).toBe("# Plan\n");
      }),
    );

    it.effect("invalidates workspace entry search cache after writes", () =>
      Effect.gen(function* () {
        const workspaceEntries = yield* WorkspaceEntries.WorkspaceEntries;
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const cwd = yield* makeTempDir;
        yield* writeTextFile(cwd, "src/existing.ts", "export {};\n");

        const beforeWrite = yield* workspaceEntries.list({ cwd });
        expect(beforeWrite.entries.some((entry) => entry.path === "plans/effect-rpc.md")).toBe(
          false,
        );

        yield* workspaceFileSystem.writeFile({
          cwd,
          relativePath: "plans/effect-rpc.md",
          contents: "# Plan\n",
        });

        const afterWrite = yield* workspaceEntries.list({ cwd });
        expect(afterWrite.entries).toEqual(
          expect.arrayContaining([expect.objectContaining({ path: "plans/effect-rpc.md" })]),
        );
        expect(afterWrite.truncated).toBe(false);
      }),
    );

    it.effect("rejects writes outside the workspace root", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const cwd = yield* makeTempDir;
        const path = yield* Path.Path;
        const fileSystem = yield* FileSystem.FileSystem;

        const error = yield* workspaceFileSystem
          .writeFile({
            cwd,
            relativePath: "../escape.md",
            contents: "# nope\n",
          })
          .pipe(Effect.flip);

        expect(error.message).toContain(
          "Workspace file path must be relative to the project root: ../escape.md",
        );

        const escapedPath = path.resolve(cwd, "..", "escape.md");
        const escapedStat = yield* fileSystem
          .stat(escapedPath)
          .pipe(Effect.orElseSucceed(() => null));
        expect(escapedStat).toBeNull();
      }),
    );

    it.effect("refuses to overwrite a file that changed after it was opened", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const cwd = yield* makeTempDir;
        yield* writeTextFile(cwd, "analysis.m", "answer = 1;\n");
        const opened = yield* workspaceFileSystem.readFile({ cwd, relativePath: "analysis.m" });

        yield* fileSystem.writeFileString(path.join(cwd, "analysis.m"), "answer = 2;\n");
        const error = yield* workspaceFileSystem
          .writeFile({
            cwd,
            relativePath: "analysis.m",
            contents: "answer = 3;\n",
            expectedRevision: opened.revision,
          })
          .pipe(Effect.flip);

        expect(error).toBeInstanceOf(WorkspaceFileSystem.WorkspaceFileRevisionConflictError);
        expect(error).toMatchObject({ relativePath: "analysis.m" });
        expect(yield* fileSystem.readFileString(path.join(cwd, "analysis.m"))).toBe(
          "answer = 2;\n",
        );
      }),
    );

    it.effect("returns the revision produced by a conditional atomic write", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const cwd = yield* makeTempDir;
        yield* writeTextFile(cwd, "analysis.m", "answer = 1;\n");
        const opened = yield* workspaceFileSystem.readFile({ cwd, relativePath: "analysis.m" });

        const written = yield* workspaceFileSystem.writeFile({
          cwd,
          relativePath: "analysis.m",
          contents: "answer = 2;\n",
          expectedRevision: opened.revision,
        });
        const reread = yield* workspaceFileSystem.readFile({ cwd, relativePath: "analysis.m" });

        expect(written.revision).toBe(reread.revision);
        expect(reread.contents).toBe("answer = 2;\n");
      }),
    );

    it.effect("allows only one concurrent write against the same source revision", () =>
      Effect.gen(function* () {
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const cwd = yield* makeTempDir;
        yield* writeTextFile(cwd, "analysis.m", "answer = 1;\n");
        const opened = yield* workspaceFileSystem.readFile({ cwd, relativePath: "analysis.m" });

        const results = yield* Effect.all(
          [
            workspaceFileSystem
              .writeFile({
                cwd,
                relativePath: "analysis.m",
                contents: "answer = 2;\n",
                expectedRevision: opened.revision,
              })
              .pipe(Effect.result),
            workspaceFileSystem
              .writeFile({
                cwd,
                relativePath: "analysis.m",
                contents: "answer = 3;\n",
                expectedRevision: opened.revision,
              })
              .pipe(Effect.result),
          ],
          { concurrency: 2 },
        );

        expect(results.filter((result) => result._tag === "Success")).toHaveLength(1);
        const failure = results.find((result) => result._tag === "Failure");
        expect(failure?._tag).toBe("Failure");
        if (failure?._tag === "Failure") {
          expect(failure.failure).toBeInstanceOf(
            WorkspaceFileSystem.WorkspaceFileRevisionConflictError,
          );
        }
      }),
    );

    it.effect("preserves executable permissions when atomically replacing a source file", () =>
      Effect.gen(function* () {
        if ((yield* HostProcessPlatform) === "win32") return;
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const cwd = yield* makeTempDir;
        const sourcePath = path.join(cwd, "analysis.m");
        yield* writeTextFile(cwd, "analysis.m", "answer = 1;\n");
        yield* fileSystem.chmod(sourcePath, 0o755);
        const opened = yield* workspaceFileSystem.readFile({ cwd, relativePath: "analysis.m" });

        yield* workspaceFileSystem.writeFile({
          cwd,
          relativePath: "analysis.m",
          contents: "answer = 2;\n",
          expectedRevision: opened.revision,
        });

        expect((yield* fileSystem.stat(sourcePath)).mode & 0o777).toBe(0o755);
      }),
    );

    it.effect("preserves in-project symlinks when conditionally saving their target", () =>
      Effect.gen(function* () {
        if ((yield* HostProcessPlatform) === "win32") return;
        const workspaceFileSystem = yield* WorkspaceFileSystem.WorkspaceFileSystem;
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const cwd = yield* makeTempDir;
        const targetPath = path.join(cwd, "src", "analysis.m");
        const linkPath = path.join(cwd, "analysis.m");
        yield* writeTextFile(cwd, "src/analysis.m", "answer = 1;\n");
        yield* fileSystem.symlink(targetPath, linkPath);
        const opened = yield* workspaceFileSystem.readFile({ cwd, relativePath: "analysis.m" });

        yield* workspaceFileSystem.writeFile({
          cwd,
          relativePath: "analysis.m",
          contents: "answer = 2;\n",
          expectedRevision: opened.revision,
        });

        expect(yield* fileSystem.readLink(linkPath)).toBe(targetPath);
        expect(yield* fileSystem.readFileString(targetPath)).toBe("answer = 2;\n");
      }),
    );
  });
});
