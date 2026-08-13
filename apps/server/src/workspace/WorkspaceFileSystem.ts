// @effect-diagnostics nodeBuiltinImport:off
/**
 * WorkspaceFileSystem - Effect service contract for workspace file mutations.
 *
 * Owns workspace-root-relative file read/write operations and their associated
 * safety checks and cache invalidation hooks.
 *
 * @module WorkspaceFileSystem
 */
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";

import type {
  ProjectFileWatchEvent,
  ProjectReadFileInput,
  ProjectReadFileResult,
  ProjectWriteFileInput,
  ProjectWriteFileResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as Stream from "effect/Stream";

import { writeFileStringAtomically } from "../atomicWrite.ts";
import * as WorkspaceEntries from "./WorkspaceEntries.ts";
import * as WorkspacePaths from "./WorkspacePaths.ts";

const PROJECT_READ_FILE_MAX_BYTES = 1024 * 1024;

function isNodeError(error: unknown, code: string): error is NodeJS.ErrnoException {
  return error instanceof Error && "code" in error && error.code === code;
}

export class WorkspaceFileSystemOperationError extends Schema.TaggedErrorClass<WorkspaceFileSystemOperationError>()(
  "WorkspaceFileSystemOperationError",
  {
    workspaceRoot: Schema.String,
    relativePath: Schema.String,
    resolvedPath: Schema.String,
    operationPath: Schema.String,
    operation: Schema.Literals([
      "realpath-workspace-root",
      "realpath-target",
      "realpath-watch-directory",
      "open",
      "stat",
      "read",
      "close",
      "make-directory",
      "write-file",
      "atomic-write-file",
      "watch",
    ]),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Workspace file operation '${this.operation}' failed at '${this.operationPath}' for resolved path '${this.resolvedPath}' (requested as '${this.relativePath}' in '${this.workspaceRoot}').`;
  }
}

export class WorkspaceFilePathEscapeError extends Schema.TaggedErrorClass<WorkspaceFilePathEscapeError>()(
  "WorkspaceFilePathEscapeError",
  {
    workspaceRoot: Schema.String,
    relativePath: Schema.String,
    resolvedWorkspaceRoot: Schema.String,
    resolvedPath: Schema.String,
  },
) {
  override get message(): string {
    return `Workspace file '${this.relativePath}' resolves outside workspace root '${this.workspaceRoot}': ${this.resolvedPath}`;
  }
}

export class WorkspacePathNotFileError extends Schema.TaggedErrorClass<WorkspacePathNotFileError>()(
  "WorkspacePathNotFileError",
  {
    workspaceRoot: Schema.String,
    relativePath: Schema.String,
    resolvedPath: Schema.String,
  },
) {
  override get message(): string {
    return `Workspace path '${this.relativePath}' in '${this.workspaceRoot}' is not a file: ${this.resolvedPath}`;
  }
}

export class WorkspaceBinaryFileError extends Schema.TaggedErrorClass<WorkspaceBinaryFileError>()(
  "WorkspaceBinaryFileError",
  {
    workspaceRoot: Schema.String,
    relativePath: Schema.String,
    resolvedPath: Schema.String,
  },
) {
  override get message(): string {
    return `Workspace file '${this.relativePath}' in '${this.workspaceRoot}' is binary and cannot be previewed as text.`;
  }
}

export class WorkspaceFileRevisionConflictError extends Schema.TaggedErrorClass<WorkspaceFileRevisionConflictError>()(
  "WorkspaceFileRevisionConflictError",
  {
    workspaceRoot: Schema.String,
    relativePath: Schema.String,
    resolvedPath: Schema.String,
    currentRevision: Schema.String,
  },
) {
  override get message(): string {
    return `Workspace file '${this.relativePath}' changed after it was opened. Reload it before saving.`;
  }
}

export const WorkspaceFileSystemError = Schema.Union([
  WorkspaceFileSystemOperationError,
  WorkspaceFilePathEscapeError,
  WorkspacePathNotFileError,
  WorkspaceBinaryFileError,
  WorkspaceFileRevisionConflictError,
]);
export type WorkspaceFileSystemError = typeof WorkspaceFileSystemError.Type;

/** Service tag for workspace file operations. */
export class WorkspaceFileSystem extends Context.Service<
  WorkspaceFileSystem,
  {
    /** Read a UTF-8 text file relative to the workspace root. */
    readonly readFile: (
      input: ProjectReadFileInput,
    ) => Effect.Effect<
      ProjectReadFileResult,
      WorkspaceFileSystemError | WorkspacePaths.WorkspacePathOutsideRootError
    >;
    /**
     * Write a file relative to the workspace root.
     *
     * Creates parent directories as needed and rejects paths that escape the
     * workspace root.
     */
    readonly writeFile: (
      input: ProjectWriteFileInput,
    ) => Effect.Effect<
      ProjectWriteFileResult,
      WorkspaceFileSystemError | WorkspacePaths.WorkspacePathOutsideRootError
    >;
    /** Observe native filesystem hints for one currently open file. */
    readonly watchFile: (
      input: ProjectReadFileInput,
    ) => Stream.Stream<
      ProjectFileWatchEvent,
      WorkspaceFileSystemError | WorkspacePaths.WorkspacePathOutsideRootError
    >;
  }
>()("t3/workspace/WorkspaceFileSystem") {}

export const make = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
  const workspaceEntries = yield* WorkspaceEntries.WorkspaceEntries;
  const writeSemaphoresRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());

  const writeSemaphoreFor = (absolutePath: string) =>
    SynchronizedRef.modifyEffect(writeSemaphoresRef, (semaphores) => {
      const existing = semaphores.get(absolutePath);
      if (existing) return Effect.succeed([existing, semaphores] as const);
      return Semaphore.make(1).pipe(
        Effect.map((semaphore) => {
          const next = new Map(semaphores);
          next.set(absolutePath, semaphore);
          return [semaphore, next] as const;
        }),
      );
    });

  const revisionForBytes = (bytes: Uint8Array): string =>
    `sha256:${NodeCrypto.createHash("sha256").update(bytes).digest("hex")}`;

  const revisionForContents = (contents: string): string =>
    revisionForBytes(new TextEncoder().encode(contents));

  const resolveRealFileTarget = Effect.fn("WorkspaceFileSystem.resolveRealFileTarget")(function* (
    input: ProjectReadFileInput,
  ) {
    const target = yield* workspacePaths.resolveRelativePathWithinRoot({
      workspaceRoot: input.cwd,
      relativePath: input.relativePath,
    });
    const realWorkspaceRoot = yield* Effect.tryPromise({
      try: () => NodeFSP.realpath(input.cwd),
      catch: (cause) =>
        new WorkspaceFileSystemOperationError({
          workspaceRoot: input.cwd,
          relativePath: input.relativePath,
          resolvedPath: target.absolutePath,
          operationPath: input.cwd,
          operation: "realpath-workspace-root",
          cause,
        }),
    });
    const realTargetPath = yield* Effect.tryPromise({
      try: () => NodeFSP.realpath(target.absolutePath),
      catch: (cause) =>
        new WorkspaceFileSystemOperationError({
          workspaceRoot: input.cwd,
          relativePath: input.relativePath,
          resolvedPath: target.absolutePath,
          operationPath: target.absolutePath,
          operation: "realpath-target",
          cause,
        }),
    });
    const relativeRealPath = path.relative(realWorkspaceRoot, realTargetPath);
    if (
      relativeRealPath.startsWith(`..${path.sep}`) ||
      relativeRealPath === ".." ||
      path.isAbsolute(relativeRealPath)
    ) {
      return yield* new WorkspaceFilePathEscapeError({
        workspaceRoot: input.cwd,
        relativePath: input.relativePath,
        resolvedWorkspaceRoot: realWorkspaceRoot,
        resolvedPath: realTargetPath,
      });
    }
    return { target, realWorkspaceRoot, realTargetPath };
  });

  const readFile: WorkspaceFileSystem["Service"]["readFile"] = Effect.fn(
    "WorkspaceFileSystem.readFile",
  )(function* (input) {
    const { target, realTargetPath } = yield* resolveRealFileTarget(input);

    return yield* Effect.acquireUseRelease(
      Effect.tryPromise({
        try: () => NodeFSP.open(realTargetPath, "r"),
        catch: (cause) =>
          new WorkspaceFileSystemOperationError({
            workspaceRoot: input.cwd,
            relativePath: input.relativePath,
            resolvedPath: realTargetPath,
            operationPath: realTargetPath,
            operation: "open",
            cause,
          }),
      }),
      (handle) =>
        Effect.gen(function* () {
          const stat = yield* Effect.tryPromise({
            try: () => handle.stat(),
            catch: (cause) =>
              new WorkspaceFileSystemOperationError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedPath: realTargetPath,
                operationPath: realTargetPath,
                operation: "stat",
                cause,
              }),
          });
          if (!stat.isFile()) {
            return yield* new WorkspacePathNotFileError({
              workspaceRoot: input.cwd,
              relativePath: input.relativePath,
              resolvedPath: realTargetPath,
            });
          }

          const bytesToRead = Math.min(stat.size, PROJECT_READ_FILE_MAX_BYTES);
          const buffer = Buffer.alloc(bytesToRead);
          const { bytesRead } = yield* Effect.tryPromise({
            try: () => handle.read(buffer, 0, bytesToRead, 0),
            catch: (cause) =>
              new WorkspaceFileSystemOperationError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedPath: realTargetPath,
                operationPath: realTargetPath,
                operation: "read",
                cause,
              }),
          });
          const fileBytes = buffer.subarray(0, bytesRead);
          if (fileBytes.includes(0)) {
            return yield* new WorkspaceBinaryFileError({
              workspaceRoot: input.cwd,
              relativePath: input.relativePath,
              resolvedPath: realTargetPath,
            });
          }

          return {
            relativePath: target.relativePath,
            contents: new TextDecoder("utf-8").decode(fileBytes),
            byteLength: stat.size,
            truncated: stat.size > PROJECT_READ_FILE_MAX_BYTES,
            revision: revisionForBytes(fileBytes),
          };
        }),
      (handle) =>
        Effect.tryPromise({
          try: () => handle.close(),
          catch: (cause) =>
            new WorkspaceFileSystemOperationError({
              workspaceRoot: input.cwd,
              relativePath: input.relativePath,
              resolvedPath: realTargetPath,
              operationPath: realTargetPath,
              operation: "close",
              cause,
            }),
        }),
    );
  });

  const resolveRealFileWatchTarget = Effect.fn("WorkspaceFileSystem.resolveRealFileWatchTarget")(
    function* (input: ProjectReadFileInput) {
      const target = yield* workspacePaths.resolveRelativePathWithinRoot({
        workspaceRoot: input.cwd,
        relativePath: input.relativePath,
      });
      const realWorkspaceRoot = yield* Effect.tryPromise({
        try: () => NodeFSP.realpath(input.cwd),
        catch: (cause) =>
          new WorkspaceFileSystemOperationError({
            workspaceRoot: input.cwd,
            relativePath: input.relativePath,
            resolvedPath: target.absolutePath,
            operationPath: input.cwd,
            operation: "realpath-workspace-root",
            cause,
          }),
      });
      const resolvedParentDirectory = yield* Effect.tryPromise({
        try: () => NodeFSP.realpath(path.dirname(target.absolutePath)),
        catch: (cause) =>
          new WorkspaceFileSystemOperationError({
            workspaceRoot: input.cwd,
            relativePath: input.relativePath,
            resolvedPath: target.absolutePath,
            operationPath: path.dirname(target.absolutePath),
            operation: "realpath-watch-directory",
            cause,
          }),
      });
      const relativeWatchDirectory = path.relative(realWorkspaceRoot, resolvedParentDirectory);
      if (
        relativeWatchDirectory.startsWith(`..${path.sep}`) ||
        relativeWatchDirectory === ".." ||
        path.isAbsolute(relativeWatchDirectory)
      ) {
        return yield* new WorkspaceFilePathEscapeError({
          workspaceRoot: input.cwd,
          relativePath: input.relativePath,
          resolvedWorkspaceRoot: realWorkspaceRoot,
          resolvedPath: resolvedParentDirectory,
        });
      }
      const realTargetPath = yield* Effect.tryPromise({
        try: async () => {
          try {
            return await NodeFSP.realpath(target.absolutePath);
          } catch (error) {
            if (isNodeError(error, "ENOENT")) {
              return path.join(resolvedParentDirectory, path.basename(target.absolutePath));
            }
            throw error;
          }
        },
        catch: (cause) =>
          new WorkspaceFileSystemOperationError({
            workspaceRoot: input.cwd,
            relativePath: input.relativePath,
            resolvedPath: target.absolutePath,
            operationPath: target.absolutePath,
            operation: "realpath-target",
            cause,
          }),
      });
      const relativeRealPath = path.relative(realWorkspaceRoot, realTargetPath);
      if (
        relativeRealPath.startsWith(`..${path.sep}`) ||
        relativeRealPath === ".." ||
        path.isAbsolute(relativeRealPath)
      ) {
        return yield* new WorkspaceFilePathEscapeError({
          workspaceRoot: input.cwd,
          relativePath: input.relativePath,
          resolvedWorkspaceRoot: realWorkspaceRoot,
          resolvedPath: realTargetPath,
        });
      }
      // Existing file symlinks need to watch the resolved target's directory,
      // not merely the directory containing the link. Missing files already
      // resolve through their canonical parent above.
      const watchDirectory = path.dirname(realTargetPath);
      return { realTargetPath, target, watchDirectory };
    },
  );

  const writeFile: WorkspaceFileSystem["Service"]["writeFile"] = Effect.fn(
    "WorkspaceFileSystem.writeFile",
  )(function* (input) {
    const target = yield* workspacePaths.resolveRelativePathWithinRoot({
      workspaceRoot: input.cwd,
      relativePath: input.relativePath,
    });
    const writeSemaphore = yield* writeSemaphoreFor(target.absolutePath);
    return yield* writeSemaphore.withPermits(1)(
      Effect.gen(function* () {
        let writeTargetPath = target.absolutePath;
        if (input.expectedRevision !== undefined) {
          const current = yield* readFile({ cwd: input.cwd, relativePath: input.relativePath });
          if (current.truncated || current.revision !== input.expectedRevision) {
            return yield* new WorkspaceFileRevisionConflictError({
              workspaceRoot: input.cwd,
              relativePath: input.relativePath,
              resolvedPath: target.absolutePath,
              currentRevision: current.revision,
            });
          }
          // Atomic rename replaces a symlink instead of following it. Viewer saves have
          // already verified that the existing target stays within the workspace, so write
          // through its real path to preserve the user's in-project link.
          writeTargetPath = yield* Effect.tryPromise({
            try: () => NodeFSP.realpath(target.absolutePath),
            catch: (cause) =>
              new WorkspaceFileSystemOperationError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedPath: target.absolutePath,
                operationPath: target.absolutePath,
                operation: "realpath-target",
                cause,
              }),
          });
          const realWorkspaceRoot = yield* Effect.tryPromise({
            try: () => NodeFSP.realpath(input.cwd),
            catch: (cause) =>
              new WorkspaceFileSystemOperationError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedPath: writeTargetPath,
                operationPath: input.cwd,
                operation: "realpath-workspace-root",
                cause,
              }),
          });
          const relativeRealPath = path.relative(realWorkspaceRoot, writeTargetPath);
          if (
            relativeRealPath.startsWith(`..${path.sep}`) ||
            relativeRealPath === ".." ||
            path.isAbsolute(relativeRealPath)
          ) {
            return yield* new WorkspaceFilePathEscapeError({
              workspaceRoot: input.cwd,
              relativePath: input.relativePath,
              resolvedWorkspaceRoot: realWorkspaceRoot,
              resolvedPath: writeTargetPath,
            });
          }
        }

        yield* fileSystem.makeDirectory(path.dirname(writeTargetPath), { recursive: true }).pipe(
          Effect.mapError(
            (cause) =>
              new WorkspaceFileSystemOperationError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedPath: writeTargetPath,
                operationPath: path.dirname(writeTargetPath),
                operation: "make-directory",
                cause,
              }),
          ),
        );
        const existingMode = yield* Effect.tryPromise({
          try: async () => {
            try {
              return (await NodeFSP.stat(writeTargetPath)).mode & 0o7777;
            } catch (error) {
              if (isNodeError(error, "ENOENT")) return undefined;
              throw error;
            }
          },
          catch: (cause) =>
            new WorkspaceFileSystemOperationError({
              workspaceRoot: input.cwd,
              relativePath: input.relativePath,
              resolvedPath: writeTargetPath,
              operationPath: writeTargetPath,
              operation: "stat",
              cause,
            }),
        });
        yield* writeFileStringAtomically({
          filePath: writeTargetPath,
          contents: input.contents,
          mode: existingMode,
        }).pipe(
          Effect.provideService(FileSystem.FileSystem, fileSystem),
          Effect.provideService(Path.Path, path),
          Effect.mapError(
            (cause) =>
              new WorkspaceFileSystemOperationError({
                workspaceRoot: input.cwd,
                relativePath: input.relativePath,
                resolvedPath: writeTargetPath,
                operationPath: writeTargetPath,
                operation: "atomic-write-file",
                cause,
              }),
          ),
        );
        yield* workspaceEntries.refresh(input.cwd);
        return {
          relativePath: target.relativePath,
          revision: revisionForContents(input.contents),
        };
      }),
    );
  });

  const watchFile: WorkspaceFileSystem["Service"]["watchFile"] = (input) =>
    Stream.unwrap(
      resolveRealFileWatchTarget(input).pipe(
        Effect.map(({ realTargetPath, target, watchDirectory }) => {
          return Stream.callback<ProjectFileWatchEvent, WorkspaceFileSystemOperationError>(
            (queue) =>
              Effect.acquireRelease(
                Effect.try({
                  try: () => {
                    const watcher = NodeFS.watch(watchDirectory, (_event, reportedPath) => {
                      // Node documents that filename may be absent on some
                      // platforms. Since this watcher is scoped to one parent,
                      // a conservative reread is cheaper than missing a save.
                      if (reportedPath !== null) {
                        const absoluteReportedPath = path.isAbsolute(reportedPath)
                          ? path.resolve(reportedPath)
                          : path.resolve(watchDirectory, reportedPath);
                        if (absoluteReportedPath !== realTargetPath) return;
                      }
                      Queue.offerUnsafe(queue, {
                        _tag: "file-changed",
                        relativePath: target.relativePath,
                      });
                    });
                    watcher.on("error", (cause) => {
                      Queue.failCauseUnsafe(
                        queue,
                        Cause.fail(
                          new WorkspaceFileSystemOperationError({
                            workspaceRoot: input.cwd,
                            relativePath: input.relativePath,
                            resolvedPath: realTargetPath,
                            operationPath: watchDirectory,
                            operation: "watch",
                            cause,
                          }),
                        ),
                      );
                    });
                    watcher.on("close", () => Queue.endUnsafe(queue));
                    Queue.offerUnsafe(queue, {
                      _tag: "watch-ready",
                      relativePath: target.relativePath,
                    });
                    return watcher;
                  },
                  catch: (cause) =>
                    new WorkspaceFileSystemOperationError({
                      workspaceRoot: input.cwd,
                      relativePath: input.relativePath,
                      resolvedPath: realTargetPath,
                      operationPath: watchDirectory,
                      operation: "watch",
                      cause,
                    }),
                }),
                (watcher) => Effect.sync(() => watcher.close()),
              ),
          ).pipe(
            // Editors and atomic writers commonly produce several events for
            // one save. Batch them after the write settles, but preserve the
            // explicit readiness signal used by clients and deterministic tests.
            Stream.groupedWithin(256, "100 millis"),
            Stream.flatMap((events) => {
              const coalesced: ProjectFileWatchEvent[] = [];
              if (events.some((event) => event._tag === "watch-ready")) {
                coalesced.push({ _tag: "watch-ready", relativePath: target.relativePath });
              }
              if (events.some((event) => event._tag === "file-changed")) {
                coalesced.push({ _tag: "file-changed", relativePath: target.relativePath });
              }
              return Stream.fromIterable(coalesced);
            }),
          );
        }),
      ),
    );

  return WorkspaceFileSystem.of({ readFile, watchFile, writeFile });
});

export const layer = Layer.effect(WorkspaceFileSystem, make);
