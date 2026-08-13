// @effect-diagnostics nodeBuiltinImport:off -- append-only run journals are a Node filesystem boundary.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

import {
  AnalysisArtifactContentHash,
  AnalysisRunSnapshot,
  type AnalysisArtifact,
  type AnalysisArtifactCandidate,
  type AnalysisArtifactId,
  type AnalysisArtifactRepresentation,
  type AnalysisArtifactRepresentationId,
  type AnalysisRunSnapshot as AnalysisRunSnapshotType,
  type AnalysisRunStorage,
} from "@scientfactory/analysis";
import {
  ExecutionOutputChunk,
  type ExecutionOutputChunk as OutputChunk,
} from "@scientfactory/execution";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import { writeFileStringAtomically } from "../../atomicWrite.ts";
import * as ServerConfig from "../../config.ts";

const MetadataJson = Schema.fromJsonString(AnalysisRunSnapshot);
const OutputJson = Schema.fromJsonString(ExecutionOutputChunk);
const RuntimeSettingsJson = Schema.fromJsonString(
  Schema.Struct({
    version: Schema.Literal(1),
    executablePaths: Schema.Record(Schema.String, Schema.String),
  }),
);
const decodeMetadata = Schema.decodeUnknownOption(MetadataJson);
const encodeMetadata = Schema.encodeEffect(MetadataJson);
const decodeOutput = Schema.decodeUnknownOption(OutputJson);
const encodeOutput = Schema.encodeEffect(OutputJson);
const decodeRuntimeSettings = Schema.decodeUnknownOption(RuntimeSettingsJson);
const encodeRuntimeSettings = Schema.encodeEffect(RuntimeSettingsJson);

const MAXIMUM_ARTIFACTS_PER_RUN = 50;
const MAXIMUM_REPRESENTATIONS_PER_ARTIFACT = 8;
const MAXIMUM_ARTIFACT_FILE_BYTES = 128 * 1024 * 1024;
const MAXIMUM_ARTIFACT_RUN_BYTES = 512 * 1024 * 1024;

async function sha256File(filePath: string): Promise<string> {
  return await new Promise<string>((resolve, reject) => {
    const hash = NodeCrypto.createHash("sha256");
    const stream = NodeFS.createReadStream(filePath);
    stream.on("data", (chunk) => hash.update(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve(`sha256:${hash.digest("hex")}`));
  });
}

export interface ResolvedAnalysisArtifactRepresentation {
  readonly artifact: AnalysisArtifact;
  readonly representation: AnalysisArtifactRepresentation;
  readonly path: string;
  readonly revision: { readonly size: number; readonly mtimeMs: number | null };
}

export interface RuntimeExecutableConfiguration {
  readonly executablePath: string | null;
  readonly warning: string | null;
}

export class LocalAnalysisStoreError extends Schema.TaggedErrorClass<LocalAnalysisStoreError>()(
  "LocalAnalysisStoreError",
  {
    operation: Schema.String,
    path: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export class LocalAnalysisStore extends Context.Service<
  LocalAnalysisStore,
  {
    readonly readRuntimeExecutablePath: (
      runtimeKind: string,
    ) => Effect.Effect<RuntimeExecutableConfiguration, LocalAnalysisStoreError>;
    readonly writeRuntimeExecutablePath: (
      runtimeKind: string,
      executablePath: string | null,
    ) => Effect.Effect<void, LocalAnalysisStoreError>;
    readonly persistRun: (
      run: AnalysisRunSnapshotType,
    ) => Effect.Effect<void, LocalAnalysisStoreError>;
    readonly appendOutput: (
      projectId: string,
      runId: string,
      chunk: OutputChunk,
    ) => Effect.Effect<void, LocalAnalysisStoreError>;
    readonly prepareArtifactStaging: (
      projectId: string,
      runId: string,
    ) => Effect.Effect<string, LocalAnalysisStoreError>;
    readonly publishArtifacts: (input: {
      readonly projectId: string;
      readonly runId: string;
      readonly createdAt: string;
      readonly candidates: ReadonlyArray<AnalysisArtifactCandidate>;
    }) => Effect.Effect<ReadonlyArray<AnalysisArtifact>, LocalAnalysisStoreError>;
    readonly discardArtifactStaging: (
      projectId: string,
      runId: string,
    ) => Effect.Effect<void, LocalAnalysisStoreError>;
    readonly resolveArtifact: (input: {
      readonly projectId: string;
      readonly runId: string;
      readonly artifactId: AnalysisArtifactId;
      readonly representationId: AnalysisArtifactRepresentationId;
    }) => Effect.Effect<ResolvedAnalysisArtifactRepresentation | null, LocalAnalysisStoreError>;
    readonly loadRuns: (
      projectId: string,
    ) => Effect.Effect<ReadonlyArray<AnalysisRunSnapshotType>, LocalAnalysisStoreError>;
    readonly loadRun: (
      projectId: string,
      runId: string,
    ) => Effect.Effect<AnalysisRunSnapshotType | null, LocalAnalysisStoreError>;
    readonly measureRunStorage: (
      projectId: string,
      runId: string,
    ) => Effect.Effect<AnalysisRunStorage, LocalAnalysisStoreError>;
    readonly removeDisposableRunData: (
      run: AnalysisRunSnapshotType,
      removedAt: string,
    ) => Effect.Effect<AnalysisRunSnapshotType, LocalAnalysisStoreError>;
  }
>()("t3/scient/analysis/LocalAnalysisStore") {}

const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const writeLock = yield* Semaphore.make(1);
  const root = config.analysisDir;
  const runsRoot = path.join(root, "runs");
  const runtimeSettingsPath = path.join(root, "runtime-settings.json");

  const storeError = (operation: string, operationPath: string, cause: unknown) =>
    new LocalAnalysisStoreError({ operation, path: operationPath, cause });

  const mapStoreError =
    (operation: string, operationPath: string) =>
    <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, LocalAnalysisStoreError, R> =>
      Effect.mapError(effect, (cause) => storeError(operation, operationPath, cause));

  const atomicWrite = (filePath: string, contents: string) =>
    writeFileStringAtomically({ filePath, contents }).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
      mapStoreError("atomic-write", filePath),
    );

  const readRuntimeSettings = Effect.gen(function* () {
    if (
      !(yield* fs.exists(runtimeSettingsPath).pipe(mapStoreError("exists", runtimeSettingsPath)))
    ) {
      return {
        settings: { version: 1 as const, executablePaths: {} as Record<string, string> },
        warning: null,
      };
    }
    const contents = yield* fs
      .readFileString(runtimeSettingsPath)
      .pipe(mapStoreError("read-runtime-settings", runtimeSettingsPath));
    const settings = decodeRuntimeSettings(contents);
    if (settings._tag === "None") {
      return {
        settings: { version: 1 as const, executablePaths: {} as Record<string, string> },
        warning: "Saved runtime settings are unreadable. Choose the runtime again to replace them.",
      };
    }
    return { settings: settings.value, warning: null };
  });

  const readRuntimeExecutablePath = (runtimeKind: string) =>
    Effect.map(readRuntimeSettings, ({ settings, warning }) => ({
      executablePath: settings.executablePaths[runtimeKind] ?? null,
      warning,
    }));

  const writeRuntimeExecutablePath = (runtimeKind: string, executablePath: string | null) =>
    writeLock.withPermits(1)(
      Effect.gen(function* () {
        const { settings } = yield* readRuntimeSettings;
        const executablePaths = { ...settings.executablePaths };
        if (executablePath === null) delete executablePaths[runtimeKind];
        else executablePaths[runtimeKind] = executablePath;
        const contents = yield* encodeRuntimeSettings({
          version: 1,
          executablePaths,
        }).pipe(mapStoreError("encode-runtime-settings", runtimeSettingsPath));
        yield* atomicWrite(runtimeSettingsPath, contents);
      }),
    );

  const runDirectory = (projectId: string, runId: string) => path.join(runsRoot, projectId, runId);
  const artifactStagingDirectory = (projectId: string, runId: string) =>
    path.join(runDirectory(projectId, runId), "artifact-staging");
  const artifactsDirectory = (projectId: string, runId: string) =>
    path.join(runDirectory(projectId, runId), "artifacts");
  const cleanupStagingDirectory = (projectId: string, runId: string) =>
    path.join(runDirectory(projectId, runId), ".cleanup-staging");

  const measureRunStorage = (projectId: string, runId: string) => {
    const directory = runDirectory(projectId, runId);
    const outputPath = path.join(directory, "output.ndjson");
    const artifactRoot = artifactsDirectory(projectId, runId);
    return Effect.tryPromise({
      try: async (): Promise<AnalysisRunStorage> => {
        let outputBytes = 0;
        try {
          const info = await NodeFSP.lstat(outputPath);
          if (info.isFile()) outputBytes = info.size;
        } catch (cause) {
          if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
        }
        let artifactBytes = 0;
        try {
          for (const entry of await NodeFSP.readdir(artifactRoot, { withFileTypes: true })) {
            if (!entry.isFile()) continue;
            artifactBytes += (await NodeFSP.lstat(NodePath.join(artifactRoot, entry.name))).size;
          }
        } catch (cause) {
          if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
        }
        return {
          status: "retained",
          outputBytes,
          artifactBytes,
          totalBytes: outputBytes + artifactBytes,
          removedAt: null,
        };
      },
      catch: (cause) => storeError("measure-run-storage", directory, cause),
    });
  };

  const recoverCleanupStaging = (
    projectId: string,
    runId: string,
    storageStatus: AnalysisRunStorage["status"] | null,
  ) => {
    const directory = runDirectory(projectId, runId);
    const staging = cleanupStagingDirectory(projectId, runId);
    return Effect.tryPromise({
      try: async () => {
        try {
          await NodeFSP.access(staging);
        } catch (cause) {
          if ((cause as NodeJS.ErrnoException).code === "ENOENT") return;
          throw cause;
        }
        if (storageStatus === "metadata-only") {
          await NodeFSP.rm(staging, { recursive: true, force: true });
          return;
        }
        const stagedOutput = NodePath.join(staging, "output.ndjson");
        const stagedArtifacts = NodePath.join(staging, "artifacts");
        try {
          await NodeFSP.rename(stagedOutput, NodePath.join(directory, "output.ndjson"));
        } catch (cause) {
          if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
        }
        try {
          await NodeFSP.rename(stagedArtifacts, NodePath.join(directory, "artifacts"));
        } catch (cause) {
          if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
        }
        await NodeFSP.rm(staging, { recursive: true, force: true });
      },
      catch: (cause) => storeError("recover-run-cleanup", directory, cause),
    });
  };

  const persistRun = (run: AnalysisRunSnapshotType) =>
    writeLock.withPermits(1)(
      Effect.gen(function* () {
        const directory = runDirectory(run.projectId, run.receipt.runId);
        const metadataPath = path.join(directory, "run.json");
        const metadata = {
          ...run,
          receipt: { ...run.receipt, output: [] },
        } satisfies AnalysisRunSnapshotType;
        const contents = yield* encodeMetadata(metadata).pipe(
          mapStoreError("encode-run", metadataPath),
        );
        yield* atomicWrite(metadataPath, contents);
      }),
    );

  const appendOutput = (projectId: string, runId: string, chunk: OutputChunk) =>
    writeLock.withPermits(1)(
      Effect.gen(function* () {
        const outputPath = path.join(runDirectory(projectId, runId), "output.ndjson");
        yield* fs
          .makeDirectory(path.dirname(outputPath), { recursive: true })
          .pipe(mapStoreError("make-output-directory", outputPath));
        const encodedChunk = yield* encodeOutput(chunk).pipe(
          mapStoreError("encode-output", outputPath),
        );
        yield* Effect.tryPromise({
          try: () => NodeFSP.appendFile(outputPath, `${encodedChunk}\n`, "utf8"),
          catch: (cause) => storeError("append-output", outputPath, cause),
        });
      }),
    );

  const prepareArtifactStaging = (projectId: string, runId: string) =>
    writeLock.withPermits(1)(
      Effect.gen(function* () {
        const directory = artifactStagingDirectory(projectId, runId);
        yield* fs
          .makeDirectory(directory, { recursive: true })
          .pipe(mapStoreError("prepare-artifact-staging", directory));
        return directory;
      }),
    );

  const discardArtifactStaging = (projectId: string, runId: string) => {
    const directory = artifactStagingDirectory(projectId, runId);
    return fs
      .remove(directory, { recursive: true, force: true })
      .pipe(mapStoreError("discard-artifact-staging", directory));
  };

  const publishArtifacts = (input: {
    readonly projectId: string;
    readonly runId: string;
    readonly createdAt: string;
    readonly candidates: ReadonlyArray<AnalysisArtifactCandidate>;
  }) =>
    writeLock.withPermits(1)(
      Effect.gen(function* () {
        const stagingDirectory = artifactStagingDirectory(input.projectId, input.runId);
        const stagingFilesDirectory = path.join(stagingDirectory, "files");
        const finalDirectory = artifactsDirectory(input.projectId, input.runId);
        if (input.candidates.length === 0) {
          yield* discardArtifactStaging(input.projectId, input.runId);
          return [];
        }
        if (input.candidates.length > MAXIMUM_ARTIFACTS_PER_RUN) {
          return yield* storeError(
            "publish-artifacts",
            stagingFilesDirectory,
            new Error(`A run can publish at most ${MAXIMUM_ARTIFACTS_PER_RUN} artifacts.`),
          );
        }
        const runDirectoryPath = runDirectory(input.projectId, input.runId);
        yield* fs
          .makeDirectory(runDirectoryPath, { recursive: true })
          .pipe(mapStoreError("make-artifact-run-directory", runDirectoryPath));
        const temporaryDirectory = yield* fs
          .makeTempDirectory({ directory: runDirectoryPath, prefix: ".artifacts-" })
          .pipe(mapStoreError("make-artifact-publish-directory", runDirectoryPath));
        return yield* Effect.gen(function* () {
          const artifactIds = new Set<string>();
          const fileNames = new Set<string>();
          let totalByteLength = 0;
          const artifacts: AnalysisArtifact[] = [];
          for (const candidate of input.candidates) {
            if (artifactIds.has(candidate.artifactId)) {
              return yield* storeError(
                "publish-artifacts",
                stagingFilesDirectory,
                new Error(`Duplicate artifact '${candidate.artifactId}'.`),
              );
            }
            artifactIds.add(candidate.artifactId);
            if (
              candidate.representations.length === 0 ||
              candidate.representations.length > MAXIMUM_REPRESENTATIONS_PER_ARTIFACT
            ) {
              return yield* storeError(
                "publish-artifacts",
                stagingFilesDirectory,
                new Error(
                  `Artifact '${candidate.artifactId}' has an invalid representation count.`,
                ),
              );
            }
            const representationIds = new Set<string>();
            const representations: AnalysisArtifactRepresentation[] = [];
            for (const representation of candidate.representations) {
              if (representationIds.has(representation.representationId)) {
                return yield* storeError(
                  "publish-artifacts",
                  stagingFilesDirectory,
                  new Error(
                    `Artifact '${candidate.artifactId}' repeats representation '${representation.representationId}'.`,
                  ),
                );
              }
              if (fileNames.has(representation.fileName)) {
                return yield* storeError(
                  "publish-artifacts",
                  stagingFilesDirectory,
                  new Error(`Artifact file '${representation.fileName}' is not unique.`),
                );
              }
              representationIds.add(representation.representationId);
              fileNames.add(representation.fileName);
              const sourcePath = path.join(stagingFilesDirectory, representation.fileName);
              const destinationPath = path.join(temporaryDirectory, representation.fileName);
              const sourceInfo = yield* fs
                .stat(sourcePath)
                .pipe(mapStoreError("inspect-artifact-candidate", sourcePath));
              const byteLength = Number(sourceInfo.size);
              if (
                sourceInfo.type !== "File" ||
                byteLength < 1 ||
                byteLength > MAXIMUM_ARTIFACT_FILE_BYTES ||
                totalByteLength + byteLength > MAXIMUM_ARTIFACT_RUN_BYTES
              ) {
                return yield* storeError(
                  "publish-artifacts",
                  sourcePath,
                  new Error(`Artifact file '${representation.fileName}' exceeds storage limits.`),
                );
              }
              yield* fs
                .copyFile(sourcePath, destinationPath)
                .pipe(mapStoreError("copy-artifact-candidate", sourcePath));
              const copiedInfo = yield* fs
                .stat(destinationPath)
                .pipe(mapStoreError("inspect-published-artifact", destinationPath));
              if (copiedInfo.type !== "File" || Number(copiedInfo.size) !== byteLength) {
                return yield* storeError(
                  "publish-artifacts",
                  destinationPath,
                  new Error(`Artifact file '${representation.fileName}' changed while publishing.`),
                );
              }
              const contentHash = yield* Effect.tryPromise({
                try: () => sha256File(destinationPath),
                catch: (cause) => storeError("hash-artifact", destinationPath, cause),
              });
              totalByteLength += byteLength;
              representations.push({
                ...representation,
                contentHash: AnalysisArtifactContentHash.make(contentHash),
                byteLength,
              });
            }
            artifacts.push({
              artifactId: candidate.artifactId,
              kind: candidate.kind,
              label: candidate.label,
              createdAt: input.createdAt,
              representations,
            });
          }
          yield* fs
            .rename(temporaryDirectory, finalDirectory)
            .pipe(mapStoreError("publish-artifacts", finalDirectory));
          yield* discardArtifactStaging(input.projectId, input.runId);
          return artifacts;
        }).pipe(
          Effect.ensuring(
            fs.remove(temporaryDirectory, { recursive: true, force: true }).pipe(Effect.ignore),
          ),
        );
      }),
    );

  const loadOutput = (outputPath: string) =>
    Effect.gen(function* () {
      if (!(yield* fs.exists(outputPath).pipe(mapStoreError("exists", outputPath)))) {
        return { chunks: [] as OutputChunk[], corruptLineCount: 0 };
      }
      const contents = yield* fs
        .readFileString(outputPath)
        .pipe(mapStoreError("read-output", outputPath));
      const chunks: OutputChunk[] = [];
      let corruptLineCount = 0;
      for (const line of contents.split("\n")) {
        if (line.length === 0) continue;
        const decoded = decodeOutput(line);
        if (decoded._tag === "Some") {
          chunks.push(decoded.value);
        } else {
          corruptLineCount += 1;
        }
      }
      return { chunks, corruptLineCount };
    });

  const loadMetadata = (projectId: string, runId: string) =>
    Effect.gen(function* () {
      const metadataPath = path.join(runDirectory(projectId, runId), "run.json");
      if (!(yield* fs.exists(metadataPath).pipe(mapStoreError("exists", metadataPath)))) {
        return null;
      }
      const metadataText = yield* fs
        .readFileString(metadataPath)
        .pipe(mapStoreError("read-run", metadataPath));
      const metadata = decodeMetadata(metadataText);
      if (metadata._tag === "None") {
        return yield* storeError(
          "decode-run",
          metadataPath,
          new Error("The persisted analysis run receipt is unreadable."),
        );
      }
      yield* recoverCleanupStaging(projectId, runId, metadata.value.localStorage.status);
      return metadata.value;
    });

  const removeDisposableRunData = (run: AnalysisRunSnapshotType, removedAt: string) =>
    writeLock
      .withPermits(1)(
        Effect.gen(function* () {
          const directory = runDirectory(run.projectId, run.receipt.runId);
          const staging = cleanupStagingDirectory(run.projectId, run.receipt.runId);
          yield* recoverCleanupStaging(run.projectId, run.receipt.runId, run.localStorage.status);
          const measured = yield* measureRunStorage(run.projectId, run.receipt.runId);
          const updated = {
            ...run,
            localStorage: {
              status: "metadata-only" as const,
              outputBytes: 0,
              artifactBytes: 0,
              totalBytes: 0,
              removedAt,
            },
            receipt: { ...run.receipt, output: [] },
          } satisfies AnalysisRunSnapshotType;
          const metadataPath = path.join(directory, "run.json");
          const contents = yield* encodeMetadata(updated).pipe(
            mapStoreError("encode-cleaned-run", metadataPath),
          );
          yield* Effect.tryPromise({
            try: async () => {
              await NodeFSP.mkdir(staging, { recursive: false });
              try {
                await NodeFSP.rename(
                  NodePath.join(directory, "output.ndjson"),
                  NodePath.join(staging, "output.ndjson"),
                );
              } catch (cause) {
                if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
              }
              try {
                await NodeFSP.rename(
                  NodePath.join(directory, "artifacts"),
                  NodePath.join(staging, "artifacts"),
                );
              } catch (cause) {
                if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
              }
            },
            catch: (cause) => storeError("stage-run-cleanup", staging, cause),
          });
          yield* atomicWrite(metadataPath, contents).pipe(
            Effect.tapError(() =>
              recoverCleanupStaging(run.projectId, run.receipt.runId, "retained").pipe(
                Effect.ignoreCause(),
              ),
            ),
          );
          yield* Effect.tryPromise({
            try: () => NodeFSP.rm(staging, { recursive: true, force: true }),
            catch: (cause) => storeError("finish-run-cleanup", staging, cause),
          }).pipe(Effect.ignoreCause({ log: true }));
          return {
            run: updated,
            freedBytes: measured.totalBytes,
          } as const;
        }),
      )
      .pipe(Effect.map((value) => value.run));

  const loadRuns = (projectId: string) =>
    Effect.gen(function* () {
      const projectRunsRoot = path.join(runsRoot, projectId);
      if (!(yield* fs.exists(projectRunsRoot).pipe(mapStoreError("exists", projectRunsRoot)))) {
        return [];
      }
      const runIds = yield* fs
        .readDirectory(projectRunsRoot)
        .pipe(mapStoreError("read-run-directory", projectRunsRoot));
      const runs: AnalysisRunSnapshotType[] = [];
      for (const runId of runIds) {
        const metadata = yield* loadMetadata(projectId, runId);
        if (metadata !== null) runs.push(metadata);
      }
      return runs;
    });

  const loadRun = (projectId: string, runId: string) =>
    Effect.gen(function* () {
      const metadata = yield* loadMetadata(projectId, runId);
      if (metadata === null) return null;
      const { chunks, corruptLineCount } = yield* loadOutput(
        path.join(runDirectory(projectId, runId), "output.ndjson"),
      );
      if (corruptLineCount > 0) {
        chunks.push({
          sequence: (chunks.at(-1)?.sequence ?? -1) + 1,
          stream: "system",
          text: `${corruptLineCount} persisted output record${corruptLineCount === 1 ? " was" : "s were"} unreadable.\n`,
          observedAt: metadata.receipt.finishedAt ?? metadata.receipt.startedAt,
        });
      }
      const outputByteLength = chunks.reduce(
        (total, chunk) =>
          chunk.stream === "system"
            ? total
            : total + new TextEncoder().encode(chunk.text).byteLength,
        0,
      );
      const metadataOnly = metadata.localStorage.status === "metadata-only";
      return {
        ...metadata,
        receipt: {
          ...metadata.receipt,
          output: chunks,
          outputTruncated: metadata.receipt.outputTruncated || corruptLineCount > 0,
          outputByteLength: metadataOnly ? metadata.receipt.outputByteLength : outputByteLength,
          outputContentHash:
            metadataOnly || corruptLineCount === 0 ? metadata.receipt.outputContentHash : null,
        },
      } satisfies AnalysisRunSnapshotType;
    });

  const resolveArtifact = (input: {
    readonly projectId: string;
    readonly runId: string;
    readonly artifactId: AnalysisArtifactId;
    readonly representationId: AnalysisArtifactRepresentationId;
  }) =>
    Effect.gen(function* () {
      const metadata = yield* loadMetadata(input.projectId, input.runId);
      const artifact = metadata?.artifacts.find(
        (candidate) => candidate.artifactId === input.artifactId,
      );
      const representation = artifact?.representations.find(
        (candidate) => candidate.representationId === input.representationId,
      );
      if (!artifact || !representation) return null;
      const artifactPath = path.join(
        artifactsDirectory(input.projectId, input.runId),
        representation.fileName,
      );
      const info = yield* fs.stat(artifactPath).pipe(
        Effect.map((value) => Option.some(value)),
        Effect.orElseSucceed(() => Option.none()),
      );
      if (Option.isNone(info) || info.value.type !== "File") return null;
      return {
        artifact,
        representation,
        path: artifactPath,
        revision: {
          size: Number(info.value.size),
          mtimeMs: Option.match(info.value.mtime, {
            onNone: () => null,
            onSome: (mtime) => mtime.getTime(),
          }),
        },
      } satisfies ResolvedAnalysisArtifactRepresentation;
    });

  return LocalAnalysisStore.of({
    readRuntimeExecutablePath,
    writeRuntimeExecutablePath,
    persistRun,
    appendOutput,
    prepareArtifactStaging,
    publishArtifacts,
    discardArtifactStaging,
    resolveArtifact,
    loadRuns,
    loadRun,
    measureRunStorage,
    removeDisposableRunData,
  });
});

export const layer = Layer.effect(LocalAnalysisStore, make);
