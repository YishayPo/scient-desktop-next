import {
  AnalysisArtifactFileName,
  AnalysisArtifactId,
  AnalysisArtifactRepresentationId,
  AnalysisRuntimeId,
  AnalysisSourceRevision,
  type AnalysisArtifactCandidate,
  type AnalysisRunSnapshot,
} from "@scientfactory/analysis";
import { ExecutionRunId } from "@scientfactory/execution";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as ServerConfig from "../../config.ts";
import { LocalAnalysisStore, layer } from "./LocalAnalysisStore.ts";

const UnknownJsonString = Schema.fromJsonString(Schema.Unknown);
const decodeUnknownJson = Schema.decodeUnknownEffect(UnknownJsonString);
const encodeUnknownJson = Schema.encodeEffect(UnknownJsonString);

const run = {
  contractVersion: 1,
  projectId: "project-1",
  action: "run-file",
  runtime: {
    id: AnalysisRuntimeId.make("matlab:local"),
    kind: "matlab",
    label: "MATLAB",
    availability: "available",
    source: "path",
    executablePath: "/opt/matlab/bin/matlab",
    version: null,
    detail: null,
    capabilities: ["run-file", "stream-output", "cancel-process-tree"],
    inspectedAt: "2026-08-12T00:00:00.000Z",
    verification: null,
  },
  source: {
    cwd: "/project",
    relativePath: "analysis.m",
    revision: AnalysisSourceRevision.make("sha256:source"),
  },
  phase: "running",
  queuePosition: null,
  diagnostics: [],
  artifacts: [],
  artifactReceipt: { status: "not-requested", failureMessage: null },
  localStorage: {
    status: "retained",
    outputBytes: 0,
    artifactBytes: 0,
    totalBytes: 0,
    removedAt: null,
  },
  receipt: {
    runId: ExecutionRunId.make("run-1"),
    status: "running",
    startedAt: "2026-08-12T00:00:00.000Z",
    finishedAt: null,
    exitCode: null,
    failureMessage: null,
    cancellationRequested: false,
    outputTruncated: false,
    outputByteLength: 19,
    outputContentHash: null,
    output: [],
  },
} satisfies AnalysisRunSnapshot;

describe("LocalAnalysisStore", () => {
  it.effect(
    "persists environment runtime settings and append-only run output across lifetimes",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const baseDir = yield* fs.makeTempDirectoryScoped({
          prefix: "scient-analysis-store-",
        });
        const storeLayer = layer.pipe(
          Layer.provide(ServerConfig.ServerConfig.layerTest(process.cwd(), baseDir)),
          Layer.provideMerge(NodeServices.layer),
        );

        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalAnalysisStore;
            yield* store.writeRuntimeExecutablePath("matlab", "/opt/matlab/bin/matlab");
            yield* store.persistRun(run);
            yield* store.appendOutput(run.projectId, run.receipt.runId, {
              sequence: 0,
              stream: "stdout",
              text: "scient-analysis-ok\n",
              observedAt: "2026-08-12T00:00:01.000Z",
            });
          }).pipe(Effect.provide(storeLayer)),
        );

        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalAnalysisStore;
            expect(yield* store.readRuntimeExecutablePath("matlab")).toEqual({
              executablePath: "/opt/matlab/bin/matlab",
              warning: null,
            });
            const restored = yield* store.loadRuns(run.projectId);
            expect(restored).toHaveLength(1);
            expect(restored[0]).toMatchObject({
              source: run.source,
              receipt: { status: "running", output: [] },
            });
            const restoredWithOutput = yield* store.loadRun(run.projectId, run.receipt.runId);
            expect(restoredWithOutput).toMatchObject({
              receipt: {
                status: "running",
                output: [{ sequence: 0, stream: "stdout", text: "scient-analysis-ok\n" }],
              },
            });
          }).pipe(Effect.provide(storeLayer)),
        );
      }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect("marks a partly corrupt output journal as truncated and unhashed", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-analysis-store-corrupt-output-",
      });
      const storeLayer = layer.pipe(
        Layer.provide(ServerConfig.ServerConfig.layerTest(process.cwd(), baseDir)),
        Layer.provideMerge(NodeServices.layer),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const store = yield* LocalAnalysisStore;
          yield* store.persistRun(run);
        }).pipe(Effect.provide(storeLayer)),
      );
      const outputPath = path.join(
        baseDir,
        "userdata",
        "analysis",
        "runs",
        run.projectId,
        run.receipt.runId,
        "output.ndjson",
      );
      const encodedChunk = yield* encodeUnknownJson({
        sequence: 0,
        stream: "stdout",
        text: "retained prefix\n",
        observedAt: "2026-08-12T00:00:01.000Z",
      });
      yield* fs.writeFileString(outputPath, `${encodedChunk}\n{not-json}\n`);

      const restored = yield* Effect.scoped(
        Effect.gen(function* () {
          const store = yield* LocalAnalysisStore;
          return yield* store.loadRun(run.projectId, run.receipt.runId);
        }).pipe(Effect.provide(storeLayer)),
      );
      expect(restored?.receipt.outputTruncated).toBe(true);
      expect(restored?.receipt.outputContentHash).toBeNull();
      expect(restored?.receipt.output.map((chunk) => chunk.stream)).toEqual(["stdout", "system"]);
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect("loads receipt metadata written before output content hashes were introduced", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-analysis-store-legacy-",
      });
      const storeLayer = layer.pipe(
        Layer.provide(ServerConfig.ServerConfig.layerTest(process.cwd(), baseDir)),
        Layer.provideMerge(NodeServices.layer),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const store = yield* LocalAnalysisStore;
          yield* store.persistRun(run);
        }).pipe(Effect.provide(storeLayer)),
      );

      const metadataPath = path.join(
        baseDir,
        "userdata",
        "analysis",
        "runs",
        run.projectId,
        run.receipt.runId,
        "run.json",
      );
      const legacyMetadata = yield* decodeUnknownJson(yield* fs.readFileString(metadataPath));
      if (
        typeof legacyMetadata !== "object" ||
        legacyMetadata === null ||
        !("receipt" in legacyMetadata) ||
        typeof legacyMetadata.receipt !== "object" ||
        legacyMetadata.receipt === null
      ) {
        throw new Error("Expected persisted analysis receipt metadata.");
      }
      delete (legacyMetadata.receipt as { outputContentHash?: unknown }).outputContentHash;
      delete (legacyMetadata as { phase?: unknown }).phase;
      delete (legacyMetadata as { queuePosition?: unknown }).queuePosition;
      delete (legacyMetadata as { diagnostics?: unknown }).diagnostics;
      if (
        "runtime" in legacyMetadata &&
        typeof legacyMetadata.runtime === "object" &&
        legacyMetadata.runtime !== null
      ) {
        delete (legacyMetadata.runtime as { verification?: unknown }).verification;
      }
      delete (legacyMetadata as { artifacts?: unknown }).artifacts;
      delete (legacyMetadata as { artifactReceipt?: unknown }).artifactReceipt;
      yield* fs.writeFileString(metadataPath, yield* encodeUnknownJson(legacyMetadata));

      const restored = yield* Effect.scoped(
        Effect.gen(function* () {
          const store = yield* LocalAnalysisStore;
          return yield* store.loadRun(run.projectId, run.receipt.runId);
        }).pipe(Effect.provide(storeLayer)),
      );

      expect(restored?.receipt.outputContentHash).toBeNull();
      expect(restored?.phase).toBe("finished");
      expect(restored?.queuePosition).toBeNull();
      expect(restored?.diagnostics).toEqual([]);
      expect(restored?.runtime.verification).toBeNull();
      expect(restored?.artifacts).toEqual([]);
      expect(restored?.artifactReceipt).toEqual({
        status: "not-requested",
        failureMessage: null,
      });
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect("surfaces unreadable runtime settings and lets configuration repair them", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-analysis-store-invalid-runtime-",
      });
      const storeLayer = layer.pipe(
        Layer.provide(ServerConfig.ServerConfig.layerTest(process.cwd(), baseDir)),
        Layer.provideMerge(NodeServices.layer),
      );
      const settingsPath = path.join(baseDir, "userdata", "analysis", "runtime-settings.json");
      yield* fs.makeDirectory(path.dirname(settingsPath), { recursive: true });
      yield* fs.writeFileString(settingsPath, "{not-json");

      yield* Effect.scoped(
        Effect.gen(function* () {
          const store = yield* LocalAnalysisStore;
          const unreadable = yield* store.readRuntimeExecutablePath("matlab");
          expect(unreadable.executablePath).toBeNull();
          expect(unreadable.warning).toContain("unreadable");
          yield* store.writeRuntimeExecutablePath("matlab", "/Applications/MATLAB.app/bin/matlab");
          expect(yield* store.readRuntimeExecutablePath("matlab")).toEqual({
            executablePath: "/Applications/MATLAB.app/bin/matlab",
            warning: null,
          });
        }).pipe(Effect.provide(storeLayer)),
      );
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect("fails visibly instead of omitting an unreadable run receipt", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-analysis-store-invalid-receipt-",
      });
      const storeLayer = layer.pipe(
        Layer.provide(ServerConfig.ServerConfig.layerTest(process.cwd(), baseDir)),
        Layer.provideMerge(NodeServices.layer),
      );
      const runDirectory = path.join(
        baseDir,
        "userdata",
        "analysis",
        "runs",
        run.projectId,
        run.receipt.runId,
      );
      yield* fs.makeDirectory(runDirectory, { recursive: true });
      yield* fs.writeFileString(path.join(runDirectory, "run.json"), "{not-json");

      const loaded = yield* Effect.scoped(
        Effect.gen(function* () {
          const store = yield* LocalAnalysisStore;
          return yield* store.loadRuns(run.projectId).pipe(Effect.result);
        }).pipe(Effect.provide(storeLayer)),
      );
      expect(loaded._tag).toBe("Failure");
      if (loaded._tag === "Failure") expect(loaded.failure.operation).toBe("decode-run");
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect(
    "publishes immutable multi-representation artifacts and resolves them across lifetimes",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const baseDir = yield* fs.makeTempDirectoryScoped({
          prefix: "scient-analysis-store-artifacts-",
        });
        const storeLayer = layer.pipe(
          Layer.provide(ServerConfig.ServerConfig.layerTest(process.cwd(), baseDir)),
          Layer.provideMerge(NodeServices.layer),
        );
        const artifactId = AnalysisArtifactId.make("figure-001");
        const pngRepresentationId = AnalysisArtifactRepresentationId.make("static-png");
        const candidates = [
          {
            artifactId,
            kind: "figure",
            label: "Figure 1",
            representations: [
              {
                representationId: pngRepresentationId,
                fileName: AnalysisArtifactFileName.make("figure-001.png"),
                mediaType: "image/png",
                presentation: "static",
                requiresNetworkForFullExperience: false,
              },
              {
                representationId: AnalysisArtifactRepresentationId.make("interactive-html"),
                fileName: AnalysisArtifactFileName.make("figure-001.html"),
                mediaType: "text/html",
                presentation: "interactive",
                requiresNetworkForFullExperience: true,
              },
            ],
          },
        ] satisfies ReadonlyArray<AnalysisArtifactCandidate>;

        const published = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalAnalysisStore;
            const staging = yield* store.prepareArtifactStaging(run.projectId, run.receipt.runId);
            const files = path.join(staging, "files");
            yield* fs.makeDirectory(files, { recursive: true });
            yield* fs.writeFile(path.join(files, "figure-001.png"), new Uint8Array([1, 2, 3]));
            yield* fs.writeFileString(path.join(files, "figure-001.html"), "<html>figure</html>");
            const artifacts = yield* store.publishArtifacts({
              projectId: run.projectId,
              runId: run.receipt.runId,
              createdAt: "2026-08-12T00:00:02.000Z",
              candidates,
            });
            yield* store.persistRun({ ...run, artifacts });
            return artifacts;
          }).pipe(Effect.provide(storeLayer)),
        );

        expect(published).toHaveLength(1);
        expect(published[0]).toMatchObject({
          artifactId: "figure-001",
          representations: [
            { byteLength: 3, contentHash: expect.stringMatching(/^sha256:[0-9a-f]{64}$/u) },
            { byteLength: 19, contentHash: expect.stringMatching(/^sha256:[0-9a-f]{64}$/u) },
          ],
        });

        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalAnalysisStore;
            const restored = yield* store.loadRun(run.projectId, run.receipt.runId);
            expect(restored?.artifacts).toEqual(published);
            const resolved = yield* store.resolveArtifact({
              projectId: run.projectId,
              runId: run.receipt.runId,
              artifactId,
              representationId: pngRepresentationId,
            });
            expect(resolved).toMatchObject({
              artifact: { artifactId: "figure-001" },
              representation: { representationId: "static-png", byteLength: 3 },
              revision: { size: 3 },
            });
            expect(resolved?.path.endsWith("/artifacts/figure-001.png")).toBe(true);
            expect(
              yield* store.resolveArtifact({
                projectId: run.projectId,
                runId: run.receipt.runId,
                artifactId,
                representationId: AnalysisArtifactRepresentationId.make("missing"),
              }),
            ).toBeNull();
          }).pipe(Effect.provide(storeLayer)),
        );
      }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect("rejects ambiguous artifact candidates without publishing a partial final set", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-analysis-store-invalid-artifacts-",
      });
      const storeLayer = layer.pipe(
        Layer.provide(ServerConfig.ServerConfig.layerTest(process.cwd(), baseDir)),
        Layer.provideMerge(NodeServices.layer),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const store = yield* LocalAnalysisStore;
          const staging = yield* store.prepareArtifactStaging(run.projectId, run.receipt.runId);
          const files = path.join(staging, "files");
          yield* fs.makeDirectory(files, { recursive: true });
          yield* fs.writeFile(path.join(files, "figure-001.png"), new Uint8Array([1]));
          const representation = {
            representationId: AnalysisArtifactRepresentationId.make("static-png"),
            fileName: AnalysisArtifactFileName.make("figure-001.png"),
            mediaType: "image/png" as const,
            presentation: "static" as const,
            requiresNetworkForFullExperience: false,
          };
          const error = yield* store
            .publishArtifacts({
              projectId: run.projectId,
              runId: run.receipt.runId,
              createdAt: "2026-08-12T00:00:02.000Z",
              candidates: [
                {
                  artifactId: AnalysisArtifactId.make("figure-001"),
                  kind: "figure",
                  label: "Figure 1",
                  representations: [representation, representation],
                },
              ],
            })
            .pipe(Effect.flip);
          expect(error).toMatchObject({ operation: "publish-artifacts" });
          expect(
            yield* fs.exists(
              path.join(
                baseDir,
                "userdata",
                "analysis",
                "runs",
                run.projectId,
                run.receipt.runId,
                "artifacts",
              ),
            ),
          ).toBe(false);
        }).pipe(Effect.provide(storeLayer)),
      );
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect("removes disposable output transactionally while preserving metadata provenance", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const baseDir = yield* fs.makeTempDirectoryScoped({
        prefix: "scient-analysis-store-cleanup-",
      });
      const storeLayer = layer.pipe(
        Layer.provide(ServerConfig.ServerConfig.layerTest(process.cwd(), baseDir)),
        Layer.provideMerge(NodeServices.layer),
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const store = yield* LocalAnalysisStore;
          const staging = yield* store.prepareArtifactStaging(run.projectId, run.receipt.runId);
          const files = path.join(staging, "files");
          yield* fs.makeDirectory(files, { recursive: true });
          yield* fs.writeFile(path.join(files, "figure-001.png"), new Uint8Array([1, 2, 3]));
          const artifacts = yield* store.publishArtifacts({
            projectId: run.projectId,
            runId: run.receipt.runId,
            createdAt: "2026-08-12T00:00:02.000Z",
            candidates: [
              {
                artifactId: AnalysisArtifactId.make("figure-001"),
                kind: "figure",
                label: "Figure 1",
                representations: [
                  {
                    representationId: AnalysisArtifactRepresentationId.make("static-png"),
                    fileName: AnalysisArtifactFileName.make("figure-001.png"),
                    mediaType: "image/png",
                    presentation: "static",
                    requiresNetworkForFullExperience: false,
                  },
                ],
              },
            ],
          });
          yield* store.appendOutput(run.projectId, run.receipt.runId, {
            sequence: 0,
            stream: "stdout",
            text: "retained output\n",
            observedAt: "2026-08-12T00:00:01.000Z",
          });
          const measured = yield* store.measureRunStorage(run.projectId, run.receipt.runId);
          expect(measured.outputBytes).toBeGreaterThan(0);
          expect(measured.artifactBytes).toBe(3);
          const terminal = {
            ...run,
            phase: "finished" as const,
            artifacts,
            localStorage: measured,
            receipt: {
              ...run.receipt,
              status: "succeeded" as const,
              finishedAt: "2026-08-12T00:00:03.000Z",
              exitCode: 0,
              outputByteLength: 16,
              outputContentHash: `sha256:${"a".repeat(64)}`,
            },
          } satisfies AnalysisRunSnapshot;
          yield* store.persistRun(terminal);

          const cleaned = yield* store.removeDisposableRunData(
            terminal,
            "2026-08-12T00:00:04.000Z",
          );
          expect(cleaned.localStorage).toEqual({
            status: "metadata-only",
            outputBytes: 0,
            artifactBytes: 0,
            totalBytes: 0,
            removedAt: "2026-08-12T00:00:04.000Z",
          });
          const restored = yield* store.loadRun(run.projectId, run.receipt.runId);
          expect(restored).toMatchObject({
            artifacts: [{ artifactId: "figure-001" }],
            localStorage: { status: "metadata-only" },
            receipt: {
              output: [],
              outputByteLength: 16,
              outputContentHash: terminal.receipt.outputContentHash,
            },
          });
          expect(
            yield* store.resolveArtifact({
              projectId: run.projectId,
              runId: run.receipt.runId,
              artifactId: AnalysisArtifactId.make("figure-001"),
              representationId: AnalysisArtifactRepresentationId.make("static-png"),
            }),
          ).toBeNull();
          expect(
            (yield* store.measureRunStorage(run.projectId, run.receipt.runId)).totalBytes,
          ).toBe(0);
        }).pipe(Effect.provide(storeLayer)),
      );
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );
});
