// @effect-diagnostics nodeBuiltinImport:off -- local execution receipts use host SHA-256.
import * as NodeCrypto from "node:crypto";

import { inspectScientProject, readScientProjectIdentity } from "@scientfactory/project-init";
import { HostProcessEnvironment, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import {
  AnalysisOperationError,
  type AnalysisArtifactId,
  type AnalysisArtifactRepresentationId,
  AnalysisSourceRevision,
  summarizeAnalysisRun,
  type AnalysisCancelRunInput,
  type AnalysisCleanupProjectInput,
  type AnalysisCleanupResult,
  type AnalysisCleanupRunInput,
  type AnalysisConfigureRuntimeInput,
  type AnalysisGetRunInput,
  type AnalysisInspectRuntimesInput,
  type AnalysisListRunsInput,
  type AnalysisListRunsResult,
  type AnalysisRunSnapshot,
  type AnalysisRunSummary,
  type AnalysisRunStreamEvent,
  type AnalysisRuntimeAdapter,
  type AnalysisRuntimeInspection,
  type AnalysisRuntimeProfile,
  type AnalysisRuntimeVerification,
  type AnalysisStartRunInput,
  type AnalysisStorageSummary,
  type AnalysisStorageSummaryInput,
  type AnalysisSubscribeRunsInput,
  type AnalysisVerifyRuntimeInput,
} from "@scientfactory/analysis";
import {
  ExecutionRunId,
  TERMINAL_EXECUTION_STATUSES,
  executionOutputContentParts,
  transitionExecutionStatus,
  type ExecutionOutputChunk,
  type ExecutionProcessHandle,
  type ExecutionProcessOutput,
  type ExecutionStatus,
} from "@scientfactory/execution";
import * as Context from "effect/Context";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import * as WorkspaceFileSystem from "../../workspace/WorkspaceFileSystem.ts";
import * as WorkspacePaths from "../../workspace/WorkspacePaths.ts";
import * as AnalysisRunIndex from "./AnalysisRunIndex.ts";
import * as LocalAnalysisStore from "./LocalAnalysisStore.ts";
import type { ResolvedAnalysisArtifactRepresentation } from "./LocalAnalysisStore.ts";
import * as LocalExecutionProcess from "../execution/LocalExecutionProcess.ts";
import { matlabRuntimeAdapter } from "./MatlabAdapter.ts";

const MAXIMUM_OUTPUT_BYTES = 4 * 1024 * 1024;
const MAXIMUM_OUTPUT_CHUNK_BYTES = 256 * 1024;
const RUN_HISTORY_DEFAULT_LIMIT = 20;
const OUTPUT_COALESCE_MAX_CHUNK = 64;
const OUTPUT_COALESCE_WINDOW = Duration.millis(25);
const MAXIMUM_VERIFICATION_OUTPUT_BYTES = 512 * 1024;
const MAXIMUM_QUEUED_RUNS_PER_RUNTIME = 100;
const runtimeAdapters: ReadonlyArray<AnalysisRuntimeAdapter> = [matlabRuntimeAdapter];

interface QueuedAnalysisRun {
  readonly runId: ExecutionRunId;
  readonly adapter: AnalysisRuntimeAdapter;
  readonly profile: AnalysisRuntimeProfile;
  readonly absoluteSourcePath: string;
}

type AnalysisOperation = AnalysisOperationError["operation"];
type AnalysisReason = AnalysisOperationError["reason"];

function analysisError(
  operation: AnalysisOperation,
  reason: AnalysisReason,
  message: string,
  cause?: unknown,
): AnalysisOperationError {
  return new AnalysisOperationError({
    operation,
    reason,
    message,
    ...(cause === undefined ? {} : { cause }),
  });
}

function isTerminal(status: ExecutionStatus): boolean {
  return TERMINAL_EXECUTION_STATUSES.has(status);
}

function outputContentHash(chunks: ReadonlyArray<ExecutionOutputChunk>): string {
  const hash = NodeCrypto.createHash("sha256");
  for (const part of executionOutputContentParts(chunks)) hash.update(part, "utf8");
  return `sha256:${hash.digest("hex")}`;
}

/** Interrupted output known to be incomplete or corrupt must not receive a fidelity hash. */
export function recoveredOutputContentHash(
  chunks: ReadonlyArray<ExecutionOutputChunk>,
  outputTruncated: boolean,
): string | null {
  return outputTruncated ? null : outputContentHash(chunks);
}

function utf8Boundary(bytes: Uint8Array, requested: number): number {
  if (requested >= bytes.byteLength) return bytes.byteLength;
  let boundary = requested;
  while (boundary > 0 && (bytes[boundary]! & 0xc0) === 0x80) boundary -= 1;
  return boundary;
}

function decodeUtf8Chunks(bytes: Uint8Array): ReadonlyArray<string> {
  const chunks: string[] = [];
  let offset = 0;
  while (offset < bytes.byteLength) {
    const boundary = utf8Boundary(
      bytes,
      Math.min(bytes.byteLength, offset + MAXIMUM_OUTPUT_CHUNK_BYTES),
    );
    if (boundary <= offset) break;
    chunks.push(new TextDecoder().decode(bytes.subarray(offset, boundary)));
    offset = boundary;
  }
  return chunks;
}

function coalesceProcessOutput(
  outputs: ReadonlyArray<ExecutionProcessOutput>,
): ReadonlyArray<ExecutionProcessOutput> {
  const coalesced: Array<ExecutionProcessOutput> = [];
  for (const output of outputs) {
    if (output.text.length === 0) continue;
    const previous = coalesced.at(-1);
    if (previous?.stream === output.stream) {
      coalesced[coalesced.length - 1] = {
        stream: previous.stream,
        text: previous.text + output.text,
      };
    } else {
      coalesced.push(output);
    }
  }
  return coalesced;
}

function matchesSubscription(
  event: AnalysisRunStreamEvent,
  input: AnalysisSubscribeRunsInput,
): boolean {
  const source = event._tag === "run-output" ? event.source : event.run.source;
  return (
    source.cwd === input.cwd &&
    (input.relativePath === undefined || source.relativePath === input.relativePath)
  );
}

function eventProjectId(event: AnalysisRunStreamEvent): string {
  return event._tag === "run-output" ? event.projectId : event.run.projectId;
}

export class AnalysisService extends Context.Service<
  AnalysisService,
  {
    readonly inspectRuntimes: (
      input: AnalysisInspectRuntimesInput,
    ) => Effect.Effect<AnalysisRuntimeInspection, AnalysisOperationError>;
    readonly configureRuntime: (
      input: AnalysisConfigureRuntimeInput,
    ) => Effect.Effect<AnalysisRuntimeInspection, AnalysisOperationError>;
    readonly verifyRuntime: (
      input: AnalysisVerifyRuntimeInput,
    ) => Effect.Effect<AnalysisRuntimeProfile, AnalysisOperationError>;
    readonly startRun: (
      input: AnalysisStartRunInput,
    ) => Effect.Effect<AnalysisRunSnapshot, AnalysisOperationError>;
    readonly cancelRun: (
      input: AnalysisCancelRunInput,
    ) => Effect.Effect<AnalysisRunSnapshot, AnalysisOperationError>;
    readonly listRuns: (
      input: AnalysisListRunsInput,
    ) => Effect.Effect<AnalysisListRunsResult, AnalysisOperationError>;
    readonly getRun: (
      input: AnalysisGetRunInput,
    ) => Effect.Effect<AnalysisRunSnapshot, AnalysisOperationError>;
    readonly storageSummary: (
      input: AnalysisStorageSummaryInput,
    ) => Effect.Effect<AnalysisStorageSummary, AnalysisOperationError>;
    readonly cleanupRun: (
      input: AnalysisCleanupRunInput,
    ) => Effect.Effect<AnalysisCleanupResult, AnalysisOperationError>;
    readonly cleanupProject: (
      input: AnalysisCleanupProjectInput,
    ) => Effect.Effect<AnalysisCleanupResult, AnalysisOperationError>;
    readonly resolveArtifact: (input: {
      readonly projectId: string;
      readonly runId: ExecutionRunId;
      readonly artifactId: AnalysisArtifactId;
      readonly representationId: AnalysisArtifactRepresentationId;
    }) => Effect.Effect<ResolvedAnalysisArtifactRepresentation | null, AnalysisOperationError>;
    readonly subscribeRuns: (
      input: AnalysisSubscribeRunsInput,
    ) => Effect.Effect<Stream.Stream<AnalysisRunStreamEvent>, AnalysisOperationError, Scope.Scope>;
  }
>()("t3/scient/analysis/AnalysisService") {}

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const hostEnvironment = yield* HostProcessEnvironment;
  const hostPlatform = yield* HostProcessPlatform;
  const processes = yield* LocalExecutionProcess.ExecutionProcess;
  const workspaceFiles = yield* WorkspaceFileSystem.WorkspaceFileSystem;
  const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
  const store = yield* LocalAnalysisStore.LocalAnalysisStore;
  const runIndex = yield* AnalysisRunIndex.AnalysisRunIndex;
  const executionScope = yield* Scope.make("sequential");
  const startLock = yield* Semaphore.make(1);
  const projectLoadLock = yield* Semaphore.make(1);
  const runtimeSemaphores = new Map(
    yield* Effect.forEach(runtimeAdapters, (adapter) =>
      Semaphore.make(1).pipe(Effect.map((semaphore) => [adapter.id, semaphore] as const)),
    ),
  );
  const runtimeWorkQueues = new Map(
    yield* Effect.forEach(runtimeAdapters, (adapter) =>
      Queue.bounded<QueuedAnalysisRun>(MAXIMUM_QUEUED_RUNS_PER_RUNTIME).pipe(
        Effect.map((queue) => [adapter.id, queue] as const),
      ),
    ),
  );
  const runsRef = yield* Ref.make(new Map<string, AnalysisRunSnapshot>());
  const loadedProjectsRef = yield* Ref.make(new Set<string>());
  const dirtyIndexProjectsRef = yield* Ref.make(new Set<string>());
  const runtimeQueuesRef = yield* Ref.make(new Map<string, ReadonlyArray<string>>());
  const handlesRef = yield* Ref.make(new Map<string, ExecutionProcessHandle>());
  const runtimeProfilesRef = yield* Ref.make(new Map<string, AnalysisRuntimeProfile>());
  const verificationCacheRef = yield* Ref.make(
    new Map<
      string,
      {
        readonly executablePath: string;
        readonly verification: AnalysisRuntimeVerification;
      }
    >(),
  );
  const eventSequenceRef = yield* Ref.make(0);
  const pubsub = yield* PubSub.bounded<AnalysisRunStreamEvent>(256);

  yield* Effect.addFinalizer(() => Scope.close(executionScope, Exit.void));

  const nowIso = DateTime.now.pipe(Effect.map(DateTime.formatIso));

  const nextEventSequence = Ref.getAndUpdate(eventSequenceRef, (value) => value + 1);

  const publish = (run: AnalysisRunSnapshot) =>
    Effect.gen(function* () {
      const eventSequence = yield* nextEventSequence;
      yield* PubSub.publish(pubsub, {
        _tag: "run-updated" as const,
        eventSequence,
        run: summarizeAnalysisRun(run),
      });
    });

  const publishOutput = (run: AnalysisRunSnapshot, chunks: ReadonlyArray<ExecutionOutputChunk>) =>
    Effect.gen(function* () {
      const eventSequence = yield* nextEventSequence;
      yield* PubSub.publish(pubsub, {
        _tag: "run-output" as const,
        eventSequence,
        projectId: run.projectId,
        runId: run.receipt.runId,
        source: run.source,
        chunks,
        outputTruncated: run.receipt.outputTruncated,
        outputByteLength: run.receipt.outputByteLength,
      });
    });

  const markIndexDirty = (projectId: string) =>
    Ref.update(dirtyIndexProjectsRef, (projects) => new Set(projects).add(projectId));

  const persist = (operation: AnalysisOperation, run: AnalysisRunSnapshot) =>
    Effect.gen(function* () {
      const mutation = yield* runIndex
        .beginMutation(run.projectId)
        .pipe(
          Effect.mapError((cause) =>
            analysisError(
              operation,
              "persistence-failed",
              "Unable to begin the local analysis history update.",
              cause,
            ),
          ),
        );
      yield* store
        .persistRun(run)
        .pipe(
          Effect.catch((cause) =>
            markIndexDirty(run.projectId).pipe(
              Effect.andThen(
                Effect.fail(
                  analysisError(
                    operation,
                    "persistence-failed",
                    "Unable to persist the local run journal.",
                    cause,
                  ),
                ),
              ),
            ),
          ),
        );
      yield* runIndex
        .upsert(summarizeAnalysisRun(run), mutation)
        .pipe(
          Effect.catch((cause) =>
            markIndexDirty(run.projectId).pipe(
              Effect.andThen(
                Effect.fail(
                  analysisError(
                    operation,
                    "persistence-failed",
                    "The run was saved, but its local history projection could not be updated.",
                    cause,
                  ),
                ),
              ),
            ),
          ),
        );
    });

  const mapIndexLoadError = (message: string) =>
    Effect.mapError((cause: AnalysisRunIndex.AnalysisRunIndexError) =>
      analysisError("list", "persistence-failed", message, cause),
    );

  const putRun = (operation: AnalysisOperation, run: AnalysisRunSnapshot) =>
    Effect.gen(function* () {
      yield* Ref.update(runsRef, (runs) => {
        const next = new Map(runs);
        next.set(run.receipt.runId, run);
        return next;
      });
      yield* persist(operation, run);
      yield* publish(run);
      return run;
    });

  const identityForCwd = (operation: AnalysisOperation, cwd: string) =>
    Effect.tryPromise({
      try: async () => {
        const inspection = await inspectScientProject(cwd);
        return inspection.state === "initialized" ? readScientProjectIdentity(cwd) : null;
      },
      catch: (cause) =>
        analysisError(
          operation,
          "operation-failed",
          "Unable to inspect the Scient project identity.",
          cause,
        ),
    }).pipe(
      Effect.flatMap((identity) =>
        identity === null
          ? Effect.fail(
              analysisError(
                operation,
                "project-not-initialized",
                "Initialize this folder as a Scient project before running analysis files.",
              ),
            )
          : Effect.succeed(identity),
      ),
    );

  const inspectProfile = (adapter: AnalysisRuntimeAdapter, refresh: boolean) =>
    Effect.gen(function* () {
      const cached = (yield* Ref.get(runtimeProfilesRef)).get(adapter.kind);
      if (cached !== undefined && !refresh) return cached;
      const runtimeConfiguration = yield* store
        .readRuntimeExecutablePath(adapter.kind)
        .pipe(
          Effect.mapError((cause) =>
            analysisError(
              "inspect",
              "persistence-failed",
              `Unable to read ${adapter.kind} settings.`,
              cause,
            ),
          ),
        );
      const inspectedAt = yield* nowIso;
      const inspectedProfile = yield* Effect.tryPromise({
        try: () =>
          adapter.inspect({
            inspectedAt,
            platform: hostPlatform,
            environment: hostEnvironment,
            ...(runtimeConfiguration.executablePath === null
              ? {}
              : { customExecutablePath: runtimeConfiguration.executablePath }),
          }),
        catch: (cause) =>
          analysisError(
            "inspect",
            "operation-failed",
            `Unable to inspect the ${adapter.kind} runtime.`,
            cause,
          ),
      });
      const profile =
        runtimeConfiguration.warning === null
          ? inspectedProfile
          : {
              ...inspectedProfile,
              detail: [runtimeConfiguration.warning, inspectedProfile.detail]
                .filter((value): value is string => value !== null)
                .join(" "),
            };
      yield* Ref.update(runtimeProfilesRef, (profiles) =>
        new Map(profiles).set(adapter.kind, profile),
      );
      return profile;
    });

  const inspectRuntimes = (input: AnalysisInspectRuntimesInput) =>
    Effect.map(
      Effect.forEach(runtimeAdapters, (adapter) => inspectProfile(adapter, input.refresh === true)),
      (runtimes) => ({ contractVersion: 1 as const, runtimes }),
    );

  const configureRuntime = (input: AnalysisConfigureRuntimeInput) =>
    Effect.gen(function* () {
      const adapter = runtimeAdapters.find((candidate) => candidate.kind === input.runtimeKind);
      if (adapter === undefined) {
        return yield* analysisError(
          "configure",
          "runtime-invalid",
          `Unsupported analysis runtime '${input.runtimeKind}'.`,
        );
      }
      yield* store
        .writeRuntimeExecutablePath(adapter.kind, input.executablePath)
        .pipe(
          Effect.mapError((cause) =>
            analysisError(
              "configure",
              "persistence-failed",
              `Unable to save ${adapter.kind} settings.`,
              cause,
            ),
          ),
        );
      yield* Ref.update(runtimeProfilesRef, (profiles) => {
        const next = new Map(profiles);
        next.delete(adapter.kind);
        return next;
      });
      yield* Ref.update(verificationCacheRef, (cache) => {
        const next = new Map(cache);
        next.delete(adapter.id);
        return next;
      });
      return yield* inspectRuntimes({ cwd: input.cwd, refresh: true });
    });

  const verifyRuntime = (input: AnalysisVerifyRuntimeInput) =>
    Effect.gen(function* () {
      yield* identityForCwd("verify", input.cwd);
      const adapter = runtimeAdapters.find((candidate) => candidate.id === input.runtimeId);
      if (adapter === undefined || adapter.prepareVerification === undefined) {
        return yield* analysisError(
          "verify",
          "runtime-invalid",
          "The selected analysis runtime cannot be verified by this Scient build.",
        );
      }
      const profile = yield* inspectProfile(adapter, false);
      if (profile.availability !== "available" || profile.executablePath === null) {
        return yield* analysisError(
          "verify",
          profile.availability === "invalid" ? "runtime-invalid" : "runtime-missing",
          profile.detail ?? `${profile.label} is unavailable.`,
        );
      }
      const semaphore = runtimeSemaphores.get(adapter.id);
      if (!semaphore) {
        return yield* analysisError(
          "verify",
          "runtime-invalid",
          `${profile.label} has no local execution lease.`,
        );
      }
      return yield* semaphore.withPermits(1)(
        Effect.gen(function* () {
          const prepared = yield* Effect.tryPromise({
            try: () => adapter.prepareVerification!(profile),
            catch: (cause) =>
              analysisError(
                "verify",
                "process-failed",
                `Unable to prepare the ${profile.label} startup check.`,
                cause,
              ),
          });
          return yield* Effect.gen(function* () {
            const cached = (yield* Ref.get(verificationCacheRef)).get(adapter.id);
            if (
              input.refresh !== true &&
              cached?.executablePath === profile.executablePath &&
              cached.verification.executableIdentity === prepared.executableIdentity
            ) {
              const verifiedProfile = { ...profile, verification: cached.verification };
              yield* Ref.update(runtimeProfilesRef, (profiles) =>
                new Map(profiles).set(adapter.kind, verifiedProfile),
              );
              return verifiedProfile;
            }

            const startedAt = yield* Clock.currentTimeMillis;
            const verifiedAt = yield* nowIso;
            const verificationRunId = ExecutionRunId.make(
              yield* crypto.randomUUIDv4.pipe(
                Effect.mapError((cause) =>
                  analysisError(
                    "verify",
                    "operation-failed",
                    "Unable to create the runtime verification identifier.",
                    cause,
                  ),
                ),
              ),
            );
            const outputsRef = yield* Ref.make<ReadonlyArray<ExecutionProcessOutput>>([]);
            const outputBytesRef = yield* Ref.make(0);
            const captureOutput = (output: ExecutionProcessOutput) =>
              Effect.gen(function* () {
                const usedBytes = yield* Ref.get(outputBytesRef);
                const remaining = Math.max(0, MAXIMUM_VERIFICATION_OUTPUT_BYTES - usedBytes);
                if (remaining === 0) return;
                const encoded = new TextEncoder().encode(output.text);
                const accepted = utf8Boundary(encoded, Math.min(encoded.byteLength, remaining));
                if (accepted === 0) return;
                const text = new TextDecoder().decode(encoded.subarray(0, accepted));
                yield* Ref.update(outputsRef, (outputs) => [...outputs, { ...output, text }]);
                yield* Ref.update(outputBytesRef, (bytes) => bytes + accepted);
              });

            const processResult = yield* processes
              .start({
                runId: verificationRunId,
                executable: prepared.executable,
                args: prepared.args,
                cwd: prepared.cwd,
                environment: prepared.environment,
              })
              .pipe(
                Effect.flatMap((handle) =>
                  Effect.gen(function* () {
                    const outputFiber = yield* handle.output.pipe(
                      Stream.runForEach(captureOutput),
                      Effect.catchCause((cause) =>
                        Effect.logWarning("runtime verification output failed", { cause }),
                      ),
                      Effect.forkScoped,
                    );
                    const outcome = yield* Effect.raceFirst(
                      handle.exitCode.pipe(
                        Effect.map((exitCode) => ({ exitCode, timedOut: false as const })),
                      ),
                      Effect.sleep(Duration.millis(prepared.timeoutMs)).pipe(
                        Effect.as({ exitCode: null, timedOut: true as const }),
                      ),
                    );
                    if (outcome.timedOut) yield* handle.cancel.pipe(Effect.ignoreCause());
                    yield* Fiber.join(outputFiber).pipe(Effect.ignoreCause());
                    return outcome;
                  }),
                ),
                Effect.catch((cause) =>
                  Ref.update(outputsRef, (outputs) => [
                    ...outputs,
                    { stream: "stderr" as const, text: cause.message },
                  ]).pipe(Effect.as({ exitCode: null, timedOut: false as const })),
                ),
                Effect.scoped,
              );
            const outputs = yield* Ref.get(outputsRef);
            const finishedAt = yield* Clock.currentTimeMillis;
            const verification = yield* Effect.tryPromise({
              try: () =>
                prepared.collect({
                  ...processResult,
                  output: outputs,
                  verifiedAt,
                  durationMs: Math.max(0, finishedAt - startedAt),
                }),
              catch: (cause) =>
                analysisError(
                  "verify",
                  "operation-failed",
                  `Unable to read the ${profile.label} startup check.`,
                  cause,
                ),
            });
            const verifiedProfile = {
              ...profile,
              version: verification.version ?? profile.version,
              detail: verification.detail,
              verification,
            } satisfies AnalysisRuntimeProfile;
            yield* Ref.update(runtimeProfilesRef, (profiles) =>
              new Map(profiles).set(adapter.kind, verifiedProfile),
            );
            yield* Ref.update(verificationCacheRef, (cache) => {
              const next = new Map(cache);
              if (verification.status === "ready") {
                next.set(adapter.id, {
                  executablePath: profile.executablePath!,
                  verification,
                });
              } else {
                next.delete(adapter.id);
              }
              return next;
            });
            return verifiedProfile;
          }).pipe(
            Effect.ensuring(
              Effect.tryPromise({
                try: prepared.cleanup,
                catch: (cause) =>
                  analysisError(
                    "verify",
                    "operation-failed",
                    "Unable to remove temporary runtime verification files.",
                    cause,
                  ),
              }).pipe(Effect.ignoreCause()),
            ),
          );
        }),
      );
    });

  const projectRunsNeedLoad = (projectId: string) =>
    Effect.gen(function* () {
      const loaded = yield* Ref.get(loadedProjectsRef);
      const dirty = (yield* Ref.get(dirtyIndexProjectsRef)).has(projectId);
      return !loaded.has(projectId) || dirty;
    });

  const ensureProjectRunsLoaded = (projectId: string) =>
    Effect.gen(function* () {
      if (!(yield* projectRunsNeedLoad(projectId))) return;
      yield* projectLoadLock.withPermits(1)(
        Effect.gen(function* () {
          // The history query and live subscription mount together. Recheck
          // after acquiring the lease so they share one authoritative scan.
          if (!(yield* projectRunsNeedLoad(projectId))) return;
          const memoryDirty = (yield* Ref.get(dirtyIndexProjectsRef)).has(projectId);
          let rebuild =
            memoryDirty ||
            (yield* runIndex
              .needsRebuild(projectId)
              .pipe(mapIndexLoadError("Unable to inspect the local analysis history index.")));
          let loadedRuns: ReadonlyArray<AnalysisRunSnapshot>;
          if (rebuild) {
            loadedRuns = yield* store
              .loadRuns(projectId)
              .pipe(
                Effect.mapError((cause) =>
                  analysisError(
                    "list",
                    "persistence-failed",
                    "Unable to load local run history.",
                    cause,
                  ),
                ),
              );
          } else {
            const nonTerminal = yield* runIndex
              .listNonTerminal(projectId)
              .pipe(mapIndexLoadError("Unable to inspect interrupted analysis runs."));
            const restored = yield* Effect.forEach(
              nonTerminal,
              (summary) =>
                store
                  .loadRun(projectId, summary.receipt.runId)
                  .pipe(
                    Effect.mapError((cause) =>
                      analysisError(
                        "list",
                        "persistence-failed",
                        "Unable to recover an interrupted run.",
                        cause,
                      ),
                    ),
                  ),
              { concurrency: 8 },
            );
            if (restored.some((run) => run === null)) {
              rebuild = true;
              loadedRuns = yield* store
                .loadRuns(projectId)
                .pipe(
                  Effect.mapError((cause) =>
                    analysisError(
                      "list",
                      "persistence-failed",
                      "Unable to rebuild stale local run history.",
                      cause,
                    ),
                  ),
                );
            } else {
              loadedRuns = restored.filter((run): run is AnalysisRunSnapshot => run !== null);
            }
          }
          const observedAt = yield* nowIso;
          const indexedRuns: AnalysisRunSummary[] = [];
          for (const loadedRun of loadedRuns) {
            let run = loadedRun;
            const liveRun = (yield* Ref.get(runsRef)).get(loadedRun.receipt.runId);
            if (liveRun && !isTerminal(liveRun.receipt.status)) {
              // A dirty projection can rebuild while this process is still
              // alive. Its in-memory snapshot is newer than the disk summary
              // and must not be misclassified as restart recovery.
              run = liveRun;
            } else if (!isTerminal(loadedRun.receipt.status)) {
              const recovered = yield* store
                .loadRun(projectId, loadedRun.receipt.runId)
                .pipe(
                  Effect.mapError((cause) =>
                    analysisError(
                      "list",
                      "persistence-failed",
                      "Unable to recover an interrupted run.",
                      cause,
                    ),
                  ),
                );
              const recoveredReceipt = recovered?.receipt ?? loadedRun.receipt;
              run = {
                ...loadedRun,
                receipt: {
                  ...recoveredReceipt,
                  status: "lost" as const,
                  finishedAt: observedAt,
                  failureMessage: "The app stopped before this run reached a terminal state.",
                  outputContentHash: recoveredOutputContentHash(
                    recoveredReceipt.output,
                    recoveredReceipt.outputTruncated,
                  ),
                  output: [],
                },
              };
            }
            if (
              run.localStorage.status === "retained" &&
              run.localStorage.totalBytes === 0 &&
              isTerminal(run.receipt.status)
            ) {
              const measured = yield* store
                .measureRunStorage(projectId, run.receipt.runId)
                .pipe(
                  Effect.mapError((cause) =>
                    analysisError(
                      "list",
                      "persistence-failed",
                      "Unable to measure existing local analysis data.",
                      cause,
                    ),
                  ),
                );
              if (measured.totalBytes > 0) run = { ...run, localStorage: measured };
            }
            if (run !== loadedRun) yield* persist("list", run);
            indexedRuns.push(summarizeAnalysisRun(run));
          }
          if (rebuild) {
            const mutation = yield* runIndex
              .beginMutation(projectId)
              .pipe(mapIndexLoadError("Unable to begin rebuilding local analysis history."));
            const replaced = yield* runIndex
              .replaceProject(projectId, indexedRuns, mutation)
              .pipe(mapIndexLoadError("Unable to rebuild the local analysis history index."));
            if (!replaced) {
              yield* markIndexDirty(projectId);
              return yield* analysisError(
                "list",
                "persistence-failed",
                "Local analysis history changed during recovery. Try loading it again.",
              );
            }
          }
          yield* Ref.update(loadedProjectsRef, (projects) => new Set(projects).add(projectId));
          yield* Ref.update(dirtyIndexProjectsRef, (projects) => {
            const next = new Set(projects);
            next.delete(projectId);
            return next;
          });
        }),
      );
    });

  const updateReceipt = (
    runId: string,
    operation: AnalysisOperation,
    update: (run: AnalysisRunSnapshot) => AnalysisRunSnapshot,
  ) =>
    Effect.gen(function* () {
      const run = yield* Ref.modify(runsRef, (runs) => {
        const current = runs.get(runId);
        if (!current) return [null, runs] as const;
        const nextRun = update(current);
        const next = new Map(runs);
        next.set(runId, nextRun);
        return [nextRun, next] as const;
      });
      if (run === null) {
        return yield* analysisError(
          operation,
          "run-not-found",
          "The analysis run no longer exists.",
        );
      }
      yield* persist(operation, run);
      yield* publish(run);
      return run;
    });

  const removeQueuedRun = (runtimeId: string, runId: string) =>
    Effect.gen(function* () {
      const remaining = yield* Ref.modify(runtimeQueuesRef, (queues) => {
        const next = new Map(queues);
        const queue = (next.get(runtimeId) ?? []).filter((candidate) => candidate !== runId);
        next.set(runtimeId, queue);
        return [queue, next] as const;
      });
      yield* Effect.forEach(
        remaining,
        (queuedRunId, index) =>
          Effect.gen(function* () {
            const run = (yield* Ref.get(runsRef)).get(queuedRunId);
            if (!run || isTerminal(run.receipt.status) || run.queuePosition === index + 1) return;
            yield* updateReceipt(queuedRunId, "start", (current) => ({
              ...current,
              queuePosition: index + 1,
            }));
          }),
        { concurrency: 1, discard: true },
      );
    });

  const appendOutput = (runId: string, outputs: ReadonlyArray<ExecutionProcessOutput>) =>
    Effect.gen(function* () {
      const coalesced = coalesceProcessOutput(outputs);
      if (coalesced.length === 0) return;
      const observedAt = yield* nowIso;
      const update = yield* Ref.modify(runsRef, (runs) => {
        const run = runs.get(runId);
        if (!run) return [null, runs] as const;
        const chunks: ExecutionOutputChunk[] = [];
        let outputByteLength = run.receipt.outputByteLength;
        let outputTruncated = run.receipt.outputTruncated;
        for (const output of coalesced) {
          const remainingBytes = Math.max(0, MAXIMUM_OUTPUT_BYTES - outputByteLength);
          const encoded = new TextEncoder().encode(output.text);
          const acceptedByteLength = utf8Boundary(
            encoded,
            Math.min(encoded.byteLength, remainingBytes),
          );
          for (const acceptedText of decodeUtf8Chunks(encoded.subarray(0, acceptedByteLength))) {
            chunks.push({
              sequence: run.receipt.output.length + chunks.length,
              stream: output.stream,
              text: acceptedText,
              observedAt,
            });
          }
          outputByteLength += acceptedByteLength;
          if (encoded.byteLength > remainingBytes && !outputTruncated) {
            outputTruncated = true;
            chunks.push({
              sequence: run.receipt.output.length + chunks.length,
              stream: "system",
              text: `Output was truncated at the ${MAXIMUM_OUTPUT_BYTES}-byte limit.\n`,
              observedAt,
            });
          }
        }
        if (chunks.length === 0) return [{ run, chunks }, runs] as const;
        const nextRun = {
          ...run,
          receipt: {
            ...run.receipt,
            output: [...run.receipt.output, ...chunks],
            outputTruncated,
            outputByteLength,
          },
        } satisfies AnalysisRunSnapshot;
        const next = new Map(runs);
        next.set(runId, nextRun);
        return [{ run: nextRun, chunks }, next] as const;
      });
      if (update === null || update.chunks.length === 0) return;
      for (const chunk of update.chunks) {
        yield* store
          .appendOutput(update.run.projectId, runId, chunk)
          .pipe(
            Effect.mapError((cause) =>
              analysisError("start", "persistence-failed", "Unable to append run output.", cause),
            ),
          );
      }
      if (update.run.receipt.outputTruncated) yield* persist("start", update.run);
      yield* publishOutput(update.run, update.chunks);
    });

  const finishRun = (
    runId: string,
    status: Extract<ExecutionStatus, "succeeded" | "failed" | "cancelled" | "lost">,
    exitCode: number | null,
    failureMessage: string | null,
  ) =>
    Effect.gen(function* () {
      const finishedAt = yield* nowIso;
      const current = (yield* Ref.get(runsRef)).get(runId);
      const contentHash = current === undefined ? null : outputContentHash(current.receipt.output);
      const localStorage =
        current === undefined
          ? null
          : yield* store
              .measureRunStorage(current.projectId, runId)
              .pipe(
                Effect.mapError((cause) =>
                  analysisError(
                    "start",
                    "persistence-failed",
                    "Unable to measure retained local run data.",
                    cause,
                  ),
                ),
              );
      const finished = yield* updateReceipt(runId, "start", (run) => ({
        ...run,
        phase: "finished",
        queuePosition: null,
        ...(localStorage === null ? {} : { localStorage }),
        receipt: {
          ...run.receipt,
          status: transitionExecutionStatus(run.receipt.status, status),
          exitCode,
          failureMessage,
          finishedAt,
          outputContentHash: contentHash,
        },
      }));
      yield* Ref.update(runsRef, (runs) => {
        const next = new Map(runs);
        next.set(runId, {
          ...finished,
          receipt: { ...finished.receipt, output: [] },
        });
        return next;
      });
      return finished;
    });

  const runProcess = (
    runId: string,
    adapter: AnalysisRuntimeAdapter,
    profile: AnalysisRuntimeProfile,
    absoluteSourcePath: string,
  ) => {
    const execute = Effect.scoped(
      Effect.gen(function* () {
        const startingRun = yield* updateReceipt(runId, "start", (run) => ({
          ...run,
          phase: "launching",
          queuePosition: null,
          receipt: {
            ...run.receipt,
            status: transitionExecutionStatus(run.receipt.status, "starting"),
          },
        }));
        const artifactStagingDirectory = yield* store
          .prepareArtifactStaging(startingRun.projectId, startingRun.receipt.runId)
          .pipe(
            Effect.mapError((cause) =>
              analysisError(
                "start",
                "persistence-failed",
                "Unable to prepare local artifact capture.",
                cause,
              ),
            ),
          );
        const runContext = {
          runId: startingRun.receipt.runId,
          projectId: startingRun.projectId,
          runtime: profile,
          source: startingRun.source,
          absoluteSourcePath,
          artifactStagingDirectory,
        };
        const prepareRun = adapter.prepareRun;
        if (prepareRun) {
          yield* Effect.tryPromise({
            try: () => prepareRun(runContext),
            catch: (cause) =>
              analysisError(
                "start",
                "process-failed",
                `Unable to prepare the ${profile.label} run.`,
                cause,
              ),
          });
        }
        const prepared = adapter.prepare(runContext);
        const handle = yield* processes.start({
          runId: startingRun.receipt.runId,
          executable: prepared.executable,
          args: prepared.args,
          cwd: prepared.cwd,
          environment: prepared.environment,
        });
        yield* Ref.update(handlesRef, (handles) => new Map(handles).set(runId, handle));
        const running = yield* updateReceipt(runId, "start", (run) => ({
          ...run,
          phase: "running",
          receipt: {
            ...run.receipt,
            status: transitionExecutionStatus(run.receipt.status, "running"),
          },
        }));
        if (running.receipt.cancellationRequested) {
          yield* handle.cancel;
        }
        const [, exitCode] = yield* Effect.all(
          [
            handle.output.pipe(
              Stream.groupedWithin(OUTPUT_COALESCE_MAX_CHUNK, OUTPUT_COALESCE_WINDOW),
              Stream.runForEach((outputs) => appendOutput(runId, outputs)),
            ),
            handle.exitCode,
          ],
          { concurrency: 2 },
        );
        yield* updateReceipt(runId, "start", (run) => ({ ...run, phase: "capturing" }));
        if (adapter.collectDiagnostics) {
          const diagnostics = yield* Effect.promise(() =>
            adapter.collectDiagnostics!(runContext),
          ).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("analysis diagnostic collection failed", { runId, cause }).pipe(
                Effect.as([]),
              ),
            ),
          );
          if (diagnostics.length > 0) {
            yield* updateReceipt(runId, "start", (run) => ({ ...run, diagnostics }));
          }
        }
        const collectArtifacts = adapter.collectArtifacts;
        if (collectArtifacts) {
          const createdAt = yield* nowIso;
          const collection = yield* Effect.gen(function* () {
            const collected = yield* Effect.tryPromise({
              try: () => collectArtifacts(runContext),
              catch: (cause) =>
                analysisError(
                  "start",
                  "persistence-failed",
                  `Unable to inspect ${profile.label} artifacts.`,
                  cause,
                ),
            });
            yield* updateReceipt(runId, "start", (run) => ({ ...run, phase: "publishing" }));
            const artifacts = yield* store
              .publishArtifacts({
                projectId: startingRun.projectId,
                runId: startingRun.receipt.runId,
                createdAt,
                candidates: collected.candidates,
              })
              .pipe(
                Effect.mapError((cause) =>
                  analysisError(
                    "start",
                    "persistence-failed",
                    `Unable to publish ${profile.label} artifacts.`,
                    cause,
                  ),
                ),
              );
            return {
              status:
                collected.failureMessage === null ? ("succeeded" as const) : ("failed" as const),
              artifacts,
              failureMessage: collected.failureMessage,
            };
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.gen(function* () {
                yield* Effect.logWarning("analysis artifact capture failed", { runId, cause });
                yield* store
                  .discardArtifactStaging(startingRun.projectId, startingRun.receipt.runId)
                  .pipe(Effect.ignoreCause({ log: true }));
                return {
                  status: "failed" as const,
                  artifacts: [],
                  failureMessage: `${profile.label} finished, but Scient could not collect its generated figures.`,
                };
              }),
            ),
          );
          yield* updateReceipt(runId, "start", (run) => ({
            ...run,
            artifacts: collection.artifacts,
            artifactReceipt: {
              status: collection.status,
              failureMessage: collection.failureMessage,
            },
          }));
        } else {
          yield* store
            .discardArtifactStaging(startingRun.projectId, startingRun.receipt.runId)
            .pipe(Effect.ignoreCause({ log: true }));
        }
        const latest = (yield* Ref.get(runsRef)).get(runId);
        if (latest?.receipt.cancellationRequested) {
          yield* finishRun(runId, "cancelled", exitCode, null);
        } else if (exitCode === 0) {
          yield* finishRun(runId, "succeeded", 0, null);
        } else {
          yield* finishRun(
            runId,
            "failed",
            exitCode,
            `${profile.label} exited with code ${exitCode}.`,
          );
        }
      }),
    ).pipe(
      Effect.catchCause((cause) =>
        Effect.gen(function* () {
          let latest = (yield* Ref.get(runsRef)).get(runId);
          if (!latest || isTerminal(latest.receipt.status)) return;
          const status = latest.receipt.cancellationRequested
            ? "cancelled"
            : Cause.hasInterruptsOnly(cause)
              ? "lost"
              : "failed";
          if (latest.artifactReceipt.status === "pending") {
            yield* updateReceipt(runId, "start", (run) => ({
              ...run,
              artifactReceipt: {
                status: "failed",
                failureMessage: `The ${profile.label} run ended before Scient could collect generated figures.`,
              },
            })).pipe(Effect.ignoreCause({ log: true }));
            latest = (yield* Ref.get(runsRef)).get(runId) ?? latest;
          }
          yield* finishRun(
            runId,
            status,
            null,
            status === "cancelled"
              ? null
              : status === "lost"
                ? `Scient stopped before the ${profile.label} run completed.`
                : `${profile.label} could not be started or completed.`,
          ).pipe(Effect.ignoreCause({ log: true }));
          if (status === "failed") {
            yield* Ref.update(runtimeProfilesRef, (profiles) => {
              const next = new Map(profiles);
              next.delete(adapter.kind);
              return next;
            });
            yield* Ref.update(verificationCacheRef, (cache) => {
              const next = new Map(cache);
              next.delete(adapter.id);
              return next;
            });
          }
          yield* Effect.logWarning("analysis run process failed", { runId, cause });
        }),
      ),
      Effect.ensuring(
        Effect.all([
          Ref.update(handlesRef, (handles) => {
            const next = new Map(handles);
            next.delete(runId);
            return next;
          }),
          Effect.gen(function* () {
            const run = (yield* Ref.get(runsRef)).get(runId);
            if (run) {
              yield* store
                .discardArtifactStaging(run.projectId, runId)
                .pipe(Effect.ignoreCause({ log: true }));
            }
            yield* Ref.update(runsRef, (runs) => {
              const current = runs.get(runId);
              if (!current || !isTerminal(current.receipt.status)) return runs;
              const next = new Map(runs);
              next.delete(runId);
              return next;
            });
          }),
        ]).pipe(Effect.asVoid),
      ),
    );
    const semaphore = runtimeSemaphores.get(adapter.id);
    if (!semaphore) return execute;
    return semaphore.withPermits(1)(
      Effect.gen(function* () {
        yield* removeQueuedRun(adapter.id, runId);
        const run = (yield* Ref.get(runsRef)).get(runId);
        if (!run || isTerminal(run.receipt.status)) return;
        if (run.receipt.cancellationRequested) {
          yield* finishRun(runId, "cancelled", null, null);
          return;
        }
        yield* execute;
      }),
    );
  };

  const startRun = (input: AnalysisStartRunInput) =>
    startLock.withPermits(1)(
      Effect.gen(function* () {
        const adapter = runtimeAdapters.find((candidate) => candidate.id === input.runtimeId);
        if (adapter === undefined) {
          return yield* analysisError(
            "start",
            "runtime-invalid",
            "The selected analysis runtime is unavailable.",
          );
        }
        const lowerPath = input.relativePath.toLowerCase();
        if (!adapter.fileExtensions.some((extension) => lowerPath.endsWith(extension))) {
          return yield* analysisError(
            "start",
            "invalid-source",
            `${adapter.kind} cannot run this source file.`,
          );
        }
        const sourceValidationMessage = adapter.validateSource?.({
          cwd: input.cwd,
          relativePath: input.relativePath,
          revision: input.sourceRevision,
        });
        if (sourceValidationMessage) {
          return yield* analysisError("start", "invalid-source", sourceValidationMessage);
        }
        const identity = yield* identityForCwd("start", input.cwd);
        yield* ensureProjectRunsLoaded(identity.projectId);
        const activeRun = [...(yield* Ref.get(runsRef)).values()].find(
          (run) =>
            run.projectId === identity.projectId &&
            run.source.relativePath === input.relativePath &&
            !isTerminal(run.receipt.status),
        );
        if (activeRun) {
          return yield* analysisError(
            "start",
            "run-already-active",
            "This analysis file already has an active run.",
          );
        }
        const file = yield* workspaceFiles
          .readFile({ cwd: input.cwd, relativePath: input.relativePath })
          .pipe(
            Effect.mapError((cause) =>
              analysisError(
                "start",
                "invalid-source",
                `Unable to read the ${adapter.kind} source file.`,
                cause,
              ),
            ),
          );
        if (file.truncated) {
          return yield* analysisError(
            "start",
            "invalid-source",
            "This file exceeds the editable source limit and cannot be run from the viewer.",
          );
        }
        if (file.revision !== input.sourceRevision) {
          return yield* analysisError(
            "start",
            "source-changed",
            "The source file changed after it was opened. Wait for save or reload it before running.",
          );
        }
        const profile = yield* inspectProfile(adapter, false);
        if (profile.availability !== "available" || profile.executablePath === null) {
          return yield* analysisError(
            "start",
            profile.availability === "invalid" ? "runtime-invalid" : "runtime-missing",
            profile.detail ?? `${profile.label} is unavailable.`,
          );
        }
        if (!profile.capabilities.includes("run-file")) {
          return yield* analysisError(
            "start",
            "runtime-invalid",
            `${profile.label} does not advertise Run File support.`,
          );
        }
        const target = yield* workspacePaths
          .resolveRelativePathWithinRoot({
            workspaceRoot: input.cwd,
            relativePath: input.relativePath,
          })
          .pipe(
            Effect.mapError((cause) =>
              analysisError("start", "invalid-source", "The source path is invalid.", cause),
            ),
          );
        const runId = ExecutionRunId.make(
          yield* crypto.randomUUIDv4.pipe(
            Effect.mapError((cause) =>
              analysisError(
                "start",
                "operation-failed",
                "Unable to create the analysis run identifier.",
                cause,
              ),
            ),
          ),
        );
        const startedAt = yield* nowIso;
        const queuedRunCount = (yield* Ref.get(runtimeQueuesRef)).get(adapter.id)?.length ?? 0;
        if (queuedRunCount >= MAXIMUM_QUEUED_RUNS_PER_RUNTIME) {
          return yield* analysisError(
            "start",
            "runtime-queue-full",
            `${profile.label} already has ${MAXIMUM_QUEUED_RUNS_PER_RUNTIME} waiting runs. Cancel a queued run before starting another.`,
          );
        }
        const queuePosition = queuedRunCount + 1;
        const run: AnalysisRunSnapshot = {
          contractVersion: 1,
          projectId: identity.projectId,
          action: "run-file",
          runtime: profile,
          source: {
            cwd: input.cwd,
            relativePath: file.relativePath,
            revision: AnalysisSourceRevision.make(file.revision),
          },
          phase: "waiting",
          queuePosition,
          diagnostics: [],
          artifacts: [],
          artifactReceipt: {
            status: adapter.collectArtifacts ? "pending" : "not-requested",
            failureMessage: null,
          },
          localStorage: {
            status: "retained",
            outputBytes: 0,
            artifactBytes: 0,
            totalBytes: 0,
            removedAt: null,
          },
          receipt: {
            runId,
            status: "queued",
            startedAt,
            finishedAt: null,
            exitCode: null,
            failureMessage: null,
            cancellationRequested: false,
            outputTruncated: false,
            outputByteLength: 0,
            outputContentHash: null,
            output: [],
          },
        };
        yield* putRun("start", run);
        yield* Ref.update(runtimeQueuesRef, (queues) => {
          const next = new Map(queues);
          next.set(adapter.id, [...(next.get(adapter.id) ?? []), runId]);
          return next;
        });
        const runtimeWorkQueue = runtimeWorkQueues.get(adapter.id);
        if (!runtimeWorkQueue) {
          return yield* analysisError(
            "start",
            "runtime-invalid",
            `${profile.label} has no local execution queue.`,
          );
        }
        yield* Queue.offer(runtimeWorkQueue, {
          runId,
          adapter,
          profile,
          absoluteSourcePath: target.absolutePath,
        });
        return run;
      }),
    );

  const cancelRun = (input: AnalysisCancelRunInput) =>
    Effect.gen(function* () {
      const identity = yield* identityForCwd("cancel", input.cwd);
      yield* ensureProjectRunsLoaded(identity.projectId);
      const run = (yield* Ref.get(runsRef)).get(input.runId);
      if (!run || run.projectId !== identity.projectId) {
        return yield* analysisError(
          "cancel",
          "run-not-found",
          "The analysis run no longer exists.",
        );
      }
      if (isTerminal(run.receipt.status)) {
        return yield* analysisError(
          "cancel",
          "run-already-finished",
          "This analysis run has already finished.",
        );
      }
      const updated = yield* updateReceipt(input.runId, "cancel", (current) => ({
        ...current,
        receipt: { ...current.receipt, cancellationRequested: true },
      }));
      const handle = (yield* Ref.get(handlesRef)).get(input.runId);
      if (updated.receipt.status === "queued" && !handle) {
        yield* removeQueuedRun(updated.runtime.id, input.runId);
        const finished = yield* finishRun(input.runId, "cancelled", null, null);
        yield* Ref.update(runsRef, (runs) => {
          const next = new Map(runs);
          next.delete(input.runId);
          return next;
        });
        return finished;
      }
      if (handle) {
        yield* handle.cancel.pipe(
          Effect.mapError((cause) =>
            analysisError(
              "cancel",
              "process-failed",
              `Unable to stop the ${run.runtime.label} process tree.`,
              cause,
            ),
          ),
        );
      }
      return (yield* Ref.get(runsRef)).get(input.runId) ?? updated;
    });

  const listRuns = (input: AnalysisListRunsInput) =>
    Effect.gen(function* () {
      const identity = yield* identityForCwd("list", input.cwd);
      yield* ensureProjectRunsLoaded(identity.projectId);
      const limit = input.limit ?? RUN_HISTORY_DEFAULT_LIMIT;
      const indexInput = {
        projectId: identity.projectId,
        ...(input.relativePath === undefined ? {} : { relativePath: input.relativePath }),
        limit,
        ...(input.cursor === undefined ? {} : { cursor: input.cursor }),
      };
      const mapIndexError = (cause: AnalysisRunIndex.AnalysisRunIndexError) =>
        analysisError(
          "list",
          cause.operation === "decode-cursor" ? "invalid-cursor" : "persistence-failed",
          cause.operation === "decode-cursor"
            ? "The analysis history cursor is invalid. Refresh the run history and try again."
            : "Unable to query the local analysis history index.",
          cause,
        );
      return yield* runIndex.list(indexInput).pipe(
        Effect.catch((cause) =>
          cause.operation === "decode-cursor"
            ? Effect.fail(mapIndexError(cause))
            : Effect.gen(function* () {
                yield* Effect.logWarning(
                  "analysis history projection read failed; rebuilding from canonical receipts",
                  { projectId: identity.projectId, cause },
                );
                yield* Ref.update(dirtyIndexProjectsRef, (projects) =>
                  new Set(projects).add(identity.projectId),
                );
                yield* ensureProjectRunsLoaded(identity.projectId);
                return yield* runIndex.list(indexInput).pipe(Effect.mapError(mapIndexError));
              }),
        ),
      );
    });

  const getRun = (input: AnalysisGetRunInput) =>
    Effect.gen(function* () {
      const identity = yield* identityForCwd("get", input.cwd);
      yield* ensureProjectRunsLoaded(identity.projectId);
      const run = (yield* Ref.get(runsRef)).get(input.runId);
      if (run && run.projectId !== identity.projectId) {
        return yield* analysisError("get", "run-not-found", "The analysis run no longer exists.");
      }
      if (run && !isTerminal(run.receipt.status)) return run;
      const persistedRun = yield* store
        .loadRun(identity.projectId, input.runId)
        .pipe(
          Effect.mapError((cause) =>
            analysisError(
              "get",
              "persistence-failed",
              "Unable to load the local run output.",
              cause,
            ),
          ),
        );
      if (persistedRun === null) {
        return yield* analysisError("get", "run-not-found", "The analysis run no longer exists.");
      }
      return persistedRun;
    });

  const storageSummary = (input: AnalysisStorageSummaryInput) =>
    Effect.gen(function* () {
      const identity = yield* identityForCwd("storage", input.cwd);
      yield* ensureProjectRunsLoaded(identity.projectId);
      return yield* runIndex
        .storageSummary({
          projectId: identity.projectId,
          ...(input.relativePath === undefined ? {} : { relativePath: input.relativePath }),
        })
        .pipe(
          Effect.mapError((cause) =>
            analysisError(
              "storage",
              "persistence-failed",
              "Unable to calculate local analysis storage.",
              cause,
            ),
          ),
        );
    });

  const cleanPersistedRun = (projectId: string, runId: ExecutionRunId) =>
    Effect.gen(function* () {
      const live = (yield* Ref.get(runsRef)).get(runId);
      if (live && !isTerminal(live.receipt.status)) {
        return yield* analysisError(
          "cleanup",
          "run-already-active",
          "Wait for this analysis run to finish before removing its local data.",
        );
      }
      const run = yield* store
        .loadRun(projectId, runId)
        .pipe(
          Effect.mapError((cause) =>
            analysisError(
              "cleanup",
              "persistence-failed",
              "Unable to load the analysis run before cleanup.",
              cause,
            ),
          ),
        );
      if (run === null) {
        return yield* analysisError(
          "cleanup",
          "run-not-found",
          "The analysis run no longer exists.",
        );
      }
      if (!isTerminal(run.receipt.status)) {
        return yield* analysisError(
          "cleanup",
          "run-already-active",
          "Wait for this analysis run to finish before removing its local data.",
        );
      }
      if (run.localStorage.status === "metadata-only") {
        return { cleaned: false, freedBytes: 0 } as const;
      }
      const mutation = yield* runIndex
        .beginMutation(projectId)
        .pipe(
          Effect.mapError((cause) =>
            analysisError(
              "cleanup",
              "persistence-failed",
              "Unable to begin the local cleanup transaction.",
              cause,
            ),
          ),
        );
      const measured = yield* store.measureRunStorage(projectId, runId).pipe(
        Effect.mapError((cause) =>
          analysisError(
            "cleanup",
            "persistence-failed",
            "Unable to measure the local run data before cleanup.",
            cause,
          ),
        ),
        Effect.tapError(() => markIndexDirty(projectId)),
      );
      const removedAt = yield* nowIso;
      const cleaned = yield* store.removeDisposableRunData(run, removedAt).pipe(
        Effect.mapError((cause) =>
          analysisError(
            "cleanup",
            "persistence-failed",
            "Unable to remove the local output and artifacts.",
            cause,
          ),
        ),
        Effect.tapError(() => markIndexDirty(projectId)),
      );
      yield* runIndex.upsert(summarizeAnalysisRun(cleaned), mutation).pipe(
        Effect.mapError((cause) =>
          analysisError(
            "cleanup",
            "persistence-failed",
            "Local data was removed, but the history index could not be updated.",
            cause,
          ),
        ),
        Effect.tapError(() => markIndexDirty(projectId)),
      );
      yield* Ref.update(runsRef, (runs) => {
        if (!runs.has(runId)) return runs;
        const next = new Map(runs);
        next.set(runId, cleaned);
        return next;
      });
      yield* publish(cleaned);
      return { cleaned: true, freedBytes: measured.totalBytes } as const;
    });

  const cleanupRun = (input: AnalysisCleanupRunInput) =>
    Effect.gen(function* () {
      const identity = yield* identityForCwd("cleanup", input.cwd);
      yield* ensureProjectRunsLoaded(identity.projectId);
      const cleaned = yield* cleanPersistedRun(identity.projectId, input.runId);
      const storage = yield* runIndex
        .storageSummary({ projectId: identity.projectId })
        .pipe(
          Effect.mapError((cause) =>
            analysisError(
              "cleanup",
              "persistence-failed",
              "Unable to refresh local analysis storage after cleanup.",
              cause,
            ),
          ),
        );
      return {
        cleanedRunCount: cleaned.cleaned ? 1 : 0,
        freedBytes: cleaned.freedBytes,
        storage,
      };
    });

  const cleanupProject = (input: AnalysisCleanupProjectInput) =>
    Effect.gen(function* () {
      const identity = yield* identityForCwd("cleanup", input.cwd);
      yield* ensureProjectRunsLoaded(identity.projectId);
      const before = yield* runIndex
        .storageSummary({ projectId: identity.projectId })
        .pipe(
          Effect.mapError((cause) =>
            analysisError(
              "cleanup",
              "persistence-failed",
              "Unable to confirm local analysis storage before cleanup.",
              cause,
            ),
          ),
        );
      if (before.totalBytes !== input.expectedRetainedBytes) {
        return yield* analysisError(
          "cleanup",
          "operation-failed",
          "Local analysis storage changed. Review the updated summary before cleaning it up.",
        );
      }
      const retainedRuns = yield* runIndex
        .listRetainedTerminal(identity.projectId)
        .pipe(
          Effect.mapError((cause) =>
            analysisError(
              "cleanup",
              "persistence-failed",
              "Unable to list retained analysis runs for cleanup.",
              cause,
            ),
          ),
        );
      let freedBytes = 0;
      let cleanedRunCount = 0;
      for (const run of retainedRuns) {
        const cleaned = yield* cleanPersistedRun(identity.projectId, run.receipt.runId);
        freedBytes += cleaned.freedBytes;
        if (cleaned.cleaned) cleanedRunCount += 1;
      }
      const storage = yield* runIndex
        .storageSummary({ projectId: identity.projectId })
        .pipe(
          Effect.mapError((cause) =>
            analysisError(
              "cleanup",
              "persistence-failed",
              "Unable to refresh local analysis storage after cleanup.",
              cause,
            ),
          ),
        );
      return { cleanedRunCount, freedBytes, storage };
    });

  const resolveArtifact = (input: {
    readonly projectId: string;
    readonly runId: ExecutionRunId;
    readonly artifactId: AnalysisArtifactId;
    readonly representationId: AnalysisArtifactRepresentationId;
  }) =>
    store
      .resolveArtifact(input)
      .pipe(
        Effect.mapError((cause) =>
          analysisError(
            "get",
            "persistence-failed",
            "Unable to resolve the analysis artifact.",
            cause,
          ),
        ),
      );

  const subscribeRuns = (input: AnalysisSubscribeRunsInput) =>
    Effect.gen(function* () {
      const identity = yield* identityForCwd("subscribe", input.cwd);
      yield* ensureProjectRunsLoaded(identity.projectId);
      const subscription = yield* PubSub.subscribe(pubsub);
      const boundarySequence = yield* Ref.get(eventSequenceRef);
      const matchingRuns = [...(yield* Ref.get(runsRef)).values()]
        .filter(
          (run) =>
            run.projectId === identity.projectId &&
            matchesSubscription(
              { _tag: "run-snapshot", eventSequence: boundarySequence, run },
              input,
            ),
        )
        .toSorted((left, right) => left.receipt.startedAt.localeCompare(right.receipt.startedAt));
      const latestRunId = matchingRuns.at(-1)?.receipt.runId;
      const snapshots = matchingRuns
        .filter((run) => !isTerminal(run.receipt.status) || run.receipt.runId === latestRunId)
        .map((run) => ({
          _tag: "run-snapshot" as const,
          eventSequence: boundarySequence,
          run,
        }));
      return Stream.concat(
        Stream.fromIterable(snapshots),
        Stream.fromSubscription(subscription).pipe(
          Stream.filter(
            (event) =>
              event.eventSequence >= boundarySequence &&
              eventProjectId(event) === identity.projectId &&
              matchesSubscription(event, input),
          ),
        ),
      );
    });

  yield* Effect.forEach(
    runtimeWorkQueues.values(),
    (runtimeWorkQueue) =>
      Stream.fromQueue(runtimeWorkQueue).pipe(
        Stream.runForEach((queued) =>
          runProcess(queued.runId, queued.adapter, queued.profile, queued.absoluteSourcePath).pipe(
            Effect.catchCause((cause) =>
              Effect.logError("analysis runtime queue worker failed", {
                runId: queued.runId,
                runtimeId: queued.adapter.id,
                cause,
              }),
            ),
          ),
        ),
        Effect.forkIn(executionScope),
      ),
    { discard: true },
  );

  return AnalysisService.of({
    inspectRuntimes,
    configureRuntime,
    verifyRuntime,
    startRun,
    cancelRun,
    listRuns,
    getRun,
    storageSummary,
    cleanupRun,
    cleanupProject,
    resolveArtifact,
    subscribeRuns,
  });
});

export const layer = Layer.effect(AnalysisService, make);
