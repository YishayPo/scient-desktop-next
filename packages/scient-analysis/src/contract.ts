import {
  ExecutionOutputChunk,
  ExecutionReceipt,
  ExecutionReceiptSummary,
  ExecutionRunId,
} from "@scientfactory/execution";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const Identifier = Schema.NonEmptyString.check(Schema.isMaxLength(128));
const PathString = Schema.NonEmptyString.check(Schema.isMaxLength(32 * 1024));

export const ANALYSIS_CONTRACT_VERSION = 1 as const;

export const AnalysisRuntimeId = Identifier.pipe(Schema.brand("AnalysisRuntimeId"));
export type AnalysisRuntimeId = typeof AnalysisRuntimeId.Type;

export const AnalysisRuntimeKind = Schema.NonEmptyString.check(Schema.isMaxLength(64));
export type AnalysisRuntimeKind = typeof AnalysisRuntimeKind.Type;

export const AnalysisRuntimeCapability = Schema.Literals([
  "run-file",
  "stream-output",
  "cancel-process-tree",
  "capture-artifacts",
]);
export type AnalysisRuntimeCapability = typeof AnalysisRuntimeCapability.Type;

export const AnalysisRuntimeAvailability = Schema.Literals([
  "available",
  "missing",
  "invalid",
  "unchecked",
]);
export type AnalysisRuntimeAvailability = typeof AnalysisRuntimeAvailability.Type;

export const AnalysisRuntimeSource = Schema.Literals([
  "custom",
  "path",
  "conventional-install",
  "unconfigured",
]);
export type AnalysisRuntimeSource = typeof AnalysisRuntimeSource.Type;

export const AnalysisRuntimeProfile = Schema.Struct({
  id: AnalysisRuntimeId,
  kind: AnalysisRuntimeKind,
  label: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  availability: AnalysisRuntimeAvailability,
  source: AnalysisRuntimeSource,
  executablePath: Schema.NullOr(PathString),
  version: Schema.NullOr(Schema.String.check(Schema.isMaxLength(256))),
  detail: Schema.NullOr(Schema.String.check(Schema.isMaxLength(2048))),
  capabilities: Schema.Array(AnalysisRuntimeCapability),
  inspectedAt: Schema.String,
  verification: Schema.NullOr(
    Schema.Struct({
      status: Schema.Literals([
        "ready",
        "needs-sign-in",
        "license-unavailable",
        "missing-dependency",
        "startup-failed",
        "timed-out",
        "unknown",
      ]),
      verifiedAt: Schema.String,
      durationMs: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
      executableIdentity: Schema.NonEmptyString.check(Schema.isMaxLength(2048)),
      release: Schema.NullOr(Schema.String.check(Schema.isMaxLength(256))),
      version: Schema.NullOr(Schema.String.check(Schema.isMaxLength(256))),
      architecture: Schema.NullOr(Schema.String.check(Schema.isMaxLength(128))),
      installationRoot: Schema.NullOr(PathString),
      javaAvailable: Schema.NullOr(Schema.Boolean),
      javaVersion: Schema.NullOr(Schema.String.check(Schema.isMaxLength(1024))),
      toolboxes: Schema.Array(
        Schema.Struct({
          name: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
          version: Schema.NullOr(Schema.String.check(Schema.isMaxLength(128))),
        }),
      ),
      detail: Schema.NonEmptyString.check(Schema.isMaxLength(4096)),
    }),
  ).pipe(Schema.withDecodingDefaultKey(Effect.succeed(null))),
});
export type AnalysisRuntimeProfile = typeof AnalysisRuntimeProfile.Type;

export type AnalysisRuntimeVerification = NonNullable<AnalysisRuntimeProfile["verification"]>;

export const AnalysisRuntimeInspection = Schema.Struct({
  contractVersion: Schema.Literal(ANALYSIS_CONTRACT_VERSION),
  runtimes: Schema.Array(AnalysisRuntimeProfile),
});
export type AnalysisRuntimeInspection = typeof AnalysisRuntimeInspection.Type;

export const AnalysisSourceRevision = Schema.NonEmptyString.check(Schema.isMaxLength(256)).pipe(
  Schema.brand("AnalysisSourceRevision"),
);
export type AnalysisSourceRevision = typeof AnalysisSourceRevision.Type;

export const AnalysisRunSource = Schema.Struct({
  cwd: PathString,
  relativePath: PathString,
  revision: AnalysisSourceRevision,
});
export type AnalysisRunSource = typeof AnalysisRunSource.Type;

export const AnalysisRunAction = Schema.Literals([
  "run-file",
  "run-selection",
  "run-cell",
  "run-task",
]);
export type AnalysisRunAction = typeof AnalysisRunAction.Type;

export const AnalysisRunPhase = Schema.Literals([
  "verifying-source",
  "waiting",
  "launching",
  "running",
  "capturing",
  "publishing",
  "finished",
]);
export type AnalysisRunPhase = typeof AnalysisRunPhase.Type;

export const AnalysisDiagnosticFrame = Schema.Struct({
  relativePath: Schema.NullOr(PathString),
  functionName: Schema.NullOr(Schema.String.check(Schema.isMaxLength(512))),
  line: Schema.NullOr(Schema.Int.check(Schema.isGreaterThanOrEqualTo(1))),
  column: Schema.NullOr(Schema.Int.check(Schema.isGreaterThanOrEqualTo(1))),
});
export type AnalysisDiagnosticFrame = typeof AnalysisDiagnosticFrame.Type;

export const AnalysisRelatedDiagnostic = Schema.Struct({
  code: Schema.NullOr(Schema.String.check(Schema.isMaxLength(512))),
  message: Schema.NonEmptyString.check(Schema.isMaxLength(16 * 1024)),
  frames: Schema.Array(AnalysisDiagnosticFrame),
});
export type AnalysisRelatedDiagnostic = typeof AnalysisRelatedDiagnostic.Type;

export const AnalysisDiagnostic = Schema.Struct({
  diagnosticId: Identifier,
  severity: Schema.Literals(["error", "warning", "information", "info"]),
  source: Schema.Literals(["runtime", "code-analysis", "test", "environment"]),
  code: Schema.NullOr(Schema.String.check(Schema.isMaxLength(512))),
  message: Schema.NonEmptyString.check(Schema.isMaxLength(16 * 1024)),
  relativePath: Schema.NullOr(PathString),
  line: Schema.NullOr(Schema.Int.check(Schema.isGreaterThanOrEqualTo(1))),
  column: Schema.NullOr(Schema.Int.check(Schema.isGreaterThanOrEqualTo(1))),
  frames: Schema.Array(AnalysisDiagnosticFrame),
  related: Schema.Array(AnalysisRelatedDiagnostic).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed([])),
  ),
});
export type AnalysisDiagnostic = typeof AnalysisDiagnostic.Type;

export const AnalysisArtifactId = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(128),
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._-]*$/u),
).pipe(Schema.brand("AnalysisArtifactId"));
export type AnalysisArtifactId = typeof AnalysisArtifactId.Type;

export const AnalysisArtifactRepresentationId = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(128),
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._-]*$/u),
).pipe(Schema.brand("AnalysisArtifactRepresentationId"));
export type AnalysisArtifactRepresentationId = typeof AnalysisArtifactRepresentationId.Type;

export const AnalysisArtifactFileName = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(255),
  // eslint-disable-next-line no-control-regex -- local artifact names must reject NUL explicitly.
  Schema.isPattern(/^[^/\\\0]+$/u),
).pipe(Schema.brand("AnalysisArtifactFileName"));
export type AnalysisArtifactFileName = typeof AnalysisArtifactFileName.Type;

export const AnalysisArtifactContentHash = Schema.String.check(
  Schema.isPattern(/^sha256:[0-9a-f]{64}$/u),
).pipe(Schema.brand("AnalysisArtifactContentHash"));
export type AnalysisArtifactContentHash = typeof AnalysisArtifactContentHash.Type;

export const AnalysisArtifactMediaType = Schema.Literals([
  "image/png",
  "image/svg+xml",
  "text/html",
  "application/pdf",
  "application/vnd.mathworks.matlab.figure",
]);
export type AnalysisArtifactMediaType = typeof AnalysisArtifactMediaType.Type;

export const AnalysisArtifactPresentation = Schema.Literals(["static", "interactive", "native"]);
export type AnalysisArtifactPresentation = typeof AnalysisArtifactPresentation.Type;

export const AnalysisArtifactRepresentation = Schema.Struct({
  representationId: AnalysisArtifactRepresentationId,
  fileName: AnalysisArtifactFileName,
  mediaType: AnalysisArtifactMediaType,
  presentation: AnalysisArtifactPresentation,
  requiresNetworkForFullExperience: Schema.Boolean,
  contentHash: AnalysisArtifactContentHash,
  byteLength: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
});
export type AnalysisArtifactRepresentation = typeof AnalysisArtifactRepresentation.Type;

export const AnalysisArtifact = Schema.Struct({
  artifactId: AnalysisArtifactId,
  kind: Schema.Literal("figure"),
  label: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  createdAt: Schema.String,
  representations: Schema.Array(AnalysisArtifactRepresentation),
});
export type AnalysisArtifact = typeof AnalysisArtifact.Type;

export const AnalysisArtifactReceipt = Schema.Struct({
  status: Schema.Literals(["not-requested", "pending", "succeeded", "failed"]),
  failureMessage: Schema.NullOr(Schema.String.check(Schema.isMaxLength(2048))),
});
export type AnalysisArtifactReceipt = typeof AnalysisArtifactReceipt.Type;

export const AnalysisArtifactResourceRef = Schema.Struct({
  projectId: Identifier,
  runId: ExecutionRunId,
  artifactId: AnalysisArtifactId,
  representationId: AnalysisArtifactRepresentationId,
});
export type AnalysisArtifactResourceRef = typeof AnalysisArtifactResourceRef.Type;

export const AnalysisRunStorage = Schema.Struct({
  status: Schema.Literals(["retained", "metadata-only"]),
  outputBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  artifactBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  totalBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  removedAt: Schema.NullOr(Schema.String),
});
export type AnalysisRunStorage = typeof AnalysisRunStorage.Type;

const AnalysisRunBase = Schema.Struct({
  contractVersion: Schema.Literal(ANALYSIS_CONTRACT_VERSION),
  projectId: Identifier,
  action: AnalysisRunAction,
  runtime: AnalysisRuntimeProfile,
  source: AnalysisRunSource,
  phase: AnalysisRunPhase.pipe(Schema.withDecodingDefaultKey(Effect.succeed("finished"))),
  queuePosition: Schema.NullOr(Schema.Int.check(Schema.isGreaterThanOrEqualTo(1))).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed(null)),
  ),
  diagnostics: Schema.Array(AnalysisDiagnostic).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed([])),
  ),
  artifacts: Schema.Array(AnalysisArtifact).pipe(Schema.withDecodingDefaultKey(Effect.succeed([]))),
  artifactReceipt: AnalysisArtifactReceipt.pipe(
    Schema.withDecodingDefaultKey(
      Effect.succeed({ status: "not-requested" as const, failureMessage: null }),
    ),
  ),
  localStorage: AnalysisRunStorage.pipe(
    Schema.withDecodingDefaultKey(
      Effect.succeed({
        status: "retained" as const,
        outputBytes: 0,
        artifactBytes: 0,
        totalBytes: 0,
        removedAt: null,
      }),
    ),
  ),
});

export const AnalysisRunSummary = Schema.Struct({
  ...AnalysisRunBase.fields,
  receipt: ExecutionReceiptSummary,
});
export type AnalysisRunSummary = typeof AnalysisRunSummary.Type;

export const AnalysisRunSnapshot = Schema.Struct({
  ...AnalysisRunBase.fields,
  receipt: ExecutionReceipt,
});
export type AnalysisRunSnapshot = typeof AnalysisRunSnapshot.Type;

export const AnalysisInspectRuntimesInput = Schema.Struct({
  cwd: PathString,
  refresh: Schema.optional(Schema.Boolean),
});
export type AnalysisInspectRuntimesInput = typeof AnalysisInspectRuntimesInput.Type;

export const AnalysisConfigureRuntimeInput = Schema.Struct({
  cwd: PathString,
  runtimeKind: AnalysisRuntimeKind,
  executablePath: Schema.NullOr(PathString),
});
export type AnalysisConfigureRuntimeInput = typeof AnalysisConfigureRuntimeInput.Type;

export const AnalysisVerifyRuntimeInput = Schema.Struct({
  cwd: PathString,
  runtimeId: AnalysisRuntimeId,
  refresh: Schema.optional(Schema.Boolean),
});
export type AnalysisVerifyRuntimeInput = typeof AnalysisVerifyRuntimeInput.Type;

export const AnalysisStartRunInput = Schema.Struct({
  cwd: PathString,
  relativePath: PathString,
  sourceRevision: AnalysisSourceRevision,
  runtimeId: AnalysisRuntimeId,
});
export type AnalysisStartRunInput = typeof AnalysisStartRunInput.Type;

export const AnalysisCancelRunInput = Schema.Struct({
  cwd: PathString,
  runId: ExecutionRunId,
});
export type AnalysisCancelRunInput = typeof AnalysisCancelRunInput.Type;

export const AnalysisListRunsInput = Schema.Struct({
  cwd: PathString,
  relativePath: Schema.optional(PathString),
  limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
  cursor: Schema.optional(Schema.NonEmptyString.check(Schema.isMaxLength(2048))),
});
export type AnalysisListRunsInput = typeof AnalysisListRunsInput.Type;

export const AnalysisListRunsResult = Schema.Struct({
  runs: Schema.Array(AnalysisRunSummary),
  nextCursor: Schema.NullOr(Schema.NonEmptyString.check(Schema.isMaxLength(2048))),
  hasMore: Schema.Boolean,
});
export type AnalysisListRunsResult = typeof AnalysisListRunsResult.Type;

export const AnalysisGetRunInput = Schema.Struct({
  cwd: PathString,
  runId: ExecutionRunId,
});
export type AnalysisGetRunInput = typeof AnalysisGetRunInput.Type;

export const AnalysisStorageSummaryInput = Schema.Struct({
  cwd: PathString,
  relativePath: Schema.optional(PathString),
});
export type AnalysisStorageSummaryInput = typeof AnalysisStorageSummaryInput.Type;

export const AnalysisStorageSummary = Schema.Struct({
  retainedRunCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  metadataOnlyRunCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  outputBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  artifactBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  totalBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});
export type AnalysisStorageSummary = typeof AnalysisStorageSummary.Type;

export const AnalysisCleanupRunInput = Schema.Struct({
  cwd: PathString,
  runId: ExecutionRunId,
});
export type AnalysisCleanupRunInput = typeof AnalysisCleanupRunInput.Type;

export const AnalysisCleanupProjectInput = Schema.Struct({
  cwd: PathString,
  expectedRetainedBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});
export type AnalysisCleanupProjectInput = typeof AnalysisCleanupProjectInput.Type;

export const AnalysisCleanupResult = Schema.Struct({
  cleanedRunCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  freedBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  storage: AnalysisStorageSummary,
});
export type AnalysisCleanupResult = typeof AnalysisCleanupResult.Type;

export const AnalysisSubscribeRunsInput = Schema.Struct({
  cwd: PathString,
  relativePath: Schema.optional(PathString),
});
export type AnalysisSubscribeRunsInput = typeof AnalysisSubscribeRunsInput.Type;

export const AnalysisRunStreamEvent = Schema.Union([
  Schema.TaggedStruct("run-snapshot", {
    eventSequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    run: AnalysisRunSnapshot,
  }),
  Schema.TaggedStruct("run-updated", {
    eventSequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    run: AnalysisRunSummary,
  }),
  Schema.TaggedStruct("run-output", {
    eventSequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
    projectId: Identifier,
    runId: ExecutionRunId,
    source: AnalysisRunSource,
    chunks: Schema.Array(ExecutionOutputChunk),
    outputTruncated: Schema.Boolean,
    outputByteLength: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  }),
]);
export type AnalysisRunStreamEvent = typeof AnalysisRunStreamEvent.Type;

export function summarizeAnalysisRun(run: AnalysisRunSnapshot): AnalysisRunSummary {
  const { output: _output, ...receipt } = run.receipt;
  const verification = run.runtime.verification;
  return {
    ...run,
    runtime:
      verification === null
        ? run.runtime
        : {
            ...run.runtime,
            // The full toolbox inventory remains canonical in run.json and is
            // available through getRun/inspectRuntimes. Repeating it in every
            // paged row and stream update makes history payloads grow with the
            // local installation rather than with useful run metadata.
            verification: { ...verification, toolboxes: [] },
          },
    receipt,
  };
}

export class AnalysisOperationError extends Schema.TaggedErrorClass<AnalysisOperationError>()(
  "AnalysisOperationError",
  {
    operation: Schema.Literals([
      "inspect",
      "configure",
      "verify",
      "start",
      "cancel",
      "list",
      "get",
      "storage",
      "cleanup",
      "subscribe",
    ]),
    reason: Schema.Literals([
      "project-not-initialized",
      "invalid-source",
      "source-changed",
      "runtime-missing",
      "runtime-invalid",
      "run-not-found",
      "run-already-finished",
      "run-already-active",
      "runtime-queue-full",
      "invalid-cursor",
      "persistence-failed",
      "process-failed",
      "operation-failed",
    ]),
    message: Schema.NonEmptyString.check(Schema.isMaxLength(4096)),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface AnalysisRunContext {
  readonly runId: ExecutionRunId;
  readonly projectId: string;
  readonly runtime: AnalysisRuntimeProfile;
  readonly source: AnalysisRunSource;
  readonly absoluteSourcePath: string;
  readonly artifactStagingDirectory: string;
}

export interface AnalysisRuntimeVerificationPreparation {
  readonly executableIdentity: string;
  readonly executable: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  readonly environment: Readonly<Record<string, string>>;
  readonly timeoutMs: number;
  readonly collect: (input: {
    readonly exitCode: number | null;
    readonly timedOut: boolean;
    readonly output: ReadonlyArray<{
      readonly stream: "stdout" | "stderr";
      readonly text: string;
    }>;
    readonly verifiedAt: string;
    readonly durationMs: number;
  }) => Promise<AnalysisRuntimeVerification>;
  readonly cleanup: () => Promise<void>;
}

export interface AnalysisArtifactRepresentationCandidate {
  readonly representationId: AnalysisArtifactRepresentationId;
  readonly fileName: AnalysisArtifactFileName;
  readonly mediaType: AnalysisArtifactMediaType;
  readonly presentation: AnalysisArtifactPresentation;
  readonly requiresNetworkForFullExperience: boolean;
}

export interface AnalysisArtifactCandidate {
  readonly artifactId: AnalysisArtifactId;
  readonly kind: "figure";
  readonly label: string;
  readonly representations: ReadonlyArray<AnalysisArtifactRepresentationCandidate>;
}

export interface AnalysisArtifactCollection {
  readonly candidates: ReadonlyArray<AnalysisArtifactCandidate>;
  /** A producer warning that does not invalidate representations published successfully. */
  readonly failureMessage: string | null;
}

export interface AnalysisRuntimeAdapter {
  readonly id: AnalysisRuntimeId;
  readonly kind: AnalysisRuntimeKind;
  readonly fileExtensions: ReadonlyArray<string>;
  readonly inspect: (input: {
    readonly customExecutablePath?: string;
    readonly inspectedAt: string;
    readonly platform: string;
    readonly environment: Readonly<Record<string, string | undefined>>;
  }) => Promise<AnalysisRuntimeProfile>;
  readonly prepareVerification?: (
    profile: AnalysisRuntimeProfile,
  ) => Promise<AnalysisRuntimeVerificationPreparation>;
  readonly validateSource?: (source: AnalysisRunSource) => string | null;
  readonly prepareRun?: (context: AnalysisRunContext) => Promise<void>;
  readonly prepare: (context: AnalysisRunContext) => {
    readonly executable: string;
    readonly args: ReadonlyArray<string>;
    readonly cwd: string;
    readonly environment: Readonly<Record<string, string>>;
  };
  readonly collectArtifacts?: (context: AnalysisRunContext) => Promise<AnalysisArtifactCollection>;
  readonly collectDiagnostics?: (
    context: AnalysisRunContext,
  ) => Promise<ReadonlyArray<AnalysisDiagnostic>>;
}
