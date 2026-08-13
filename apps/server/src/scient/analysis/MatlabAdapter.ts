// @effect-diagnostics nodeBuiltinImport:off -- MATLAB discovery is an operating-system adapter.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  AnalysisArtifactFileName,
  AnalysisArtifactId,
  AnalysisArtifactRepresentationId,
  AnalysisRuntimeId,
  type AnalysisArtifactCollection,
  type AnalysisDiagnostic,
  type AnalysisRuntimeAdapter,
  type AnalysisRuntimeProfile,
  type AnalysisRuntimeVerification,
  type AnalysisRunContext,
  type AnalysisRunSource,
} from "@scientfactory/analysis";
import * as Option from "effect/Option";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const MATLAB_RUNTIME_ID = AnalysisRuntimeId.make("matlab:local");
export const MATLAB_RUNTIME_KIND = "matlab" as const;
export const MATLAB_BATCH_EXPRESSION = "cd(matlabroot);run(getenv('SCIENT_MATLAB_RUNNER'))";

const MATLAB_CAPTURE_DIRECTORY_NAME = "files";
const MATLAB_RUNNER_FILE_NAME = "scient_run_file.m";
const MATLAB_DIAGNOSTIC_FILE_NAME = "scient-diagnostic.json";
export const MATLAB_CAPTURE_FAILURE_FILE_NAME = "scient-capture-failed.txt";
const MATLAB_VERIFY_FILE_NAME = "scient_verify_runtime.m";
const MATLAB_VERIFY_RESULT_FILE_NAME = "scient-runtime.json";
const MATLAB_CAPTURE_FILE = /^figure-(\d{3})\.(fig|png)$/u;
const MATLAB_MAXIMUM_CAPTURED_FIGURES = 50;

const MATLAB_RUNNER_SOURCE = `scientEntryPoint = getenv('SCIENT_MATLAB_ENTRYPOINT');
scientArtifactDirectory = getenv('SCIENT_MATLAB_ARTIFACT_DIR');
scientDiagnosticPath = getenv('SCIENT_MATLAB_DIAGNOSTIC_PATH');
scientExecutionError = [];
scientArtifactCaptureFailed = false;

try
    run(scientEntryPoint);
catch scientCaughtExecutionError
    scientExecutionError = scientCaughtExecutionError;
    scient_write_diagnostic(scientCaughtExecutionError, scientDiagnosticPath);
end

try
    drawnow;
    scientFigures = findall(groot, 'Type', 'figure');
    try
        [~, scientFigureOrder] = sort(arrayfun(@(scientFigure) double(scientFigure.Number), scientFigures));
        scientFigures = scientFigures(scientFigureOrder);
    catch
        % Preserve findall order when a figure type does not expose a numeric Number.
    end
    scientFigureCount = min(numel(scientFigures), ${MATLAB_MAXIMUM_CAPTURED_FIGURES});
    if numel(scientFigures) > scientFigureCount
        fprintf(2, 'Scient captured the first %d of %d MATLAB figures.\\n', scientFigureCount, numel(scientFigures));
    end
    for scientFigureIndex = 1:scientFigureCount
        scientFigure = scientFigures(scientFigureIndex);
        scientBaseName = sprintf('figure-%03d', scientFigureIndex);
        scientPngCaptured = scient_try_export(@() exportgraphics(scientFigure, fullfile(scientArtifactDirectory, [scientBaseName '.partial.png']), 'Resolution', 144), fullfile(scientArtifactDirectory, [scientBaseName '.partial.png']), fullfile(scientArtifactDirectory, [scientBaseName '.png']));
        scientFigCaptured = scient_try_export(@() scient_save_figure(scientFigure, fullfile(scientArtifactDirectory, [scientBaseName '.partial.fig'])), fullfile(scientArtifactDirectory, [scientBaseName '.partial.fig']), fullfile(scientArtifactDirectory, [scientBaseName '.fig']));
        scientArtifactCaptureFailed = scientArtifactCaptureFailed || ~scientPngCaptured || ~scientFigCaptured;
    end
catch scientCaptureError
    scientArtifactCaptureFailed = true;
    fprintf(2, 'Scient could not finish MATLAB figure capture: %s\\n', scientCaptureError.message);
end

if scientArtifactCaptureFailed
    scient_write_capture_failure(fullfile(scientArtifactDirectory, '${MATLAB_CAPTURE_FAILURE_FILE_NAME}'));
end

if ~isempty(scientExecutionError)
    rethrow(scientExecutionError);
end

function scientSucceeded = scient_try_export(scientOperation, scientTemporaryPath, scientFinalPath)
    scientSucceeded = false;
    try
        scientOperation();
        movefile(scientTemporaryPath, scientFinalPath, 'f');
        scientSucceeded = true;
    catch scientExportError
        if isfile(scientTemporaryPath)
            delete(scientTemporaryPath);
        end
        fprintf(2, 'Scient could not capture %s: %s\\n', scientFinalPath, scientExportError.message);
    end
end

function scient_write_capture_failure(scientPath)
    try
        scientFile = fopen(scientPath, 'w');
        if scientFile < 0
            return;
        end
        scientCleanup = onCleanup(@() fclose(scientFile));
        fprintf(scientFile, 'One or more MATLAB figures could not be captured completely.');
        clear scientCleanup;
    catch
        % The stderr warning remains available when this private marker cannot be written.
    end
end

function scient_save_figure(scientFigure, scientPath)
    try
        savefig(scientFigure, scientPath, 'compact');
    catch
        savefig(scientFigure, scientPath);
    end
end

function scient_write_diagnostic(scientError, scientPath)
    scientTemporaryPath = [scientPath '.partial'];
    try
        scientPayload = struct('identifier', scientError.identifier, 'message', scientError.message, 'stack', scientError.stack, 'causes', scient_flatten_causes(scientError, 0));
        scientFile = fopen(scientTemporaryPath, 'w');
        if scientFile < 0
            return;
        end
        scientCleanup = onCleanup(@() fclose(scientFile));
        fwrite(scientFile, jsonencode(scientPayload), 'char');
        clear scientCleanup;
        movefile(scientTemporaryPath, scientPath, 'f');
    catch
        if isfile(scientTemporaryPath)
            delete(scientTemporaryPath);
        end
    end
end

function scientPayloads = scient_flatten_causes(scientError, scientDepth)
    scientPayloads = repmat(struct('identifier', '', 'message', '', 'stack', []), 1, 0);
    if scientDepth >= 5
        return;
    end
    scientCauses = scientError.cause;
    scientCauseCount = min(numel(scientCauses), 10);
    for scientCauseIndex = 1:scientCauseCount
        scientCause = scientCauses{scientCauseIndex};
        scientPayloads(end + 1) = struct('identifier', scientCause.identifier, 'message', scientCause.message, 'stack', scientCause.stack); %#ok<AGROW>
        scientNested = scient_flatten_causes(scientCause, scientDepth + 1);
        scientRemaining = 50 - numel(scientPayloads);
        if scientRemaining <= 0
            return;
        end
        scientPayloads = [scientPayloads scientNested(1:min(numel(scientNested), scientRemaining))]; %#ok<AGROW>
    end
end
`;

export const MATLAB_VERIFY_BATCH_EXPRESSION = "cd(matlabroot);run(getenv('SCIENT_MATLAB_VERIFY'))";

const MATLAB_VERIFY_SOURCE = `scientResultPath = getenv('SCIENT_MATLAB_VERIFY_RESULT');
scientTemporaryPath = [scientResultPath '.partial'];
scientPayload = struct;
scientPayload.release = version('-release');
scientPayload.version = version;
scientPayload.architecture = computer('arch');
scientPayload.installationRoot = matlabroot;
scientPayload.javaAvailable = usejava('jvm');
scientPayload.javaVersion = '';
if scientPayload.javaAvailable
    scientPayload.javaVersion = version('-java');
end
scientInstalled = ver;
scientToolboxes = repmat(struct('name', '', 'version', ''), 1, numel(scientInstalled));
for scientIndex = 1:numel(scientInstalled)
    scientToolboxes(scientIndex).name = scientInstalled(scientIndex).Name;
    scientToolboxes(scientIndex).version = scientInstalled(scientIndex).Version;
end
scientPayload.toolboxes = scientToolboxes;
scientFile = fopen(scientTemporaryPath, 'w');
if scientFile < 0
    error('Scient:RuntimeProbeWriteFailed', 'Unable to write the Scient runtime probe result.');
end
scientCleanup = onCleanup(@() fclose(scientFile));
fwrite(scientFile, jsonencode(scientPayload), 'char');
clear scientCleanup;
movefile(scientTemporaryPath, scientResultPath, 'f');
`;

const MatlabVerificationPayload = Schema.Struct({
  release: Schema.String,
  version: Schema.String,
  architecture: Schema.String,
  installationRoot: Schema.String,
  javaAvailable: Schema.Boolean,
  javaVersion: Schema.String,
  toolboxes: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      version: Schema.String,
    }),
  ),
});
const decodeMatlabVerificationPayload = Schema.decodeUnknownOption(MatlabVerificationPayload);

const MatlabDiagnosticPayload = Schema.Struct({
  identifier: Schema.String,
  message: Schema.String,
  stack: Schema.Array(
    Schema.Struct({
      file: Schema.String,
      name: Schema.String,
      line: Schema.Number,
    }),
  ),
  causes: Schema.Array(
    Schema.Struct({
      identifier: Schema.String,
      message: Schema.String,
      stack: Schema.Array(
        Schema.Struct({
          file: Schema.String,
          name: Schema.String,
          line: Schema.Number,
        }),
      ),
    }),
  ).pipe(Schema.withDecodingDefaultKey(Effect.succeed([]))),
});
const decodeMatlabDiagnosticPayload = Schema.decodeUnknownOption(MatlabDiagnosticPayload);

function bounded(value: string, maximum: number): string {
  return value.length <= maximum ? value : `${value.slice(0, maximum - 1)}…`;
}

function normalizedMatlabRelease(value: string): string | null {
  const release = value.trim();
  if (release.length === 0) return null;
  return bounded(/^\d{4}[ab]$/iu.test(release) ? `R${release}` : release, 256);
}

function projectRelativePath(cwd: string, filePath: string): string | null {
  const relative = NodePath.relative(cwd, filePath);
  if (
    relative.length === 0 ||
    relative === ".." ||
    relative.startsWith(`..${NodePath.sep}`) ||
    NodePath.isAbsolute(relative)
  ) {
    return null;
  }
  return relative.split(NodePath.sep).join("/");
}

function verificationFailureStatus(
  output: string,
): Extract<
  AnalysisRuntimeVerification["status"],
  "needs-sign-in" | "license-unavailable" | "missing-dependency" | "startup-failed"
> | null {
  const normalized = output.toLowerCase();
  if (/sign[ -]?in|mathworks account|log[ -]?in/u.test(normalized)) return "needs-sign-in";
  if (
    /license manager|licen[sc](?:e|ing).*(?:error|failed|unavailable)|checkout failed/u.test(
      normalized,
    )
  ) {
    return "license-unavailable";
  }
  if (
    /(?:java|jre|jvm).*(?:not found|missing|unable|failed)|unable.*(?:java|jre|jvm)/u.test(
      normalized,
    )
  ) {
    return "missing-dependency";
  }
  if (
    /segmentation fault|fatal startup|failed to start matlab|unable to start matlab|matlab.*failed to start/u.test(
      normalized,
    )
  ) {
    return "startup-failed";
  }
  return null;
}

function matlabFrames(
  cwd: string,
  stack: ReadonlyArray<{ readonly file: string; readonly name: string; readonly line: number }>,
) {
  return stack.slice(0, 100).map((frame) => ({
    relativePath: projectRelativePath(cwd, frame.file),
    functionName: frame.name.length > 0 ? bounded(frame.name, 512) : null,
    line: Number.isSafeInteger(frame.line) && frame.line >= 1 ? frame.line : null,
    column: null,
  }));
}

const MATLAB_REPRESENTATIONS = {
  fig: {
    representationId: "matlab-figure",
    mediaType: "application/vnd.mathworks.matlab.figure",
    presentation: "native",
    requiresNetworkForFullExperience: false,
  },
  png: {
    representationId: "static-png",
    mediaType: "image/png",
    presentation: "static",
    requiresNetworkForFullExperience: false,
  },
} as const;

export async function prepareMatlabRun(context: AnalysisRunContext): Promise<void> {
  const filesDirectory = NodePath.join(
    context.artifactStagingDirectory,
    MATLAB_CAPTURE_DIRECTORY_NAME,
  );
  await NodeFSP.mkdir(filesDirectory, { recursive: true });
  await NodeFSP.writeFile(
    NodePath.join(context.artifactStagingDirectory, MATLAB_RUNNER_FILE_NAME),
    MATLAB_RUNNER_SOURCE,
    { encoding: "utf8", flag: "wx" },
  );
}

export async function collectMatlabDiagnostics(
  context: AnalysisRunContext,
): Promise<ReadonlyArray<AnalysisDiagnostic>> {
  let unknown: unknown;
  try {
    unknown = JSON.parse(
      await NodeFSP.readFile(
        NodePath.join(context.artifactStagingDirectory, MATLAB_DIAGNOSTIC_FILE_NAME),
        "utf8",
      ),
    );
  } catch {
    return [];
  }
  const decoded = decodeMatlabDiagnosticPayload(unknown);
  if (Option.isNone(decoded)) return [];
  const payload = decoded.value;
  const frames = matlabFrames(context.source.cwd, payload.stack);
  const primary = frames.find((frame) => frame.relativePath !== null) ?? frames[0];
  return [
    {
      diagnosticId: "matlab-error-1",
      severity: "error",
      source: "code-analysis",
      code: payload.identifier.length > 0 ? bounded(payload.identifier, 512) : null,
      message: bounded(payload.message || "MATLAB reported an error.", 16 * 1024),
      relativePath: primary?.relativePath ?? null,
      line: primary?.line ?? null,
      column: null,
      frames,
      related: payload.causes.slice(0, 50).map((cause) => ({
        code: cause.identifier.length > 0 ? bounded(cause.identifier, 512) : null,
        message: bounded(cause.message || "MATLAB reported a related error.", 16 * 1024),
        frames: matlabFrames(context.source.cwd, cause.stack),
      })),
    },
  ];
}

export async function prepareMatlabVerification(profile: AnalysisRuntimeProfile) {
  if (profile.executablePath === null) throw new Error("MATLAB executable is unavailable.");
  const executablePath = await NodeFSP.realpath(profile.executablePath);
  const executableInfo = await NodeFSP.stat(executablePath);
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "scient-matlab-verify-"));
  const probePath = NodePath.join(directory, MATLAB_VERIFY_FILE_NAME);
  const resultPath = NodePath.join(directory, MATLAB_VERIFY_RESULT_FILE_NAME);
  await NodeFSP.writeFile(probePath, MATLAB_VERIFY_SOURCE, { encoding: "utf8", flag: "wx" });
  const executableIdentity = [
    executablePath,
    String(executableInfo.size),
    String(executableInfo.mtimeMs),
  ].join(":");
  return {
    executableIdentity,
    executable: profile.executablePath,
    args: ["-batch", MATLAB_VERIFY_BATCH_EXPRESSION],
    cwd: NodePath.dirname(profile.executablePath),
    environment: {
      SCIENT_MATLAB_VERIFY: probePath,
      SCIENT_MATLAB_VERIFY_RESULT: resultPath,
    },
    timeoutMs: 90_000,
    collect: async (input: {
      readonly exitCode: number | null;
      readonly timedOut: boolean;
      readonly output: ReadonlyArray<{
        readonly stream: "stdout" | "stderr";
        readonly text: string;
      }>;
      readonly verifiedAt: string;
      readonly durationMs: number;
    }): Promise<AnalysisRuntimeVerification> => {
      if (input.timedOut) {
        return {
          status: "timed-out",
          verifiedAt: input.verifiedAt,
          durationMs: input.durationMs,
          executableIdentity,
          release: null,
          version: null,
          architecture: null,
          installationRoot: null,
          javaAvailable: null,
          javaVersion: null,
          toolboxes: [],
          detail: "MATLAB did not finish its startup check within 90 seconds.",
        };
      }
      let decoded = Option.none<typeof MatlabVerificationPayload.Type>();
      try {
        decoded = decodeMatlabVerificationPayload(
          JSON.parse(await NodeFSP.readFile(resultPath, "utf8")),
        );
      } catch {
        // Startup failures commonly happen before the private result file exists.
      }
      if (input.exitCode === 0 && Option.isSome(decoded)) {
        const payload = decoded.value;
        return {
          status: "ready",
          verifiedAt: input.verifiedAt,
          durationMs: input.durationMs,
          executableIdentity,
          release: normalizedMatlabRelease(payload.release),
          version: bounded(payload.version, 256) || null,
          architecture: bounded(payload.architecture, 128) || null,
          installationRoot: payload.installationRoot || null,
          javaAvailable: payload.javaAvailable,
          javaVersion: bounded(payload.javaVersion, 1024) || null,
          toolboxes: payload.toolboxes.slice(0, 500).flatMap((toolbox) => {
            const name = bounded(toolbox.name.trim(), 256);
            return name.length === 0
              ? []
              : [{ name, version: bounded(toolbox.version.trim(), 128) || null }];
          }),
          detail: `MATLAB ${payload.release || payload.version} started successfully.`,
        };
      }
      const output = bounded(input.output.map((chunk) => chunk.text).join("\n"), 2048);
      const classifiedStatus = verificationFailureStatus(output);
      const status = classifiedStatus ?? "unknown";
      const detailByStatus = {
        "needs-sign-in": "MATLAB needs a MathWorks sign-in before Scient can run files.",
        "license-unavailable": "MATLAB started, but no usable license was available.",
        "missing-dependency":
          "MATLAB could not start because a required local dependency is missing.",
        "startup-failed": "MATLAB did not complete its startup check.",
        unknown:
          "MATLAB did not complete its startup check, and Scient could not identify a reliable cause.",
      } as const;
      return {
        status,
        verifiedAt: input.verifiedAt,
        durationMs: input.durationMs,
        executableIdentity,
        release: null,
        version: null,
        architecture: null,
        installationRoot: null,
        javaAvailable: null,
        javaVersion: null,
        toolboxes: [],
        detail: output.length > 0 ? `${detailByStatus[status]} ${output}` : detailByStatus[status],
      };
    },
    cleanup: () => NodeFSP.rm(directory, { recursive: true, force: true }),
  };
}

export async function collectMatlabArtifacts(
  context: AnalysisRunContext,
): Promise<AnalysisArtifactCollection> {
  const filesDirectory = NodePath.join(
    context.artifactStagingDirectory,
    MATLAB_CAPTURE_DIRECTORY_NAME,
  );
  let entries: ReadonlyArray<string>;
  try {
    entries = (await NodeFSP.readdir(filesDirectory)).toSorted();
  } catch {
    return { candidates: [], failureMessage: null };
  }
  const filesByFigure = new Map<number, Map<keyof typeof MATLAB_REPRESENTATIONS, string>>();
  for (const fileName of entries) {
    const match = MATLAB_CAPTURE_FILE.exec(fileName);
    const figureIndex = Number(match?.[1]);
    const extension = match?.[2] as keyof typeof MATLAB_REPRESENTATIONS | undefined;
    if (!Number.isSafeInteger(figureIndex) || figureIndex < 1 || !extension) continue;
    const files = filesByFigure.get(figureIndex) ?? new Map();
    files.set(extension, fileName);
    filesByFigure.set(figureIndex, files);
  }

  const artifacts: AnalysisArtifactCollection["candidates"][number][] = [];
  for (const [figureIndex, files] of [...filesByFigure.entries()].toSorted(
    ([left], [right]) => left - right,
  )) {
    const representations = [...files.entries()]
      .toSorted(([left], [right]) => left.localeCompare(right))
      .map(([extension, fileName]) => {
        const definition = MATLAB_REPRESENTATIONS[extension];
        return {
          representationId: AnalysisArtifactRepresentationId.make(definition.representationId),
          fileName: AnalysisArtifactFileName.make(fileName),
          mediaType: definition.mediaType,
          presentation: definition.presentation,
          requiresNetworkForFullExperience: definition.requiresNetworkForFullExperience,
        };
      });
    if (representations.length === 0) continue;
    artifacts.push({
      artifactId: AnalysisArtifactId.make(`figure-${String(figureIndex).padStart(3, "0")}`),
      kind: "figure",
      label: `Figure ${figureIndex}`,
      representations,
    });
  }
  return {
    candidates: artifacts,
    failureMessage: entries.includes(MATLAB_CAPTURE_FAILURE_FILE_NAME)
      ? "MATLAB finished, but Scient could not capture every figure representation. Available figures are still shown."
      : null,
  };
}

interface MatlabDiscoveryInput {
  readonly customExecutablePath?: string | undefined;
  readonly inspectedAt: string;
  readonly platform: string;
  readonly environment: Readonly<Record<string, string | undefined>>;
}

function executableName(platform: string): string {
  return platform === "win32" ? "matlab.exe" : "matlab";
}

export function matlabReleaseFromExecutablePath(executablePath: string): string | null {
  const match = /(?:^|[^a-z0-9])(R\d{4}[ab])(?:[^a-z0-9]|$)/iu.exec(executablePath);
  if (!match?.[1]) return null;
  return `${match[1].slice(0, -1).toUpperCase()}${match[1].at(-1)?.toLowerCase()}`;
}

async function executableFile(candidate: string, platform: string): Promise<boolean> {
  try {
    const stat = await NodeFSP.stat(candidate);
    if (!stat.isFile()) return false;
    if (platform !== "win32") await NodeFSP.access(candidate, NodeFSP.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

async function directoryNames(directory: string): Promise<ReadonlyArray<string>> {
  try {
    return (await NodeFSP.readdir(directory, { withFileTypes: true }))
      .filter((entry) => entry.isDirectory())
      .map((entry) => entry.name)
      .toSorted()
      .toReversed();
  } catch {
    return [];
  }
}

async function conventionalCandidates(
  platform: string,
  environment: Readonly<Record<string, string | undefined>>,
): Promise<ReadonlyArray<string>> {
  if (platform === "darwin") {
    const releases = (await directoryNames("/Applications")).filter((name) =>
      /^MATLAB_R\d{4}[ab]\.app$/u.test(name),
    );
    return releases.map((name) => NodePath.join("/Applications", name, "bin", "matlab"));
  }
  if (platform === "win32") {
    const roots = [environment.ProgramFiles, environment["ProgramFiles(x86)"]].filter(
      (value): value is string => typeof value === "string" && value.length > 0,
    );
    const candidates: string[] = [];
    for (const root of roots) {
      const matlabRoot = NodePath.join(root, "MATLAB");
      for (const release of await directoryNames(matlabRoot)) {
        candidates.push(NodePath.join(matlabRoot, release, "bin", "matlab.exe"));
      }
    }
    return candidates;
  }
  if (platform === "linux") {
    const root = "/usr/local/MATLAB";
    return (await directoryNames(root)).map((release) =>
      NodePath.join(root, release, "bin", "matlab"),
    );
  }
  return [];
}

function pathCandidates(
  platform: string,
  environment: Readonly<Record<string, string | undefined>>,
): ReadonlyArray<string> {
  const pathValue = environment.PATH;
  if (!pathValue) return [];
  return pathValue
    .split(NodePath.delimiter)
    .filter((entry) => entry.length > 0)
    .map((entry) => NodePath.join(entry, executableName(platform)));
}

export async function inspectMatlabRuntime(
  input: MatlabDiscoveryInput,
): Promise<AnalysisRuntimeProfile> {
  const platform = input.platform;
  const environment = input.environment;
  const customPath = input.customExecutablePath?.trim();
  if (customPath) {
    const absolutePath = NodePath.resolve(customPath);
    const available = await executableFile(absolutePath, platform);
    return {
      id: MATLAB_RUNTIME_ID,
      kind: MATLAB_RUNTIME_KIND,
      label: "MATLAB",
      availability: available ? "available" : "invalid",
      source: "custom",
      executablePath: absolutePath,
      version: matlabReleaseFromExecutablePath(absolutePath),
      detail: available
        ? "Configured MATLAB executable."
        : "The configured MATLAB executable does not exist or is not executable.",
      capabilities: ["run-file", "stream-output", "cancel-process-tree", "capture-artifacts"],
      inspectedAt: input.inspectedAt,
      verification: null,
    };
  }

  for (const candidate of pathCandidates(platform, environment)) {
    if (await executableFile(candidate, platform)) {
      return {
        id: MATLAB_RUNTIME_ID,
        kind: MATLAB_RUNTIME_KIND,
        label: "MATLAB",
        availability: "available",
        source: "path",
        executablePath: candidate,
        version: matlabReleaseFromExecutablePath(candidate),
        detail: "MATLAB found on PATH.",
        capabilities: ["run-file", "stream-output", "cancel-process-tree", "capture-artifacts"],
        inspectedAt: input.inspectedAt,
        verification: null,
      };
    }
  }
  for (const candidate of await conventionalCandidates(platform, environment)) {
    if (await executableFile(candidate, platform)) {
      return {
        id: MATLAB_RUNTIME_ID,
        kind: MATLAB_RUNTIME_KIND,
        label: "MATLAB",
        availability: "available",
        source: "conventional-install",
        executablePath: candidate,
        version: matlabReleaseFromExecutablePath(candidate),
        detail: "MATLAB found in a conventional installation folder.",
        capabilities: ["run-file", "stream-output", "cancel-process-tree", "capture-artifacts"],
        inspectedAt: input.inspectedAt,
        verification: null,
      };
    }
  }
  return {
    id: MATLAB_RUNTIME_ID,
    kind: MATLAB_RUNTIME_KIND,
    label: "MATLAB",
    availability: "missing",
    source: "unconfigured",
    executablePath: null,
    version: null,
    detail: "MATLAB was not found. Choose its executable to enable Run File.",
    capabilities: ["run-file", "stream-output", "cancel-process-tree", "capture-artifacts"],
    inspectedAt: input.inspectedAt,
    verification: null,
  };
}

export function prepareMatlabCommand(input: {
  readonly profile: AnalysisRuntimeProfile;
  readonly source: AnalysisRunSource;
  readonly absoluteSourcePath: string;
  readonly artifactStagingDirectory: string;
}) {
  if (input.profile.executablePath === null) {
    throw new Error("MATLAB executable is unavailable.");
  }
  return {
    executable: input.profile.executablePath,
    args: ["-batch", MATLAB_BATCH_EXPRESSION],
    cwd: input.source.cwd,
    environment: {
      SCIENT_MATLAB_ENTRYPOINT: input.absoluteSourcePath,
      SCIENT_MATLAB_ARTIFACT_DIR: NodePath.join(
        input.artifactStagingDirectory,
        MATLAB_CAPTURE_DIRECTORY_NAME,
      ),
      SCIENT_MATLAB_RUNNER: NodePath.join(input.artifactStagingDirectory, MATLAB_RUNNER_FILE_NAME),
      SCIENT_MATLAB_DIAGNOSTIC_PATH: NodePath.join(
        input.artifactStagingDirectory,
        MATLAB_DIAGNOSTIC_FILE_NAME,
      ),
    },
  } as const;
}

/** MATLAB-specific behavior behind the runtime-neutral analysis adapter port. */
export const matlabRuntimeAdapter: AnalysisRuntimeAdapter = {
  id: MATLAB_RUNTIME_ID,
  kind: MATLAB_RUNTIME_KIND,
  fileExtensions: [".m"],
  inspect: inspectMatlabRuntime,
  prepareVerification: prepareMatlabVerification,
  validateSource: (source) => {
    const stem = NodePath.basename(source.relativePath, NodePath.extname(source.relativePath));
    return /^[\p{L}][\p{L}\p{N}_]*$/u.test(stem)
      ? null
      : "MATLAB script names must begin with a letter and contain only letters, numbers, or underscores.";
  },
  prepareRun: prepareMatlabRun,
  prepare: (context) =>
    prepareMatlabCommand({
      profile: context.runtime,
      source: context.source,
      absoluteSourcePath: context.absoluteSourcePath,
      artifactStagingDirectory: context.artifactStagingDirectory,
    }),
  collectArtifacts: collectMatlabArtifacts,
  collectDiagnostics: collectMatlabDiagnostics,
};
