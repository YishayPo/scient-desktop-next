// @effect-diagnostics nodeBuiltinImport:off -- tests create temporary executable fixtures.
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  AnalysisRuntimeId,
  AnalysisSourceRevision,
  type AnalysisRunContext,
  type AnalysisRuntimeProfile,
} from "@scientfactory/analysis";
import { ExecutionRunId } from "@scientfactory/execution";
import { afterEach, describe, expect, it } from "@effect/vitest";

import {
  MATLAB_BATCH_EXPRESSION,
  MATLAB_CAPTURE_FAILURE_FILE_NAME,
  collectMatlabArtifacts,
  collectMatlabDiagnostics,
  inspectMatlabRuntime,
  matlabReleaseFromExecutablePath,
  matlabRuntimeAdapter,
  prepareMatlabCommand,
  prepareMatlabRun,
  prepareMatlabVerification,
} from "./MatlabAdapter.ts";

const tempDirectories: string[] = [];

async function executableFixture(name = "matlab"): Promise<string> {
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "scient-matlab-adapter-"));
  tempDirectories.push(directory);
  const executable = NodePath.join(directory, name);
  await NodeFSP.writeFile(executable, "#!/usr/bin/env node\n", "utf8");
  await NodeFSP.chmod(executable, 0o755);
  return executable;
}

async function runContext(executablePath = "/opt/matlab/bin/matlab") {
  const directory = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "scient-matlab-run-"));
  tempDirectories.push(directory);
  const sourcePath = NodePath.join(directory, "analysis.m");
  const artifactStagingDirectory = NodePath.join(directory, "artifact-staging");
  await NodeFSP.writeFile(sourcePath, "answer = 42;\n", "utf8");
  return {
    runId: ExecutionRunId.make("run-1"),
    projectId: "project-1",
    runtime: {
      id: AnalysisRuntimeId.make("matlab:local"),
      kind: "matlab",
      label: "MATLAB",
      availability: "available",
      source: "custom",
      executablePath,
      version: "R2026a",
      detail: null,
      capabilities: ["run-file", "stream-output", "cancel-process-tree", "capture-artifacts"],
      inspectedAt: "2026-08-12T00:00:00Z",
      verification: null,
    },
    source: {
      cwd: directory,
      relativePath: "analysis.m",
      revision: AnalysisSourceRevision.make("sha256:test"),
    },
    absoluteSourcePath: sourcePath,
    artifactStagingDirectory,
  } satisfies AnalysisRunContext;
}

function runtimeProfile(executablePath: string): AnalysisRuntimeProfile {
  return {
    id: AnalysisRuntimeId.make("matlab:local"),
    kind: "matlab",
    label: "MATLAB",
    availability: "available",
    source: "custom",
    executablePath,
    version: null,
    detail: null,
    capabilities: ["run-file", "stream-output", "cancel-process-tree", "capture-artifacts"],
    inspectedAt: "2026-08-12T00:00:00.000Z",
    verification: null,
  };
}

afterEach(async () => {
  await Promise.all(
    tempDirectories.splice(0).map((directory) => NodeFSP.rm(directory, { recursive: true })),
  );
});

describe("MATLAB runtime adapter", () => {
  it("derives a release from conventional cross-platform executable paths", () => {
    expect(matlabReleaseFromExecutablePath("/Applications/MATLAB_R2025b.app/bin/matlab")).toBe(
      "R2025b",
    );
    expect(
      matlabReleaseFromExecutablePath("C:\\Program Files\\MATLAB\\R2024A\\bin\\matlab.exe"),
    ).toBe("R2024a");
    expect(matlabReleaseFromExecutablePath("/usr/local/bin/matlab")).toBeNull();
  });

  it("prefers an explicit executable and reports invalid custom paths honestly", async () => {
    const executable = await executableFixture();
    await expect(
      inspectMatlabRuntime({
        customExecutablePath: executable,
        inspectedAt: "2026-08-12T00:00:00Z",
        platform: "linux",
        environment: { PATH: "" },
      }),
    ).resolves.toMatchObject({
      availability: "available",
      source: "custom",
      executablePath: executable,
    });
    await expect(
      inspectMatlabRuntime({
        customExecutablePath: `${executable}-missing`,
        inspectedAt: "2026-08-12T00:00:00Z",
        platform: "linux",
        environment: { PATH: "" },
      }),
    ).resolves.toMatchObject({ availability: "invalid", source: "custom" });
  });

  it("discovers MATLAB on PATH without launching it", async () => {
    const executable = await executableFixture();
    const profile = await inspectMatlabRuntime({
      inspectedAt: "2026-08-12T00:00:00Z",
      platform: "linux",
      environment: { PATH: NodePath.dirname(executable) },
    });

    expect(profile).toMatchObject({ availability: "available", source: "path" });
  });

  it("returns a useful missing state without requiring MATLAB to view files", async () => {
    await expect(
      inspectMatlabRuntime({
        inspectedAt: "2026-08-12T00:00:00Z",
        platform: "aix",
        environment: { PATH: "" },
      }),
    ).resolves.toMatchObject({
      availability: "missing",
      source: "unconfigured",
      executablePath: null,
    });
  });

  it("uses a static batch expression and passes arbitrary source paths through the environment", () => {
    const sourcePath = "/project/researcher's $results/analysis.m";
    const command = prepareMatlabCommand({
      profile: {
        id: "matlab:local" as never,
        kind: "matlab",
        label: "MATLAB",
        availability: "available",
        source: "custom",
        executablePath: "/Applications/MATLAB.app/bin/matlab",
        version: null,
        detail: null,
        capabilities: ["run-file", "stream-output", "cancel-process-tree"],
        inspectedAt: "2026-08-12T00:00:00Z",
        verification: null,
      },
      source: {
        cwd: "/project",
        relativePath: "researcher's $results/analysis.m",
        revision: AnalysisSourceRevision.make("sha256:test"),
      },
      absoluteSourcePath: sourcePath,
      artifactStagingDirectory: "/state/analysis/run-1/artifact-staging",
    });

    expect(command.args).toEqual(["-batch", MATLAB_BATCH_EXPRESSION]);
    expect(command.args.join(" ")).not.toContain(sourcePath);
    expect(command.environment).toEqual({
      SCIENT_MATLAB_ENTRYPOINT: sourcePath,
      SCIENT_MATLAB_ARTIFACT_DIR: "/state/analysis/run-1/artifact-staging/files",
      SCIENT_MATLAB_RUNNER: "/state/analysis/run-1/artifact-staging/scient_run_file.m",
      SCIENT_MATLAB_DIAGNOSTIC_PATH:
        "/state/analysis/run-1/artifact-staging/scient-diagnostic.json",
    });
  });

  it("prepares a private runner and collects only complete deterministic figure files", async () => {
    const context = await runContext();
    await prepareMatlabRun(context);
    const filesDirectory = NodePath.join(context.artifactStagingDirectory, "files");
    const runner = await NodeFSP.readFile(
      NodePath.join(context.artifactStagingDirectory, "scient_run_file.m"),
      "utf8",
    );
    expect(runner).toContain("run(scientEntryPoint)");
    expect(runner).toContain("scient_try_export");
    expect(runner).toContain("scient_write_diagnostic");
    expect(runner).toContain("rethrow(scientExecutionError)");
    expect(runner).toContain(".partial.png");
    expect(runner).toContain(".partial.fig");
    expect(runner).not.toMatch(/\.partial\.(?:html|pdf|svg)/u);

    const producerHtml =
      "<!doctype html><html><head></head><body><matlab-canvas></matlab-canvas></body></html>";
    await Promise.all([
      NodeFSP.writeFile(NodePath.join(filesDirectory, "figure-001.png"), "png"),
      NodeFSP.writeFile(NodePath.join(filesDirectory, "figure-001.svg"), "<svg/>", "utf8"),
      NodeFSP.writeFile(NodePath.join(filesDirectory, "figure-001.pdf"), "%PDF"),
      NodeFSP.writeFile(NodePath.join(filesDirectory, "figure-001.fig"), "fig"),
      NodeFSP.writeFile(NodePath.join(filesDirectory, "figure-001.html"), producerHtml, "utf8"),
      NodeFSP.writeFile(NodePath.join(filesDirectory, "figure-002.partial.png"), "partial"),
      NodeFSP.writeFile(NodePath.join(filesDirectory, "unrelated.png"), "unrelated"),
    ]);

    const collection = await collectMatlabArtifacts(context);
    const artifacts = collection.candidates;
    expect(collection.failureMessage).toBeNull();
    expect(artifacts).toHaveLength(1);
    expect(artifacts[0]).toMatchObject({
      artifactId: "figure-001",
      kind: "figure",
      label: "Figure 1",
    });
    expect(artifacts[0]?.representations.map((representation) => representation.mediaType)).toEqual(
      ["application/vnd.mathworks.matlab.figure", "image/png"],
    );
    const collectedHtml = await NodeFSP.readFile(
      NodePath.join(filesDirectory, "figure-001.html"),
      "utf8",
    );
    expect(collectedHtml).toBe(producerHtml);
  });

  it("ignores undeclared rich files until explicit runtime capture support exists", async () => {
    const context = await runContext();
    await prepareMatlabRun(context);
    const filesDirectory = NodePath.join(context.artifactStagingDirectory, "files");
    const htmlPath = NodePath.join(filesDirectory, "figure-001.html");
    await NodeFSP.writeFile(htmlPath, "<html><body>legacy export</body></html>", "utf8");

    await expect(collectMatlabArtifacts(context)).resolves.toEqual({
      candidates: [],
      failureMessage: null,
    });
  });

  it("reports partial capture while keeping successfully exported representations", async () => {
    const context = await runContext();
    await prepareMatlabRun(context);
    const filesDirectory = NodePath.join(context.artifactStagingDirectory, "files");
    await Promise.all([
      NodeFSP.writeFile(NodePath.join(filesDirectory, "figure-001.png"), "png"),
      NodeFSP.writeFile(
        NodePath.join(filesDirectory, MATLAB_CAPTURE_FAILURE_FILE_NAME),
        "capture failed",
      ),
    ]);

    await expect(collectMatlabArtifacts(context)).resolves.toMatchObject({
      candidates: [
        {
          artifactId: "figure-001",
          representations: [{ mediaType: "image/png" }],
        },
      ],
      failureMessage: expect.stringContaining("could not capture every figure"),
    });
  });

  it("verifies startup through a private result file and records reproducible runtime details", async () => {
    const executable = await executableFixture();
    const prepared = await prepareMatlabVerification(runtimeProfile(executable));
    try {
      expect(prepared.args).toEqual([
        "-batch",
        "cd(matlabroot);run(getenv('SCIENT_MATLAB_VERIFY'))",
      ]);
      expect(prepared.executableIdentity).toContain(await NodeFSP.realpath(executable));
      await NodeFSP.writeFile(
        prepared.environment.SCIENT_MATLAB_VERIFY_RESULT,
        JSON.stringify({
          release: "R2026a",
          version: "26.1.0.123456",
          architecture: "maca64",
          installationRoot: "/Applications/MATLAB_R2026a.app",
          javaAvailable: true,
          javaVersion: "Java 21",
          toolboxes: [
            { name: "MATLAB", version: "26.1" },
            { name: "Image Processing Toolbox", version: "26.1" },
          ],
        }),
        "utf8",
      );
      await expect(
        prepared.collect({
          exitCode: 0,
          timedOut: false,
          output: [],
          verifiedAt: "2026-08-12T00:00:05.000Z",
          durationMs: 5_000,
        }),
      ).resolves.toMatchObject({
        status: "ready",
        release: "R2026a",
        architecture: "maca64",
        javaAvailable: true,
        toolboxes: [
          { name: "MATLAB", version: "26.1" },
          { name: "Image Processing Toolbox", version: "26.1" },
        ],
      });
    } finally {
      await prepared.cleanup();
    }
  });

  it("classifies sign-in, license, dependency, startup, and timeout failures honestly", async () => {
    const cases = [
      ["Please sign in to your MathWorks Account", "needs-sign-in"],
      ["License Manager Error - checkout failed", "license-unavailable"],
      ["Unable to find a Java runtime", "missing-dependency"],
      ["MATLAB failed to start", "startup-failed"],
    ] as const;
    for (const [output, status] of cases) {
      const prepared = await prepareMatlabVerification(runtimeProfile(await executableFixture()));
      try {
        await expect(
          prepared.collect({
            exitCode: 1,
            timedOut: false,
            output: [{ stream: "stderr", text: output }],
            verifiedAt: "2026-08-12T00:00:05.000Z",
            durationMs: 500,
          }),
        ).resolves.toMatchObject({ status });
      } finally {
        await prepared.cleanup();
      }
    }

    const ambiguous = await prepareMatlabVerification(runtimeProfile(await executableFixture()));
    try {
      await expect(
        ambiguous.collect({
          exitCode: 1,
          timedOut: false,
          output: [{ stream: "stderr", text: "Unexpected exit 17" }],
          verifiedAt: "2026-08-12T00:00:05.000Z",
          durationMs: 500,
        }),
      ).resolves.toMatchObject({ status: "unknown", detail: expect.stringContaining("exit 17") });
    } finally {
      await ambiguous.cleanup();
    }

    const timedOut = await prepareMatlabVerification(runtimeProfile(await executableFixture()));
    try {
      await expect(
        timedOut.collect({
          exitCode: null,
          timedOut: true,
          output: [],
          verifiedAt: "2026-08-12T00:01:30.000Z",
          durationMs: 90_000,
        }),
      ).resolves.toMatchObject({ status: "timed-out" });
    } finally {
      await timedOut.cleanup();
    }
  });

  it("collects a MATLAB MException side channel without making outside paths clickable", async () => {
    const context = await runContext();
    await prepareMatlabRun(context);
    await NodeFSP.writeFile(
      NodePath.join(context.artifactStagingDirectory, "scient-diagnostic.json"),
      JSON.stringify({
        identifier: "MATLAB:undefinedVarOrClass",
        message: "Unrecognized function or variable 'missingValue'.",
        stack: [
          { file: context.absoluteSourcePath, name: "analysis", line: 7 },
          {
            file: "/Applications/MATLAB_R2026a.app/toolbox/matlab/general/run.m",
            name: "run",
            line: 99,
          },
        ],
        causes: [
          {
            identifier: "MATLAB:nestedFailure",
            message: "Underlying calculation failed.",
            stack: [{ file: context.absoluteSourcePath, name: "analysis", line: 3 }],
          },
        ],
      }),
      "utf8",
    );

    await expect(collectMatlabDiagnostics(context)).resolves.toEqual([
      expect.objectContaining({
        severity: "error",
        code: "MATLAB:undefinedVarOrClass",
        relativePath: "analysis.m",
        line: 7,
        frames: [
          expect.objectContaining({ relativePath: "analysis.m", line: 7 }),
          expect.objectContaining({ relativePath: null, line: 99 }),
        ],
        related: [
          expect.objectContaining({
            code: "MATLAB:nestedFailure",
            message: "Underlying calculation failed.",
            frames: [expect.objectContaining({ relativePath: "analysis.m", line: 3 })],
          }),
        ],
      }),
    ]);
  });

  it("rejects invalid MATLAB script names without rejecting spaces in parent folders", () => {
    const revision = AnalysisSourceRevision.make("sha256:test");
    expect(
      matlabRuntimeAdapter.validateSource?.({
        cwd: "/project with spaces",
        relativePath: "folder with spaces/valid_name2.m",
        revision,
      }),
    ).toBeNull();
    expect(
      matlabRuntimeAdapter.validateSource?.({
        cwd: "/project",
        relativePath: "my-analysis.m",
        revision,
      }),
    ).toContain("MATLAB script names");
  });
});

describe.runIf(process.env.SCIENT_REAL_MATLAB === "1")("MATLAB runtime adapter integration", () => {
  it("verifies the installed MATLAB release, architecture, Java, and toolbox inventory", async () => {
    const executablePath =
      process.env.SCIENT_REAL_MATLAB_EXECUTABLE ?? "/Applications/MATLAB_R2026a.app/bin/matlab";
    const prepared = await prepareMatlabVerification(runtimeProfile(executablePath));
    try {
      const result = await new Promise<{
        code: number | null;
        output: Array<{ stream: "stdout" | "stderr"; text: string }>;
      }>((resolve, reject) => {
        const child = NodeChildProcess.spawn(prepared.executable, prepared.args, {
          cwd: prepared.cwd,
          env: { ...process.env, ...prepared.environment },
          stdio: ["ignore", "pipe", "pipe"],
          timeout: 300_000,
        });
        const output: Array<{ stream: "stdout" | "stderr"; text: string }> = [];
        child.stdout.on("data", (chunk) => output.push({ stream: "stdout", text: String(chunk) }));
        child.stderr.on("data", (chunk) => output.push({ stream: "stderr", text: String(chunk) }));
        child.on("error", reject);
        child.on("close", (code) => resolve({ code, output }));
      });
      const verification = await prepared.collect({
        exitCode: result.code,
        timedOut: false,
        output: result.output,
        verifiedAt: "2026-08-12T00:00:00.000Z",
        durationMs: 0,
      });
      expect(verification, result.output.map((chunk) => chunk.text).join("\n")).toMatchObject({
        status: "ready",
        release: "R2026a",
      });
      expect(verification.architecture).not.toBeNull();
      expect(verification.installationRoot).toContain("MATLAB_R2026a.app");
      expect(verification.toolboxes.some((toolbox) => toolbox.name === "MATLAB")).toBe(true);
    } finally {
      await prepared.cleanup();
    }
  }, 360_000);

  it("captures the fast static preview and native continuation formats by default", async () => {
    const executablePath =
      process.env.SCIENT_REAL_MATLAB_EXECUTABLE ?? "/Applications/MATLAB_R2026a.app/bin/matlab";
    const context = await runContext(executablePath);
    await NodeFSP.writeFile(
      context.absoluteSourcePath,
      "set(groot, 'defaultFigureVisible', 'off'); x = linspace(0, 2*pi, 100); figure; plot(x, sin(x), 'LineWidth', 2); title('Scient real adapter probe');\n",
      "utf8",
    );
    await prepareMatlabRun(context);
    const command = prepareMatlabCommand({
      profile: context.runtime,
      source: context.source,
      absoluteSourcePath: context.absoluteSourcePath,
      artifactStagingDirectory: context.artifactStagingDirectory,
    });
    const result = await new Promise<{ code: number | null; output: string }>((resolve, reject) => {
      const child = NodeChildProcess.spawn(command.executable, command.args, {
        cwd: command.cwd,
        env: { ...process.env, ...command.environment },
        stdio: ["ignore", "pipe", "pipe"],
        timeout: 300_000,
      });
      let output = "";
      child.stdout.on("data", (chunk) => {
        output += String(chunk);
      });
      child.stderr.on("data", (chunk) => {
        output += String(chunk);
      });
      child.on("error", reject);
      child.on("close", (code) => resolve({ code, output }));
    });
    expect(result, result.output).toMatchObject({ code: 0 });

    const artifacts = (await collectMatlabArtifacts(context)).candidates;
    expect(artifacts).toHaveLength(1);
    expect(artifacts[0]?.representations.map((representation) => representation.mediaType)).toEqual(
      ["application/vnd.mathworks.matlab.figure", "image/png"],
    );
  }, 360_000);
});
