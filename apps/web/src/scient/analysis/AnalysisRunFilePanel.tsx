import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import { AnalysisSourceRevision } from "@t3tools/contracts";
import type {
  AnalysisRunSnapshot,
  AnalysisRunSummary,
  AnalysisDiagnostic,
  AnalysisRuntimeKind,
  AnalysisRuntimeProfile,
  EnvironmentId,
  ScopedThreadRef,
} from "@t3tools/contracts";
import {
  ChevronDown,
  ChevronUp,
  CircleAlert,
  LoaderCircle,
  Play,
  RotateCcw,
  Square,
} from "lucide-react";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/unstable/reactivity";
import { useEffect, useMemo, useRef, useState } from "react";

import { Button } from "~/components/ui/button";
import { Input } from "~/components/ui/input";
import { ScrollArea } from "~/components/ui/scroll-area";
import { stackedThreadToast, toastManager } from "~/components/ui/toast";
import { analysisEnvironment } from "~/state/analysis";
import { useAtomCommand } from "~/state/use-atom-command";
import { useComposerDraftStore } from "~/composerDraftStore";
import { useRightPanelStore } from "~/rightPanelStore";

import {
  analysisOperationReason,
  analysisRunIdToAutoExpand,
  emptyAnalysisRunOutputLabel,
  isTerminalAnalysisRunStatus,
} from "./analysisRunUiState";
import { AnalysisArtifactStrip } from "./AnalysisArtifactStrip";
import { artifactDisplayStatus, runForArtifactDisplay } from "./analysisArtifactPresentation";

interface AnalysisRunFilePanelProps {
  readonly environmentId: EnvironmentId;
  readonly threadRef: ScopedThreadRef;
  readonly cwd: string;
  readonly relativePath: string;
  readonly sourceRevision: string;
  readonly sourcePending: boolean;
  readonly runtimeKind: AnalysisRuntimeKind;
  readonly runtimeLabel: string;
}

function resultValue<A, E>(result: AsyncResult.AsyncResult<A, E>): A | null {
  return Option.getOrNull(AsyncResult.value(result));
}

function failureMessage(result: {
  readonly cause: Parameters<typeof squashAtomCommandFailure>[0]["cause"];
}): string {
  const error = squashAtomCommandFailure(result);
  return error instanceof Error ? error.message : "The analysis operation failed.";
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(1)} GB`;
}

function withOccurrenceKeys<A>(
  values: ReadonlyArray<A>,
  keyFor: (value: A) => string,
): ReadonlyArray<{ readonly key: string; readonly value: A }> {
  const occurrences = new Map<string, number>();
  return values.map((value) => {
    const base = keyFor(value);
    const occurrence = occurrences.get(base) ?? 0;
    occurrences.set(base, occurrence + 1);
    return { key: `${base}:${occurrence}`, value };
  });
}

function diagnosticFrameKey(frame: AnalysisDiagnostic["frames"][number]): string {
  return [frame.relativePath ?? "external", frame.functionName ?? "", frame.line ?? 0].join(":");
}

function outputSegments(chunks: AnalysisRunSnapshot["receipt"]["output"]) {
  const segments: Array<{
    readonly firstSequence: number;
    readonly stream: AnalysisRunSnapshot["receipt"]["output"][number]["stream"];
    text: string;
  }> = [];
  for (const chunk of chunks) {
    const previous = segments.at(-1);
    if (previous?.stream === chunk.stream) {
      previous.text += chunk.text;
    } else {
      segments.push({ firstSequence: chunk.sequence, stream: chunk.stream, text: chunk.text });
    }
  }
  return segments;
}

function statusLabel(
  run: AnalysisRunSnapshot | AnalysisRunSummary | null,
  runtimeLabel: string,
): string {
  if (!run) return "No runs yet";
  switch (run.receipt.status) {
    case "queued":
      return run.queuePosition ? `Waiting · ${run.queuePosition} in queue` : "Waiting";
    case "starting":
      return run.phase === "launching" ? `Launching ${runtimeLabel}` : `Starting ${runtimeLabel}`;
    case "running":
      if (run.receipt.cancellationRequested) return "Stopping";
      if (run.phase === "capturing") return "Capturing figures";
      if (run.phase === "publishing") return "Publishing figures";
      return "Running";
    case "succeeded":
      return "Succeeded";
    case "failed":
      return "Failed";
    case "cancelled":
      return "Cancelled";
    case "lost":
      return "Interrupted when Scient closed";
  }
}

function RunOutputView(props: {
  readonly run: AnalysisRunSnapshot;
  readonly runtimeLabel: string;
  readonly threadRef: ScopedThreadRef;
  readonly onAskAboutDiagnostic: (
    diagnostic: AnalysisDiagnostic,
    runId: AnalysisRunSnapshot["receipt"]["runId"],
  ) => void;
}) {
  const segments = useMemo(
    () => outputSegments(props.run.receipt.output),
    [props.run.receipt.output],
  );
  const output = (
    <pre className="whitespace-pre-wrap break-words font-mono text-xs leading-relaxed">
      {segments.length > 0
        ? segments.map((segment) => (
            <span
              key={segment.firstSequence}
              className={segment.stream === "stderr" ? "text-destructive" : undefined}
            >
              {segment.text}
            </span>
          ))
        : emptyAnalysisRunOutputLabel(props.run.receipt.status)}
    </pre>
  );
  return (
    <ScrollArea className="max-h-64 min-h-24">
      <div className="space-y-2 p-3">
        <div className="flex items-center justify-between text-[11px] text-muted-foreground">
          <span>{statusLabel(props.run, props.runtimeLabel)}</span>
          <span className="max-w-[50%] truncate font-mono" title={props.run.source.cwd}>
            {props.run.source.cwd}
          </span>
        </div>
        {props.run.diagnostics.map((diagnostic) => (
          <div
            key={diagnostic.diagnosticId}
            className="space-y-1 rounded-md border border-destructive/25 bg-destructive/5 p-2 text-xs"
          >
            <div className="flex items-start gap-2">
              <CircleAlert className="mt-0.5 size-3.5 shrink-0 text-destructive" />
              <div className="min-w-0 flex-1">
                <p className="font-medium text-destructive">
                  {diagnostic.code ? `${diagnostic.code}: ` : ""}
                  {diagnostic.message}
                </p>
                {diagnostic.relativePath ? (
                  <button
                    type="button"
                    className="mt-1 font-mono text-muted-foreground underline-offset-2 hover:underline"
                    onClick={() =>
                      useRightPanelStore
                        .getState()
                        .openFile(
                          props.threadRef,
                          diagnostic.relativePath!,
                          diagnostic.line ?? undefined,
                        )
                    }
                  >
                    {diagnostic.relativePath}
                    {diagnostic.line ? `:${diagnostic.line}` : ""}
                  </button>
                ) : null}
                {diagnostic.frames.length > 0 ? (
                  <details className="mt-1">
                    <summary className="cursor-pointer text-[11px] text-muted-foreground">
                      Stack frames
                    </summary>
                    <div className="mt-1 flex flex-col items-start gap-0.5">
                      {withOccurrenceKeys(diagnostic.frames.slice(0, 20), diagnosticFrameKey).map(
                        ({ key, value: frame }) =>
                          frame.relativePath ? (
                            <button
                              key={key}
                              type="button"
                              className="max-w-full truncate font-mono text-[11px] text-muted-foreground underline-offset-2 hover:underline"
                              title={`${frame.relativePath}${frame.line ? `:${frame.line}` : ""}`}
                              onClick={() =>
                                useRightPanelStore
                                  .getState()
                                  .openFile(
                                    props.threadRef,
                                    frame.relativePath!,
                                    frame.line ?? undefined,
                                  )
                              }
                            >
                              {frame.functionName ? `${frame.functionName} · ` : ""}
                              {frame.relativePath}
                              {frame.line ? `:${frame.line}` : ""}
                            </button>
                          ) : (
                            <span key={key} className="font-mono text-[11px] text-muted-foreground">
                              {frame.functionName ?? "External MATLAB frame"}
                              {frame.line ? `:${frame.line}` : ""}
                            </span>
                          ),
                      )}
                    </div>
                  </details>
                ) : null}
                {diagnostic.related.length > 0 ? (
                  <details className="mt-1">
                    <summary className="cursor-pointer text-[11px] text-muted-foreground">
                      {diagnostic.related.length} related cause
                      {diagnostic.related.length === 1 ? "" : "s"}
                    </summary>
                    <div className="mt-1 space-y-1 text-[11px] text-muted-foreground">
                      {withOccurrenceKeys(
                        diagnostic.related,
                        (related) => `${related.code ?? "cause"}:${related.message}`,
                      ).map(({ key, value: related }) => (
                        <div key={key} className="space-y-0.5">
                          <p>
                            {related.code ? `${related.code}: ` : ""}
                            {related.message}
                          </p>
                          {withOccurrenceKeys(related.frames.slice(0, 5), diagnosticFrameKey).map(
                            ({ key: frameKey, value: frame }) =>
                              frame.relativePath ? (
                                <button
                                  key={frameKey}
                                  type="button"
                                  className="block max-w-full truncate font-mono underline-offset-2 hover:underline"
                                  onClick={() =>
                                    useRightPanelStore
                                      .getState()
                                      .openFile(
                                        props.threadRef,
                                        frame.relativePath!,
                                        frame.line ?? undefined,
                                      )
                                  }
                                >
                                  {frame.functionName ? `${frame.functionName} · ` : ""}
                                  {frame.relativePath}
                                  {frame.line ? `:${frame.line}` : ""}
                                </button>
                              ) : null,
                          )}
                        </div>
                      ))}
                    </div>
                  </details>
                ) : null}
              </div>
              <Button
                size="xs"
                variant="ghost"
                onClick={() => props.onAskAboutDiagnostic(diagnostic, props.run.receipt.runId)}
              >
                Ask agent
              </Button>
            </div>
          </div>
        ))}
        {props.run.diagnostics.length > 0 ? (
          <details>
            <summary className="cursor-pointer text-[11px] text-muted-foreground">
              Raw {props.runtimeLabel} output
            </summary>
            <div className="pt-2">{output}</div>
          </details>
        ) : (
          output
        )}
        {props.run.receipt.failureMessage ? (
          <p className="text-xs text-destructive">{props.run.receipt.failureMessage}</p>
        ) : null}
      </div>
    </ScrollArea>
  );
}

function PersistedRunOutput(props: {
  readonly environmentId: EnvironmentId;
  readonly cwd: string;
  readonly runId: AnalysisRunSnapshot["receipt"]["runId"];
  readonly runtimeLabel: string;
  readonly threadRef: ScopedThreadRef;
  readonly onAskAboutDiagnostic: (
    diagnostic: AnalysisDiagnostic,
    runId: AnalysisRunSnapshot["receipt"]["runId"],
  ) => void;
  readonly storageStatus: AnalysisRunSummary["localStorage"]["status"];
}) {
  const runAtom = analysisEnvironment.run({
    environmentId: props.environmentId,
    input: { cwd: props.cwd, runId: props.runId },
  });
  const result = useAtomValue(runAtom);
  const refreshRun = useAtomRefresh(runAtom);
  useEffect(() => refreshRun(), [props.storageStatus, refreshRun]);
  const run = resultValue(result);
  if (run)
    return (
      <RunOutputView
        run={run}
        runtimeLabel={props.runtimeLabel}
        threadRef={props.threadRef}
        onAskAboutDiagnostic={props.onAskAboutDiagnostic}
      />
    );
  if (result._tag === "Failure") {
    return <div className="p-3 text-xs text-destructive">Unable to load this run output.</div>;
  }
  return <div className="p-3 text-xs text-muted-foreground">Loading run output…</div>;
}

function runtimeStatus(profile: AnalysisRuntimeProfile | null, runtimeLabel: string): string {
  if (!profile) return `Checking ${runtimeLabel}…`;
  if (profile.availability === "available") {
    const versionLabel = profile.version ? ` ${profile.version}` : "";
    if (profile.verification?.status === "ready") return `${runtimeLabel}${versionLabel} · Ready`;
    if (profile.verification) {
      const statusLabel = {
        "needs-sign-in": "Sign-in required",
        "license-unavailable": "License unavailable",
        "missing-dependency": "Missing dependency",
        "startup-failed": "Startup failed",
        "timed-out": "Verification timed out",
        unknown: "Status unknown",
      }[profile.verification.status];
      return `${runtimeLabel}${versionLabel} · ${statusLabel}`;
    }
    return `${runtimeLabel}${versionLabel} · Not verified`;
  }
  return profile.detail ?? `${runtimeLabel} is unavailable.`;
}

export function AnalysisRunFilePanel(props: AnalysisRunFilePanelProps) {
  const [historyCursor, setHistoryCursor] = useState<string | null>(null);
  const [loadedHistory, setLoadedHistory] = useState<ReadonlyArray<AnalysisRunSummary>>([]);
  const runtimeAtom = analysisEnvironment.runtimes({
    environmentId: props.environmentId,
    input: { cwd: props.cwd },
  });
  const runsAtom = analysisEnvironment.runs({
    environmentId: props.environmentId,
    input: {
      cwd: props.cwd,
      relativePath: props.relativePath,
      limit: 30,
      ...(historyCursor === null ? {} : { cursor: historyCursor }),
    },
  });
  const eventsAtom = analysisEnvironment.runEvents({
    environmentId: props.environmentId,
    input: { cwd: props.cwd, relativePath: props.relativePath },
  });
  const storageAtom = analysisEnvironment.storage({
    environmentId: props.environmentId,
    input: { cwd: props.cwd },
  });
  const runtimeResult = useAtomValue(runtimeAtom);
  const runsResult = useAtomValue(runsAtom);
  const eventResult = useAtomValue(eventsAtom);
  const storageResult = useAtomValue(storageAtom);
  const refreshRuntime = useAtomRefresh(runtimeAtom);
  const refreshRuns = useAtomRefresh(runsAtom);
  const refreshStorage = useAtomRefresh(storageAtom);
  const startRun = useAtomCommand(analysisEnvironment.startRun, { reportFailure: false });
  const cancelRun = useAtomCommand(analysisEnvironment.cancelRun, { reportFailure: false });
  const configureRuntime = useAtomCommand(analysisEnvironment.configureRuntime, {
    reportFailure: false,
  });
  const verifyRuntime = useAtomCommand(analysisEnvironment.verifyRuntime, { reportFailure: false });
  const cleanupRun = useAtomCommand(analysisEnvironment.cleanupRun, { reportFailure: false });
  const cleanupProject = useAtomCommand(analysisEnvironment.cleanupProject, {
    reportFailure: false,
  });
  const [expanded, setExpanded] = useState(false);
  const [showRuntimeDetails, setShowRuntimeDetails] = useState(false);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [runtimePath, setRuntimePath] = useState("");
  const [operation, setOperation] = useState<
    "configure" | "verify" | "run" | "cancel" | "cleanup-run" | "cleanup-project" | null
  >(null);
  const refreshedTerminalRunIdRef = useRef<string | null>(null);
  const observedStreamRunIdsRef = useRef<Set<string> | null>(null);

  const inspection = resultValue(runtimeResult);
  const profile =
    inspection?.runtimes.find((runtime) => runtime.kind === props.runtimeKind) ?? null;
  const historyPage = resultValue(runsResult);
  const history = loadedHistory;
  const streamedRunValue = resultValue(eventResult);
  const streamedRuns = streamedRunValue ?? [];
  const runs = useMemo(() => {
    const byId = new Map<string, AnalysisRunSummary | AnalysisRunSnapshot>(
      history.map((run) => [run.receipt.runId, run]),
    );
    for (const run of streamedRuns) byId.set(run.receipt.runId, run);
    return [...byId.values()].toSorted((left, right) =>
      right.receipt.startedAt.localeCompare(left.receipt.startedAt),
    );
  }, [history, streamedRuns]);
  const selectedRun = runs.find((run) => run.receipt.runId === selectedRunId) ?? runs[0] ?? null;
  const selectedLiveRun =
    streamedRuns.find((run) => run.receipt.runId === selectedRun?.receipt.runId) ?? null;
  const activeRun =
    streamedRuns.find((run) => !isTerminalAnalysisRunStatus(run.receipt.status)) ?? null;
  const latestTerminalRunId =
    streamedRuns.find((run) => isTerminalAnalysisRunStatus(run.receipt.status))?.receipt.runId ??
    null;
  const artifactRun = runForArtifactDisplay(runs, selectedRun);
  const artifactStatus = artifactRun
    ? artifactDisplayStatus({
        artifactRun,
        selectedRun,
        activeRun,
        sourceRevision: props.sourceRevision,
      })
    : null;
  const storage = resultValue(storageResult);
  const projectNotInitialized = analysisOperationReason(runsResult) === "project-not-initialized";

  useEffect(() => {
    if (profile?.executablePath) setRuntimePath(profile.executablePath);
  }, [profile?.executablePath]);

  useEffect(() => {
    setHistoryCursor(null);
    setLoadedHistory([]);
  }, [props.cwd, props.relativePath]);

  useEffect(() => {
    if (!historyPage) return;
    setLoadedHistory((current) => {
      if (historyCursor === null) return historyPage.runs;
      const byId = new Map(current.map((run) => [run.receipt.runId, run]));
      for (const run of historyPage.runs) byId.set(run.receipt.runId, run);
      return [...byId.values()].toSorted((left, right) =>
        right.receipt.startedAt.localeCompare(left.receipt.startedAt),
      );
    });
  }, [historyCursor, historyPage]);

  useEffect(() => {
    if (streamedRunValue === null) return;
    const nextObserved = new Set(streamedRuns.map((run) => run.receipt.runId));
    const previousObserved = observedStreamRunIdsRef.current;
    observedStreamRunIdsRef.current = new Set([...(previousObserved ?? []), ...nextObserved]);
    // The subscription starts with the latest persisted run. Keep its figures visible,
    // but do not force old console output open every time the user revisits the file.
    const runIdToExpand = analysisRunIdToAutoExpand(streamedRuns, previousObserved);
    if (!runIdToExpand) return;
    setSelectedRunId(runIdToExpand);
    setExpanded(true);
  }, [streamedRunValue, streamedRuns]);

  useEffect(() => {
    if (!latestTerminalRunId || refreshedTerminalRunIdRef.current === latestTerminalRunId) return;
    refreshedTerminalRunIdRef.current = latestTerminalRunId;
    setHistoryCursor(null);
    setLoadedHistory([]);
    refreshRuns();
  }, [latestTerminalRunId, refreshRuns]);

  const reportFailure = (
    title: string,
    result: { readonly cause: Parameters<typeof squashAtomCommandFailure>[0]["cause"] },
  ) => {
    toastManager.add(
      stackedThreadToast({ type: "error", title, description: failureMessage(result) }),
    );
  };

  const handleRun = async () => {
    if (!profile || profile.availability !== "available") {
      setExpanded(true);
      return;
    }
    setOperation("run");
    const result = await startRun({
      environmentId: props.environmentId,
      input: {
        cwd: props.cwd,
        relativePath: props.relativePath,
        sourceRevision: AnalysisSourceRevision.make(props.sourceRevision),
        runtimeId: profile.id,
      },
    });
    setOperation(null);
    if (result._tag === "Success") {
      setSelectedRunId(result.value.receipt.runId);
      setExpanded(true);
      setHistoryCursor(null);
      setLoadedHistory([]);
      refreshRuns();
    } else if (!isAtomCommandInterrupted(result)) {
      reportFailure(`Unable to run ${props.runtimeLabel} file`, result);
      refreshRuntime();
    }
  };

  const handleCancel = async () => {
    if (!activeRun) return;
    setOperation("cancel");
    const result = await cancelRun({
      environmentId: props.environmentId,
      input: { cwd: props.cwd, runId: activeRun.receipt.runId },
    });
    setOperation(null);
    if (result._tag !== "Success" && !isAtomCommandInterrupted(result)) {
      reportFailure(`Unable to stop ${props.runtimeLabel}`, result);
    }
  };

  const handleConfigure = async () => {
    setOperation("configure");
    const result = await configureRuntime({
      environmentId: props.environmentId,
      input: {
        cwd: props.cwd,
        runtimeKind: props.runtimeKind,
        executablePath: runtimePath.trim() || null,
      },
    });
    setOperation(null);
    if (result._tag === "Success") {
      refreshRuntime();
    } else if (!isAtomCommandInterrupted(result)) {
      reportFailure(`Unable to configure ${props.runtimeLabel}`, result);
    }
  };

  const handleVerify = async () => {
    if (!profile) return;
    setOperation("verify");
    const result = await verifyRuntime({
      environmentId: props.environmentId,
      input: { cwd: props.cwd, runtimeId: profile.id, refresh: true },
    });
    setOperation(null);
    if (result._tag === "Success") {
      refreshRuntime();
    } else if (!isAtomCommandInterrupted(result)) {
      reportFailure(`Unable to verify ${props.runtimeLabel}`, result);
    }
  };

  const handleCleanupRun = async () => {
    if (!selectedRun || selectedRun.localStorage.status !== "retained") return;
    const retained = selectedRun.localStorage.totalBytes;
    if (
      !globalThis.confirm(
        `Remove ${formatBytes(retained)} of local output and artifacts from this run? Its status, diagnostics, hashes, and provenance will remain in history.`,
      )
    ) {
      return;
    }
    setOperation("cleanup-run");
    const result = await cleanupRun({
      environmentId: props.environmentId,
      input: { cwd: props.cwd, runId: selectedRun.receipt.runId },
    });
    setOperation(null);
    if (result._tag === "Success") {
      setHistoryCursor(null);
      setLoadedHistory([]);
      refreshRuns();
      refreshStorage();
    } else if (!isAtomCommandInterrupted(result)) {
      reportFailure("Unable to remove local run data", result);
      setHistoryCursor(null);
      setLoadedHistory([]);
      refreshRuns();
      refreshStorage();
    }
  };

  const handleCleanupProject = async () => {
    if (!storage || storage.totalBytes === 0) return;
    if (
      !globalThis.confirm(
        `Remove ${formatBytes(storage.totalBytes)} of retained ${props.runtimeLabel} output and artifacts across ${storage.retainedRunCount} local runs? Run metadata, diagnostics, hashes, and provenance will remain.`,
      )
    ) {
      return;
    }
    setOperation("cleanup-project");
    const result = await cleanupProject({
      environmentId: props.environmentId,
      input: { cwd: props.cwd, expectedRetainedBytes: storage.totalBytes },
    });
    setOperation(null);
    if (result._tag === "Success") {
      setHistoryCursor(null);
      setLoadedHistory([]);
      refreshRuns();
      refreshStorage();
    } else if (!isAtomCommandInterrupted(result)) {
      reportFailure("Unable to clean up local analysis data", result);
      setHistoryCursor(null);
      setLoadedHistory([]);
      refreshRuns();
      refreshStorage();
    }
  };

  const askAboutDiagnostic = (
    diagnostic: AnalysisDiagnostic,
    runId: AnalysisRunSnapshot["receipt"]["runId"],
  ) => {
    const store = useComposerDraftStore.getState();
    const current = store.getComposerDraft(props.threadRef)?.prompt ?? "";
    const location = diagnostic.relativePath
      ? `${diagnostic.relativePath}${diagnostic.line ? `:${diagnostic.line}` : ""}`
      : props.relativePath;
    const request = `Please diagnose and fix this ${props.runtimeLabel} error in ${location}. Scient analysis run ID: ${runId}; diagnostic ID: ${diagnostic.diagnosticId}.\n\n${diagnostic.code ? `${diagnostic.code}: ` : ""}${diagnostic.message}`;
    store.setPrompt(
      props.threadRef,
      current.trim().length > 0 ? `${current}\n\n${request}` : request,
    );
    toastManager.add(
      stackedThreadToast({
        type: "success",
        title: `Added ${props.runtimeLabel} error to the composer`,
        description: "Review the request, then send it to your agent.",
      }),
    );
  };

  const runtimeReady = profile?.availability === "available";
  const runtimeInspectionPending = inspection === null && runtimeResult._tag !== "Failure";
  const primaryActionIsSetup = !projectNotInitialized && !runtimeReady && !runtimeInspectionPending;
  const primaryActionDisabled = primaryActionIsSetup
    ? operation !== null
    : props.sourcePending ||
      runsResult.waiting ||
      projectNotInitialized ||
      activeRun !== null ||
      operation !== null ||
      !runtimeReady;
  const verificationReady = profile?.verification?.status === "ready";
  const runtimeStatusText =
    runtimeResult._tag === "Failure" && profile === null
      ? `Unable to check ${props.runtimeLabel}`
      : runtimeStatus(profile, props.runtimeLabel);
  const operationStatus =
    operation === "run"
      ? "Verifying source"
      : operation === "verify"
        ? `Verifying ${props.runtimeLabel}…`
        : operation === "configure"
          ? `Saving ${props.runtimeLabel} path…`
          : operation === "cleanup-run" || operation === "cleanup-project"
            ? "Removing local data…"
            : null;

  return (
    <section
      className="shrink-0 border-t border-border bg-muted/20"
      aria-label={`${props.runtimeLabel} Run File`}
    >
      <div className="flex min-h-10 items-center gap-2 px-3 py-1.5">
        {activeRun ? (
          <Button
            size="xs"
            variant="outline"
            disabled={operation !== null || activeRun.receipt.cancellationRequested}
            onClick={handleCancel}
          >
            {operation === "cancel" ? <LoaderCircle className="animate-spin" /> : <Square />}
            Stop
          </Button>
        ) : (
          <Button size="xs" disabled={primaryActionDisabled} onClick={handleRun}>
            {operation === "run" ? (
              <LoaderCircle className="animate-spin" />
            ) : primaryActionIsSetup ? null : (
              <Play />
            )}
            {primaryActionIsSetup
              ? `Set up ${props.runtimeLabel}`
              : props.sourcePending
                ? "Saving…"
                : "Run file"}
          </Button>
        )}
        <span
          className="min-w-0 flex-1 truncate text-xs text-muted-foreground"
          title={runtimeStatusText}
        >
          {activeRun
            ? statusLabel(activeRun, props.runtimeLabel)
            : operationStatus
              ? operationStatus
              : projectNotInitialized
                ? "Set up this folder as a Scient project to run"
                : runtimeStatusText}
        </span>
        {runtimeReady ? (
          <Button
            size="xs"
            variant="ghost"
            aria-expanded={showRuntimeDetails}
            onClick={() => setShowRuntimeDetails((value) => !value)}
          >
            Details
          </Button>
        ) : null}
        {runs.length > 1 ? (
          <select
            className="max-w-44 rounded-md border border-input bg-background px-2 py-1 text-xs"
            value={selectedRun?.receipt.runId ?? ""}
            onChange={(event) => {
              setSelectedRunId(event.target.value);
              setExpanded(true);
            }}
            aria-label={`Local ${props.runtimeLabel} run history`}
          >
            {runs.map((run) => (
              <option key={run.receipt.runId} value={run.receipt.runId}>
                {statusLabel(run, props.runtimeLabel)} ·{" "}
                {new Date(run.receipt.startedAt).toLocaleTimeString()}
              </option>
            ))}
          </select>
        ) : null}
        <Button
          size="icon-xs"
          variant="ghost"
          aria-label={
            expanded
              ? `Collapse ${props.runtimeLabel} output`
              : `Expand ${props.runtimeLabel} output`
          }
          onClick={() => setExpanded((value) => !value)}
        >
          {expanded ? <ChevronDown /> : <ChevronUp />}
        </Button>
      </div>

      {runtimeReady && showRuntimeDetails ? (
        <div className="space-y-2 border-t border-border px-3 py-2 text-[11px]">
          <dl className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1">
            <dt className="text-muted-foreground">Working folder</dt>
            <dd className="truncate font-mono" title={props.cwd}>
              {props.cwd}
            </dd>
            <dt className="text-muted-foreground">Executable</dt>
            <dd className="truncate font-mono" title={profile?.executablePath ?? undefined}>
              {profile?.executablePath}
            </dd>
            <dt className="text-muted-foreground">Release</dt>
            <dd>{profile?.verification?.release ?? profile?.version ?? "Not reported"}</dd>
            <dt className="text-muted-foreground">Architecture</dt>
            <dd>{profile?.verification?.architecture ?? "Not checked"}</dd>
            <dt className="text-muted-foreground">Java</dt>
            <dd>
              {profile?.verification?.javaAvailable === null || !profile?.verification
                ? "Not checked"
                : profile.verification.javaAvailable
                  ? (profile.verification.javaVersion ?? "Available")
                  : "Unavailable"}
            </dd>
            <dt className="text-muted-foreground">Toolboxes</dt>
            <dd>
              {profile?.verification
                ? `${profile.verification.toolboxes.length} detected`
                : "Not checked"}
            </dd>
          </dl>
          <div className="flex flex-wrap gap-2 border-t border-border/60 pt-2">
            <Input
              className="min-w-48 flex-1"
              size="sm"
              value={runtimePath}
              placeholder={`Path to ${props.runtimeLabel} executable`}
              onValueChange={setRuntimePath}
              aria-label={`${props.runtimeLabel} executable path`}
            />
            <Button
              size="sm"
              variant="outline"
              disabled={operation !== null || activeRun !== null}
              onClick={handleConfigure}
            >
              {operation === "configure" ? <LoaderCircle className="animate-spin" /> : null}
              Use path
            </Button>
            <Button
              size="icon-sm"
              variant="ghost"
              disabled={operation !== null || activeRun !== null}
              onClick={() => refreshRuntime()}
              aria-label={`Scan for ${props.runtimeLabel} again`}
            >
              <RotateCcw />
            </Button>
            {verificationReady ? (
              <Button
                size="sm"
                variant="outline"
                disabled={operation !== null || activeRun !== null}
                onClick={handleVerify}
              >
                {operation === "verify" ? <LoaderCircle className="animate-spin" /> : null}
                Verify again
              </Button>
            ) : null}
          </div>
        </div>
      ) : null}

      {artifactRun && artifactStatus && artifactRun.localStorage.status === "retained" ? (
        <AnalysisArtifactStrip
          environmentId={props.environmentId}
          threadRef={props.threadRef}
          run={artifactRun}
          status={artifactStatus}
        />
      ) : null}

      {selectedRun?.artifactReceipt.status === "failed" ? (
        <div
          className="border-t border-warning/30 bg-warning-surface px-3 py-2 text-xs text-warning-foreground"
          role="alert"
        >
          {selectedRun.artifactReceipt.failureMessage ??
            "The run finished, but Scient could not collect its generated figures."}
        </div>
      ) : null}

      {expanded ? (
        <div className="border-t border-border">
          {projectNotInitialized ? (
            <div className="p-3 text-xs text-muted-foreground">
              Set up this folder as a Scient project before running analysis files. Viewing and
              editing remain available.
            </div>
          ) : !runtimeReady ? (
            <div className="space-y-2 p-3">
              <p className="text-xs text-muted-foreground">
                {runtimeResult._tag === "Failure"
                  ? `Unable to inspect ${props.runtimeLabel} on this environment.`
                  : (profile?.detail ?? `Checking for ${props.runtimeLabel} on this environment.`)}
              </p>
              <div className="flex flex-wrap gap-2">
                <Input
                  className="min-w-48 flex-1"
                  size="sm"
                  value={runtimePath}
                  placeholder={`Path to ${props.runtimeLabel} executable`}
                  onValueChange={setRuntimePath}
                  aria-label={`${props.runtimeLabel} executable path`}
                />
                <Button
                  size="sm"
                  variant="outline"
                  disabled={operation !== null}
                  onClick={handleConfigure}
                >
                  {operation === "configure" ? <LoaderCircle className="animate-spin" /> : null}
                  Use path
                </Button>
                <Button
                  size="icon-sm"
                  variant="ghost"
                  disabled={operation !== null}
                  onClick={() => refreshRuntime()}
                  aria-label={`Scan for ${props.runtimeLabel} again`}
                >
                  <RotateCcw />
                </Button>
              </div>
              <p className="text-[11px] text-muted-foreground">
                Viewing and editing the file does not require {props.runtimeLabel}. Scient launches
                it only when you press Run file.
              </p>
            </div>
          ) : !verificationReady ? (
            <div className="border-b border-border px-3 py-2 text-xs">
              <div className="flex items-start gap-2">
                <span className="min-w-0 flex-1 leading-relaxed text-muted-foreground">
                  {profile?.verification?.detail ??
                    `${props.runtimeLabel} was found. You can run now or verify the installation first.`}
                </span>
                <Button
                  size="xs"
                  variant="outline"
                  disabled={operation !== null || activeRun !== null}
                  onClick={handleVerify}
                >
                  {operation === "verify" ? <LoaderCircle className="animate-spin" /> : null}
                  {profile?.verification ? "Verify again" : "Verify"}
                </Button>
              </div>
            </div>
          ) : null}
          {!projectNotInitialized && selectedLiveRun ? (
            <RunOutputView
              run={selectedLiveRun}
              runtimeLabel={props.runtimeLabel}
              threadRef={props.threadRef}
              onAskAboutDiagnostic={askAboutDiagnostic}
            />
          ) : !projectNotInitialized && selectedRun ? (
            <PersistedRunOutput
              environmentId={props.environmentId}
              cwd={props.cwd}
              runId={selectedRun.receipt.runId}
              runtimeLabel={props.runtimeLabel}
              threadRef={props.threadRef}
              onAskAboutDiagnostic={askAboutDiagnostic}
              storageStatus={selectedRun.localStorage.status}
            />
          ) : !projectNotInitialized && runtimeReady ? (
            <div className="p-3 text-xs text-muted-foreground">
              Run this project-owned source file to stream {props.runtimeLabel} output here.
            </div>
          ) : null}
          {!projectNotInitialized && selectedRun?.localStorage.status === "metadata-only" ? (
            <div className="border-t border-border px-3 py-2 text-[11px] text-muted-foreground">
              Local output and artifact files were removed. Run metadata, diagnostics, hashes, and
              provenance remain available.
            </div>
          ) : null}
          {!projectNotInitialized && storage && storage.totalBytes > 0 ? (
            <div className="flex items-center gap-2 border-t border-border px-3 py-2 text-[11px] text-muted-foreground">
              <span className="min-w-0 flex-1">
                Local results use {formatBytes(storage.totalBytes)} across{" "}
                {storage.retainedRunCount}
                {" retained run"}
                {storage.retainedRunCount === 1 ? "" : "s"}.
              </span>
              {selectedRun?.localStorage.status === "retained" &&
              selectedRun.localStorage.totalBytes > 0 ? (
                <Button
                  size="xs"
                  variant="ghost"
                  disabled={
                    operation !== null || !isTerminalAnalysisRunStatus(selectedRun.receipt.status)
                  }
                  onClick={handleCleanupRun}
                >
                  {operation === "cleanup-run" ? <LoaderCircle className="animate-spin" /> : null}
                  Remove this run
                </Button>
              ) : null}
              <Button
                size="xs"
                variant="ghost"
                disabled={operation !== null || activeRun !== null}
                onClick={handleCleanupProject}
              >
                {operation === "cleanup-project" ? <LoaderCircle className="animate-spin" /> : null}
                Clean up project
              </Button>
            </div>
          ) : null}
          {!projectNotInitialized && historyPage?.hasMore && historyPage.nextCursor ? (
            <div className="border-t border-border p-2 text-center">
              <Button
                size="xs"
                variant="ghost"
                disabled={runsResult.waiting}
                onClick={() => setHistoryCursor(historyPage.nextCursor)}
              >
                {runsResult.waiting ? <LoaderCircle className="animate-spin" /> : null}
                Load older runs
              </Button>
            </div>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}
