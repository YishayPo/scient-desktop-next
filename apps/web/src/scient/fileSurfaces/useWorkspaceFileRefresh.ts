import { useAtomValue } from "@effect/atom-react";
import {
  ProjectWriteFileError,
  type EnvironmentId,
  type ProjectFileWatchEvent,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/unstable/reactivity";
import { useCallback, useEffect, useRef, useState } from "react";

import {
  clearProjectFileQueryData,
  useProjectFileQuery,
} from "~/components/files/projectFilesQueryState";
import { projectEnvironment } from "~/state/projects";

import { hasExternalFileConflict } from "./fileRefreshPolicy";

const isProjectWriteFileError = Schema.is(ProjectWriteFileError);
const EMPTY_FILE_CHANGES_ATOM = Atom.make(
  AsyncResult.initial<ProjectFileWatchEvent, never>(false),
).pipe(Atom.withLabel("scient-file-changes:empty"));

function useWorkspaceFileChanges(
  environmentId: EnvironmentId,
  cwd: string,
  relativePath: string | null,
) {
  const atom =
    relativePath === null
      ? EMPTY_FILE_CHANGES_ATOM
      : projectEnvironment.fileChanges({
          environmentId,
          input: { cwd, relativePath },
        });
  const result = useAtomValue(atom);
  return Option.getOrNull(AsyncResult.value(result));
}

export interface FileSaveResolution {
  readonly id: number;
  readonly relativePath: string;
  readonly revision: string;
  readonly action: "discard" | "retry";
}

export interface FileReloadNotice {
  readonly kind: "external-change" | "manual-reload" | "confirm-overwrite";
  readonly relativePath: string;
  readonly revision: string;
}

/**
 * Scient's additive freshness seam for the inherited file surface. Native
 * watcher hints, manual reload, optimistic-editor conflict
 * detection, and binary-view cache invalidation stay here so FilePreviewPanel
 * only needs a narrow set of bindings when upstream changes it.
 */
export function useWorkspaceFileRefresh(input: {
  readonly environmentId: EnvironmentId;
  readonly cwd: string;
  readonly relativePath: string | null;
  readonly loadAsText: boolean;
  readonly sourcePending: boolean;
}) {
  const file = useProjectFileQuery(
    input.environmentId,
    input.cwd,
    input.relativePath,
    input.loadAsText,
  );
  const fileChange = useWorkspaceFileChanges(input.environmentId, input.cwd, input.relativePath);
  const [reloadNotice, setReloadNotice] = useState<FileReloadNotice | null>(null);
  const [saveResolution, setSaveResolution] = useState<FileSaveResolution | null>(null);
  const [viewerRefreshKey, setViewerRefreshKey] = useState(0);
  const lastObservedChangeRef = useRef<object | null>(null);
  const lastConfirmedSaveRef = useRef<{
    readonly relativePath: string;
    readonly contents: string;
    readonly revision: string;
  } | null>(null);

  const refreshAuthoritativeFile = useCallback(() => {
    if (input.relativePath === null) return;
    file.refresh();
  }, [file.refresh, input.relativePath]);

  const refreshViewer = useCallback(() => {
    refreshAuthoritativeFile();
    setViewerRefreshKey((current) => current + 1);
  }, [refreshAuthoritativeFile]);

  useEffect(() => {
    if (fileChange === null || lastObservedChangeRef.current === fileChange) {
      return;
    }
    lastObservedChangeRef.current = fileChange;
    // Read once when the watcher becomes ready as well as after changes. This
    // closes the query-to-watcher race, bypasses a still-fresh cached read on
    // remount, and realigns the viewer after subscription reconnects.
    refreshViewer();
  }, [fileChange, refreshViewer]);

  useEffect(() => {
    const relativePath = input.relativePath;
    const authoritative = file.authoritativeData;
    if (relativePath === null || authoritative === null) return;
    if (
      hasExternalFileConflict({
        authoritative,
        optimistic: input.sourcePending ? file.data : null,
        lastConfirmedSave:
          lastConfirmedSaveRef.current?.relativePath === relativePath
            ? lastConfirmedSaveRef.current
            : null,
        pending: input.sourcePending,
      })
    ) {
      setReloadNotice((current) =>
        current?.kind === "confirm-overwrite" && current.relativePath === relativePath
          ? current
          : {
              kind: "external-change",
              relativePath,
              revision: authoritative.revision,
            },
      );
    }
  }, [file.authoritativeData, file.data, input.relativePath, input.sourcePending]);

  useEffect(() => {
    setReloadNotice(null);
    setSaveResolution(null);
    lastObservedChangeRef.current = null;
    lastConfirmedSaveRef.current = null;
  }, [input.relativePath]);

  useEffect(() => {
    const authoritativeRevision = file.authoritativeData?.revision;
    if (authoritativeRevision === undefined) return;
    setReloadNotice((current) =>
      current?.kind === "manual-reload" && current.relativePath === input.relativePath
        ? { ...current, revision: authoritativeRevision }
        : current,
    );
  }, [file.authoritativeData, input.relativePath]);

  const handleSaveFailure = useCallback(
    (path: string, error: unknown) => {
      if (path !== input.relativePath || !isProjectWriteFileError(error)) return;
      if (error.failure !== "revision_conflict" || error.currentRevision === undefined) return;
      setReloadNotice({
        kind: "external-change",
        relativePath: path,
        revision: error.currentRevision,
      });
      refreshAuthoritativeFile();
    },
    [input.relativePath, refreshAuthoritativeFile],
  );

  const handleSaveConfirmed = useCallback((path: string, contents: string, revision: string) => {
    lastConfirmedSaveRef.current = { relativePath: path, contents, revision };
    setReloadNotice((current) => (current?.relativePath === path ? null : current));
  }, []);

  const handleSaveResolutionApplied = useCallback(() => {
    setSaveResolution(null);
  }, []);

  const requestManualReload = useCallback(() => {
    if (input.relativePath === null) return;
    if (input.sourcePending && file.data !== null) {
      setReloadNotice({
        kind: "manual-reload",
        relativePath: input.relativePath,
        revision: file.authoritativeData?.revision ?? file.data.revision,
      });
      refreshViewer();
      return;
    }
    clearProjectFileQueryData(input.environmentId, input.cwd, input.relativePath);
    setReloadNotice(null);
    refreshViewer();
  }, [
    file.authoritativeData,
    file.data,
    input.cwd,
    input.environmentId,
    input.relativePath,
    input.sourcePending,
    refreshViewer,
  ]);

  const resolveReloadNotice = useCallback(
    (action: "discard" | "retry") => {
      if (reloadNotice === null || input.relativePath === null) return;
      setSaveResolution({
        id: (saveResolution?.id ?? 0) + 1,
        relativePath: input.relativePath,
        revision: reloadNotice.revision,
        action,
      });
      if (action === "discard") {
        clearProjectFileQueryData(input.environmentId, input.cwd, input.relativePath);
        setReloadNotice(null);
        refreshViewer();
      }
    },
    [
      input.cwd,
      input.environmentId,
      input.relativePath,
      refreshViewer,
      reloadNotice,
      saveResolution?.id,
    ],
  );

  const cancelReloadNotice = useCallback(() => {
    setReloadNotice((current) => {
      if (current?.kind === "confirm-overwrite") {
        return { ...current, kind: "external-change" };
      }
      return null;
    });
  }, []);

  const requestOverwrite = useCallback(() => {
    setReloadNotice((current) =>
      current?.kind === "external-change" ? { ...current, kind: "confirm-overwrite" } : current,
    );
  }, []);

  return {
    cancelReloadNotice,
    file,
    handleSaveConfirmed,
    handleSaveFailure,
    handleSaveResolutionApplied,
    reloadNotice,
    requestManualReload,
    requestOverwrite,
    resolveReloadNotice,
    saveResolution,
    viewerRefreshKey,
  };
}
