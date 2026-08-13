import * as NodeBuffer from "node:buffer";

import {
  AnalysisRunSummary,
  type AnalysisListRunsResult,
  type AnalysisRunSummary as AnalysisRunSummaryType,
  type AnalysisStorageSummary,
} from "@scientfactory/analysis";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runScientMigrations } from "../../orchestration/scient-fork/scientMigrator.ts";

const SummaryJson = Schema.fromJsonString(AnalysisRunSummary);
const CursorJson = Schema.fromJsonString(
  Schema.Struct({
    startedAt: Schema.String,
    runId: Schema.String,
  }),
);
const encodeSummary = Schema.encodeEffect(SummaryJson);
const decodeSummary = Schema.decodeUnknownEffect(SummaryJson);
const encodeCursorJson = Schema.encodeSync(CursorJson);
const decodeCursorJson = Schema.decodeUnknownOption(CursorJson);

interface IndexedRunRow {
  readonly summary_json: string;
}

interface ProjectionStateRow {
  readonly revision: number;
  readonly clean: number;
}

interface StorageSummaryRow {
  readonly retained_run_count: number;
  readonly metadata_only_run_count: number;
  readonly output_bytes: number;
  readonly artifact_bytes: number;
  readonly total_bytes: number;
}

export interface AnalysisIndexMutation {
  readonly revision: number;
  readonly wasClean: boolean;
}

export class AnalysisRunIndexError extends Schema.TaggedErrorClass<AnalysisRunIndexError>()(
  "AnalysisRunIndexError",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {}
const isAnalysisRunIndexError = Schema.is(AnalysisRunIndexError);

function cursorFor(run: AnalysisRunSummaryType): string {
  return NodeBuffer.Buffer.from(
    encodeCursorJson({ startedAt: run.receipt.startedAt, runId: run.receipt.runId }),
    "utf8",
  ).toString("base64url");
}

function decodeCursor(
  value: string,
): { readonly startedAt: string; readonly runId: string } | null {
  try {
    const decoded = decodeCursorJson(NodeBuffer.Buffer.from(value, "base64url").toString("utf8"));
    return decoded._tag === "Some" ? decoded.value : null;
  } catch {
    return null;
  }
}

export class AnalysisRunIndex extends Context.Service<
  AnalysisRunIndex,
  {
    readonly replaceProject: (
      projectId: string,
      runs: ReadonlyArray<AnalysisRunSummaryType>,
      mutation: AnalysisIndexMutation,
    ) => Effect.Effect<boolean, AnalysisRunIndexError>;
    readonly beginMutation: (
      projectId: string,
    ) => Effect.Effect<AnalysisIndexMutation, AnalysisRunIndexError>;
    readonly needsRebuild: (projectId: string) => Effect.Effect<boolean, AnalysisRunIndexError>;
    readonly listNonTerminal: (
      projectId: string,
    ) => Effect.Effect<ReadonlyArray<AnalysisRunSummaryType>, AnalysisRunIndexError>;
    readonly listRetainedTerminal: (
      projectId: string,
    ) => Effect.Effect<ReadonlyArray<AnalysisRunSummaryType>, AnalysisRunIndexError>;
    readonly storageSummary: (input: {
      readonly projectId: string;
      readonly relativePath?: string | undefined;
    }) => Effect.Effect<AnalysisStorageSummary, AnalysisRunIndexError>;
    readonly upsert: (
      run: AnalysisRunSummaryType,
      mutation: AnalysisIndexMutation,
    ) => Effect.Effect<void, AnalysisRunIndexError>;
    readonly list: (input: {
      readonly projectId: string;
      readonly relativePath?: string | undefined;
      readonly limit: number;
      readonly cursor?: string | undefined;
    }) => Effect.Effect<AnalysisListRunsResult, AnalysisRunIndexError>;
  }
>()("t3/scient/analysis/AnalysisRunIndex") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // The index may be wired to a separately memoized SQLite layer in focused
  // route tests. Enforce the same fail-closed Scient migration ledger on the
  // exact connection used by this service; production startup normally makes
  // this an idempotent no-op.
  yield* runScientMigrations(sql);
  // Seventeen bound columns per row. Keep rebuilds below SQLite's conservative
  // 999-parameter builds while still avoiding one statement per receipt.
  const rebuildBatchSize = 50;

  const mapError = (operation: string) =>
    Effect.mapError((cause: unknown) => new AnalysisRunIndexError({ operation, cause }));

  const indexedFields = (run: AnalysisRunSummaryType, summaryJson: string) => ({
    project_id: run.projectId,
    run_id: run.receipt.runId,
    relative_path: run.source.relativePath,
    started_at: run.receipt.startedAt,
    status: run.receipt.status,
    summary_json: summaryJson,
    source_revision: run.source.revision,
    runtime_id: run.runtime.id,
    runtime_release: run.runtime.verification?.release ?? run.runtime.version,
    action: run.action,
    finished_at: run.receipt.finishedAt,
    artifact_count: run.artifacts.length,
    diagnostic_count: run.diagnostics.length,
    retained_output_bytes: run.localStorage.outputBytes,
    retained_artifact_bytes: run.localStorage.artifactBytes,
    retained_bytes: run.localStorage.totalBytes,
    storage_status: run.localStorage.status,
  });

  const beginMutation = (projectId: string) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const previous = yield* sql<ProjectionStateRow>`
            SELECT revision, clean FROM scient_analysis_run_index_state
            WHERE project_id = ${projectId}
          `;
          const wasClean = previous[0]?.clean === 1;
          yield* sql`
            INSERT INTO scient_analysis_run_index_state (project_id, revision, clean, indexed_at)
            VALUES (${projectId}, 1, 0, NULL)
            ON CONFLICT(project_id) DO UPDATE SET
              revision = scient_analysis_run_index_state.revision + 1,
              clean = 0,
              indexed_at = NULL
          `;
          const rows = yield* sql<ProjectionStateRow>`
            SELECT revision, clean FROM scient_analysis_run_index_state
            WHERE project_id = ${projectId}
          `;
          const revision = rows[0]?.revision;
          if (revision === undefined)
            throw new Error("Unable to read analysis projection revision.");
          return { revision, wasClean } satisfies AnalysisIndexMutation;
        }),
      )
      .pipe(mapError("begin-mutation"));

  const upsertRow = (run: AnalysisRunSummaryType, mutation: AnalysisIndexMutation) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const summaryJson = yield* encodeSummary(run);
          const fields = indexedFields(run, summaryJson);
          yield* sql`
        INSERT INTO scient_analysis_run_index (
          project_id, run_id, relative_path, started_at, status, summary_json,
          source_revision, runtime_id, runtime_release, action, finished_at,
          artifact_count, diagnostic_count, retained_output_bytes,
          retained_artifact_bytes, retained_bytes, storage_status
        ) VALUES (
          ${fields.project_id}, ${fields.run_id}, ${fields.relative_path},
          ${fields.started_at}, ${fields.status}, ${fields.summary_json},
          ${fields.source_revision}, ${fields.runtime_id}, ${fields.runtime_release},
          ${fields.action}, ${fields.finished_at}, ${fields.artifact_count},
          ${fields.diagnostic_count}, ${fields.retained_output_bytes},
          ${fields.retained_artifact_bytes}, ${fields.retained_bytes}, ${fields.storage_status}
        )
        ON CONFLICT(project_id, run_id) DO UPDATE SET
          relative_path = excluded.relative_path,
          started_at = excluded.started_at,
          status = excluded.status,
          summary_json = excluded.summary_json,
          source_revision = excluded.source_revision,
          runtime_id = excluded.runtime_id,
          runtime_release = excluded.runtime_release,
          action = excluded.action,
          finished_at = excluded.finished_at,
          artifact_count = excluded.artifact_count,
          diagnostic_count = excluded.diagnostic_count,
          retained_output_bytes = excluded.retained_output_bytes,
          retained_artifact_bytes = excluded.retained_artifact_bytes,
          retained_bytes = excluded.retained_bytes,
          storage_status = excluded.storage_status
      `;
          if (mutation.wasClean) {
            yield* sql`
            UPDATE scient_analysis_run_index_state
            SET clean = 1, indexed_at = current_timestamp
            WHERE project_id = ${run.projectId} AND revision = ${mutation.revision}
          `;
          } else {
            yield* sql`
            UPDATE scient_analysis_run_index_state
            SET clean = 0, indexed_at = NULL
            WHERE project_id = ${run.projectId} AND revision = ${mutation.revision}
          `;
          }
        }),
      )
      .pipe(mapError("upsert"));

  const replaceProject = (
    projectId: string,
    runs: ReadonlyArray<AnalysisRunSummaryType>,
    mutation: AnalysisIndexMutation,
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const state = yield* sql<ProjectionStateRow>`
            SELECT revision, clean FROM scient_analysis_run_index_state
            WHERE project_id = ${projectId}
          `;
          if (state[0]?.revision !== mutation.revision) return false;
          const rows = yield* Effect.forEach(
            runs,
            (run) =>
              encodeSummary(run).pipe(Effect.map((summaryJson) => indexedFields(run, summaryJson))),
            { concurrency: 16 },
          );
          yield* sql`DELETE FROM scient_analysis_run_index WHERE project_id = ${projectId}`;
          for (let offset = 0; offset < rows.length; offset += rebuildBatchSize) {
            yield* sql`
            INSERT INTO scient_analysis_run_index
            ${sql.insert(rows.slice(offset, offset + rebuildBatchSize))}
          `;
          }
          yield* sql`
            UPDATE scient_analysis_run_index_state
            SET clean = 1, indexed_at = current_timestamp
            WHERE project_id = ${projectId} AND revision = ${mutation.revision}
          `;
          return true;
        }),
      )
      .pipe(mapError("replace-project"));

  const needsRebuild = (projectId: string) =>
    Effect.gen(function* () {
      const rows = yield* sql<ProjectionStateRow>`
        SELECT revision, clean FROM scient_analysis_run_index_state
        WHERE project_id = ${projectId}
      `;
      return rows.length === 0 || rows[0]!.clean !== 1;
    }).pipe(mapError("needs-rebuild"));

  const listNonTerminal = (projectId: string) =>
    Effect.gen(function* () {
      const rows = yield* sql<IndexedRunRow>`
        SELECT summary_json FROM scient_analysis_run_index
        WHERE project_id = ${projectId}
          AND status NOT IN ('succeeded', 'failed', 'cancelled', 'lost')
        ORDER BY started_at ASC, run_id ASC
      `;
      return yield* Effect.forEach(rows, (row) => decodeSummary(row.summary_json), {
        concurrency: 16,
      });
    }).pipe(mapError("list-non-terminal"));

  const listRetainedTerminal = (projectId: string) =>
    Effect.gen(function* () {
      const rows = yield* sql<IndexedRunRow>`
        SELECT summary_json FROM scient_analysis_run_index
        WHERE project_id = ${projectId}
          AND storage_status = 'retained'
          AND status IN ('succeeded', 'failed', 'cancelled', 'lost')
        ORDER BY started_at ASC, run_id ASC
      `;
      return yield* Effect.forEach(rows, (row) => decodeSummary(row.summary_json), {
        concurrency: 16,
      });
    }).pipe(mapError("list-retained-terminal"));

  const storageSummary = (input: {
    readonly projectId: string;
    readonly relativePath?: string | undefined;
  }) =>
    Effect.gen(function* () {
      const rows =
        input.relativePath === undefined
          ? yield* sql<StorageSummaryRow>`
              SELECT
                SUM(CASE WHEN storage_status = 'retained' THEN 1 ELSE 0 END) AS retained_run_count,
                SUM(CASE WHEN storage_status = 'metadata-only' THEN 1 ELSE 0 END) AS metadata_only_run_count,
                COALESCE(SUM(retained_output_bytes), 0) AS output_bytes,
                COALESCE(SUM(retained_artifact_bytes), 0) AS artifact_bytes,
                COALESCE(SUM(retained_bytes), 0) AS total_bytes
              FROM scient_analysis_run_index WHERE project_id = ${input.projectId}
            `
          : yield* sql<StorageSummaryRow>`
              SELECT
                SUM(CASE WHEN storage_status = 'retained' THEN 1 ELSE 0 END) AS retained_run_count,
                SUM(CASE WHEN storage_status = 'metadata-only' THEN 1 ELSE 0 END) AS metadata_only_run_count,
                COALESCE(SUM(retained_output_bytes), 0) AS output_bytes,
                COALESCE(SUM(retained_artifact_bytes), 0) AS artifact_bytes,
                COALESCE(SUM(retained_bytes), 0) AS total_bytes
              FROM scient_analysis_run_index
              WHERE project_id = ${input.projectId} AND relative_path = ${input.relativePath}
            `;
      const row = rows[0];
      return {
        retainedRunCount: Number(row?.retained_run_count ?? 0),
        metadataOnlyRunCount: Number(row?.metadata_only_run_count ?? 0),
        outputBytes: Number(row?.output_bytes ?? 0),
        artifactBytes: Number(row?.artifact_bytes ?? 0),
        totalBytes: Number(row?.total_bytes ?? 0),
      } satisfies AnalysisStorageSummary;
    }).pipe(mapError("storage-summary"));

  const list = (input: {
    readonly projectId: string;
    readonly relativePath?: string | undefined;
    readonly limit: number;
    readonly cursor?: string | undefined;
  }) =>
    Effect.gen(function* () {
      const cursor = input.cursor === undefined ? null : decodeCursor(input.cursor);
      if (input.cursor !== undefined && cursor === null) {
        return yield* new AnalysisRunIndexError({
          operation: "decode-cursor",
          cause: new Error("The analysis history cursor is invalid."),
        });
      }
      const rowLimit = input.limit + 1;
      let rows: ReadonlyArray<IndexedRunRow>;
      if (input.relativePath !== undefined && cursor !== null) {
        rows = yield* sql<IndexedRunRow>`
          SELECT summary_json FROM scient_analysis_run_index
          WHERE project_id = ${input.projectId}
            AND relative_path = ${input.relativePath}
            AND (started_at < ${cursor.startedAt}
              OR (started_at = ${cursor.startedAt} AND run_id < ${cursor.runId}))
          ORDER BY started_at DESC, run_id DESC LIMIT ${rowLimit}
        `;
      } else if (input.relativePath !== undefined) {
        rows = yield* sql<IndexedRunRow>`
          SELECT summary_json FROM scient_analysis_run_index
          WHERE project_id = ${input.projectId} AND relative_path = ${input.relativePath}
          ORDER BY started_at DESC, run_id DESC LIMIT ${rowLimit}
        `;
      } else if (cursor !== null) {
        rows = yield* sql<IndexedRunRow>`
          SELECT summary_json FROM scient_analysis_run_index
          WHERE project_id = ${input.projectId}
            AND (started_at < ${cursor.startedAt}
              OR (started_at = ${cursor.startedAt} AND run_id < ${cursor.runId}))
          ORDER BY started_at DESC, run_id DESC LIMIT ${rowLimit}
        `;
      } else {
        rows = yield* sql<IndexedRunRow>`
          SELECT summary_json FROM scient_analysis_run_index
          WHERE project_id = ${input.projectId}
          ORDER BY started_at DESC, run_id DESC LIMIT ${rowLimit}
        `;
      }
      const decoded = yield* Effect.forEach(
        rows.slice(0, input.limit),
        (row) => decodeSummary(row.summary_json),
        { concurrency: 16 },
      );
      const hasMore = rows.length > input.limit;
      return {
        runs: decoded,
        hasMore,
        nextCursor: hasMore && decoded.length > 0 ? cursorFor(decoded[decoded.length - 1]!) : null,
      } satisfies AnalysisListRunsResult;
    }).pipe(
      Effect.mapError((cause) =>
        isAnalysisRunIndexError(cause)
          ? cause
          : new AnalysisRunIndexError({ operation: "list", cause }),
      ),
    );

  return AnalysisRunIndex.of({
    replaceProject,
    beginMutation,
    needsRebuild,
    listNonTerminal,
    listRetainedTerminal,
    storageSummary,
    upsert: upsertRow,
    list,
  });
});

export const layer = Layer.effect(AnalysisRunIndex, make);
