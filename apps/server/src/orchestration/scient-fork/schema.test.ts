/**
 * Comprehensive Scient migration runner tests.
 *
 * Covers VAL-MIGRATE-01 through VAL-MIGRATE-12, VAL-MIGRATE-14, and
 * VAL-MIGRATE-15, plus strict ledger-integrity preflight tests (gaps, name
 * mismatches, unknown future IDs). All fixtures are synthetic in-memory or
 * temporary file SQLite databases. No live user data is used.
 *
 * Each test gets its own fresh in-memory database via `Effect.provide` so
 * schema state never leaks between tests.
 */
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import { runMigrations } from "../../persistence/Migrations.ts";
import { runScientMigrations, SCIENT_MIGRATIONS } from "./scientMigrator.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

interface TableColumn {
  readonly name: string;
  readonly type: string;
  readonly notnull: number;
  readonly dflt_value: string | null;
}

/** Provide a fresh in-memory SQLite database to an effect. */
function withMemory<E, A>(
  effect: Effect.Effect<A, E, SqlClient.SqlClient>,
): Effect.Effect<A, E | SqlError, never> {
  return effect.pipe(Effect.provide(NodeSqliteClient.layerMemory()));
}

/** PRAGMA table_info helper (uses unsafe because PRAGMA doesn't support parameters). */
function tableInfo(sql: SqlClient.SqlClient, table: string) {
  return sql.unsafe<TableColumn>(`PRAGMA table_info(${table})`);
}

/** PRAGMA index_list helper. */
function indexList(sql: SqlClient.SqlClient, table: string) {
  return sql.unsafe<{ readonly name: string }>(`PRAGMA index_list(${table})`);
}

/** Evidence payload schema for quarantined lineage rows. */
const QuarantinePayloadEvidence = Schema.fromJsonString(
  Schema.Struct({
    thread_id: Schema.Unknown,
    forked_from_thread_id: Schema.NullOr(Schema.String),
    fork_point_turn_count: Schema.Unknown,
    workspace_mode: Schema.NullOr(Schema.String),
    status: Schema.NullOr(Schema.String),
  }),
);
const decodeQuarantinePayload = Schema.decodeSync(QuarantinePayloadEvidence);

const SCIENT_MIGRATION_IDS = [1, 2, 3, 4, 5, 6, 7];
const SCIENT_MIGRATION_NAMES = [
  "durable-thread-forks",
  "durable-provider-bootstrap",
  "normalize-active-lineage",
  "quarantine-invalid-lineage",
  "analysis-run-index",
  "analysis-run-projection-state",
  "analysis-run-storage-status",
];
const SCIENT_MIGRATIONS_AFTER_BOOTSTRAP = SCIENT_MIGRATION_IDS.slice(2);

/** Restore the schema boundary immediately before migration 4. */
const removePostMigrationThreeAnalysisSchema = (sql: SqlClient.SqlClient) =>
  Effect.gen(function* () {
    yield* sql`DROP TABLE IF EXISTS scient_analysis_run_index_state`;
    yield* sql`DROP TABLE IF EXISTS scient_analysis_run_index`;
  });

// ---------------------------------------------------------------------------
// VAL-MIGRATE-01: Fresh install creates canonical Scient schema
// ---------------------------------------------------------------------------

it.effect("fresh install creates canonical Scient schema with ledger and indexes", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const executed = yield* runScientMigrations(sql);

      // All migrations ran in order.
      assert.deepStrictEqual(
        executed.map(([id]) => id),
        SCIENT_MIGRATION_IDS,
      );

      // Ledger exists with all migrations (created_at, no applied_at).
      const ledger = yield* sql<{
        readonly migration_id: number;
        readonly name: string;
        readonly created_at: string;
      }>`SELECT migration_id, name, created_at FROM scient_schema_migrations ORDER BY migration_id`;
      assert.strictEqual(ledger.length, SCIENT_MIGRATION_IDS.length);
      assert.deepStrictEqual(
        ledger.map((row) => row.migration_id),
        SCIENT_MIGRATION_IDS,
      );
      assert.deepStrictEqual(
        ledger.map((row) => row.name),
        SCIENT_MIGRATION_NAMES,
      );
      assert.isTrue(ledger.every((row) => row.created_at.length > 0));

      // scient_thread_lineage has required columns.
      const columns = yield* tableInfo(sql, "scient_thread_lineage");
      const columnNames = new Set(columns.map((column) => column.name));
      for (const required of [
        "thread_id",
        "forked_from_thread_id",
        "fork_point_turn_id",
        "fork_point_turn_count",
        "source_checkpoint_turn_count",
        "baseline_turn_id",
        "baseline_user_message_id",
        "baseline_assistant_message_id",
        "workspace_mode",
        "provider_mode",
        "provider_bootstrap_status",
        "attachment_copies_json",
        "fidelity_mode",
        "status",
        "checkpoint_status",
        "workspace_status",
        "attempt_count",
        "last_error",
        "created_at",
        "updated_at",
      ]) {
        assert.isTrue(columnNames.has(required), `Missing column: ${required}`);
      }

      // Indexes exist.
      const indexes = yield* indexList(sql, "scient_thread_lineage");
      const indexNames = new Set(indexes.map((index) => index.name));
      assert.isTrue(indexNames.has("idx_scient_thread_lineage_forked_from"));
      assert.isTrue(indexNames.has("idx_scient_thread_lineage_status"));

      // Quarantine table exists for malformed-row evidence.
      const quarantineColumns = yield* tableInfo(sql, "scient_thread_lineage_quarantine");
      const quarantineColumnNames = new Set(quarantineColumns.map((column) => column.name));
      for (const required of [
        "quarantine_id",
        "source_rowid",
        "thread_id",
        "reason",
        "payload_json",
        "quarantined_at",
      ]) {
        assert.isTrue(
          quarantineColumnNames.has(required),
          `Missing quarantine column: ${required}`,
        );
      }

      const analysisColumns = yield* tableInfo(sql, "scient_analysis_run_index");
      const analysisColumnNames = new Set(analysisColumns.map((column) => column.name));
      for (const required of [
        "project_id",
        "run_id",
        "relative_path",
        "source_revision",
        "runtime_id",
        "runtime_release",
        "action",
        "status",
        "finished_at",
        "artifact_count",
        "diagnostic_count",
        "retained_output_bytes",
        "retained_artifact_bytes",
        "retained_bytes",
        "storage_status",
        "summary_json",
      ]) {
        assert.isTrue(
          analysisColumnNames.has(required),
          `Missing analysis index column: ${required}`,
        );
      }
      const analysisIndexes = new Set(
        (yield* indexList(sql, "scient_analysis_run_index")).map((index) => index.name),
      );
      for (const required of [
        "scient_analysis_run_index_project_started",
        "scient_analysis_run_index_file_started",
        "scient_analysis_run_index_storage",
      ]) {
        assert.isTrue(analysisIndexes.has(required), `Missing analysis index: ${required}`);
      }
      assert.deepStrictEqual(
        (yield* tableInfo(sql, "scient_analysis_run_index_state")).map((column) => column.name),
        ["project_id", "revision", "clean", "indexed_at"],
      );
    }),
  ),
);

// ---------------------------------------------------------------------------
// VAL-MIGRATE-02: Pre-bootstrap prototype rows fail closed without fabrication
// ---------------------------------------------------------------------------

it.effect("pre-bootstrap prototype lineage is quarantined without fabricating a baseline", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      // Create the prototype table shape (Claude's original schema).
      yield* sql`
        CREATE TABLE scient_thread_lineage (
          thread_id TEXT PRIMARY KEY,
          forked_from_thread_id TEXT,
          fork_point_turn_count INTEGER,
          workspace_mode TEXT,
          fidelity_mode TEXT,
          created_at TEXT
        )
      `;
      yield* sql`
        INSERT INTO scient_thread_lineage (
          thread_id, forked_from_thread_id, fork_point_turn_count,
          workspace_mode, fidelity_mode, created_at
        ) VALUES (
          'claude-fork', 'origin-thread', 3,
          'local', 'chat-only', '2026-08-08T00:00:00.000Z'
        )
      `;

      yield* runScientMigrations(sql);

      const active = yield* sql`SELECT thread_id FROM scient_thread_lineage`;
      assert.strictEqual(active.length, 0);
      const quarantined = yield* sql<{
        readonly thread_id: string;
        readonly reason: string;
        readonly payload_json: string;
      }>`SELECT thread_id, reason, payload_json FROM scient_thread_lineage_quarantine`;
      assert.strictEqual(quarantined.length, 1);
      assert.strictEqual(quarantined[0]!.thread_id, "claude-fork");
      assert.include(quarantined[0]!.reason, "baseline_turn_id is missing or blank");
      const payload = decodeQuarantinePayload(quarantined[0]!.payload_json);
      assert.strictEqual(payload.forked_from_thread_id, "origin-thread");
      assert.strictEqual(payload.fork_point_turn_count, 3);
    }),
  ),
);

// ---------------------------------------------------------------------------
// VAL-MIGRATE-03: Current schema upgrade preserves valid lifecycle state
// ---------------------------------------------------------------------------

it.effect("current schema upgrade preserves valid lifecycle state", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      // Run migrations first to get the full schema, then reset data.
      yield* runScientMigrations(sql);
      yield* sql`DELETE FROM scient_thread_lineage`;

      yield* sql`
        INSERT INTO scient_thread_lineage (
          thread_id, forked_from_thread_id, fork_point_turn_id, fork_point_turn_count,
          source_checkpoint_turn_count, baseline_turn_id, baseline_user_message_id,
          baseline_assistant_message_id, workspace_mode, provider_mode,
          provider_bootstrap_status, attachment_copies_json, fidelity_mode,
          status, checkpoint_status, workspace_status, attempt_count, last_error,
          created_at, updated_at
        ) VALUES (
          'fork-pending', 'origin-1', 'turn-1', 2,
          2, 'baseline-1', 'user-1', 'asst-1',
          'local', 'transcript-bootstrap',
          'pending', '[]', 'transcript-bootstrap',
          'pending', 'pending', 'pending', 0, NULL,
          '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO scient_thread_lineage (
          thread_id, forked_from_thread_id, fork_point_turn_id, fork_point_turn_count,
          source_checkpoint_turn_count, baseline_turn_id, baseline_user_message_id,
          baseline_assistant_message_id, workspace_mode, provider_mode,
          provider_bootstrap_status, attachment_copies_json, fidelity_mode,
          status, checkpoint_status, workspace_status, attempt_count, last_error,
          created_at, updated_at
        ) VALUES (
          'fork-ready', 'origin-2', 'turn-2', 1,
          NULL, 'baseline-2', NULL, 'asst-2',
          'new-worktree', 'transcript-bootstrap',
          'completed', '[]', 'transcript-bootstrap',
          'ready', 'ready', 'worktree', 1, NULL,
          '2026-02-01T00:00:00.000Z', '2026-02-01T12:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO scient_thread_lineage (
          thread_id, forked_from_thread_id, fork_point_turn_id, fork_point_turn_count,
          source_checkpoint_turn_count, baseline_turn_id, baseline_user_message_id,
          baseline_assistant_message_id, workspace_mode, provider_mode,
          provider_bootstrap_status, attachment_copies_json, fidelity_mode,
          status, checkpoint_status, workspace_status, attempt_count, last_error,
          created_at, updated_at
        ) VALUES (
          'fork-failed', 'origin-3', 'turn-3', 4,
          4, 'baseline-3', 'user-3', 'asst-3',
          'local', 'transcript-bootstrap',
          'pending', '[]', 'transcript-bootstrap',
          'failed', 'unavailable', 'shared', 2, 'checkpoint missing',
          '2026-03-01T00:00:00.000Z', '2026-03-01T06:00:00.000Z'
        )
      `;

      const beforeRows = yield* sql<{
        readonly thread_id: string;
        readonly status: string;
        readonly provider_mode: string;
        readonly fidelity_mode: string;
        readonly last_error: string | null;
        readonly attempt_count: number;
        readonly created_at: string;
        readonly updated_at: string;
      }>`
        SELECT thread_id, status, provider_mode, fidelity_mode, last_error,
               attempt_count, created_at, updated_at
        FROM scient_thread_lineage ORDER BY thread_id
      `;

      // Re-run migrations (idempotent — nothing pending).
      const executed = yield* runScientMigrations(sql);
      assert.strictEqual(executed.length, 0);

      const afterRows = yield* sql<{
        readonly thread_id: string;
        readonly status: string;
        readonly provider_mode: string;
        readonly fidelity_mode: string;
        readonly last_error: string | null;
        readonly attempt_count: number;
        readonly created_at: string;
        readonly updated_at: string;
      }>`
        SELECT thread_id, status, provider_mode, fidelity_mode, last_error,
               attempt_count, created_at, updated_at
        FROM scient_thread_lineage ORDER BY thread_id
      `;
      assert.deepStrictEqual(afterRows, beforeRows);
    }),
  ),
);

// ---------------------------------------------------------------------------
// VAL-MIGRATE-04: Legacy ledger history is immutable
// ---------------------------------------------------------------------------

it.effect("legacy ledger IDs 1/2 with names and timestamps remain intact", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      // Create a ledger with the old applied_at column and legacy entries.
      yield* sql`
        CREATE TABLE scient_schema_migrations (
          migration_id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          applied_at TEXT NOT NULL
        )
      `;
      yield* sql`
        INSERT INTO scient_schema_migrations (migration_id, name, applied_at)
        VALUES (1, 'durable-thread-forks', '2026-07-01T10:00:00.000Z')
      `;
      yield* sql`
        INSERT INTO scient_schema_migrations (migration_id, name, applied_at)
        VALUES (2, 'durable-provider-bootstrap', '2026-07-02T12:00:00.000Z')
      `;

      // Also create the lineage table so migration 3 doesn't fail on missing table.
      yield* sql`
        CREATE TABLE scient_thread_lineage (
          thread_id TEXT PRIMARY KEY,
          forked_from_thread_id TEXT,
          fork_point_turn_count INTEGER,
          workspace_mode TEXT,
          fidelity_mode TEXT,
          created_at TEXT
        )
      `;

      const executed = yield* runScientMigrations(sql);

      // Migrations 3 through 7 ran (1 and 2 were already in the ledger).
      assert.deepStrictEqual(
        executed.map(([id]) => id),
        SCIENT_MIGRATIONS_AFTER_BOOTSTRAP,
      );

      // Legacy entries preserved with both applied_at and created_at.
      const ledger = yield* sql<{
        readonly migration_id: number;
        readonly name: string;
        readonly created_at: string;
        readonly applied_at: string;
      }>`SELECT migration_id, name, created_at, applied_at FROM scient_schema_migrations ORDER BY migration_id`;
      assert.strictEqual(ledger.length, SCIENT_MIGRATION_IDS.length);

      assert.strictEqual(ledger[0]!.migration_id, 1);
      assert.strictEqual(ledger[0]!.name, "durable-thread-forks");
      assert.strictEqual(ledger[0]!.applied_at, "2026-07-01T10:00:00.000Z");
      assert.strictEqual(ledger[0]!.created_at, "2026-07-01T10:00:00.000Z");

      assert.strictEqual(ledger[1]!.migration_id, 2);
      assert.strictEqual(ledger[1]!.name, "durable-provider-bootstrap");
      assert.strictEqual(ledger[1]!.applied_at, "2026-07-02T12:00:00.000Z");
      assert.strictEqual(ledger[1]!.created_at, "2026-07-02T12:00:00.000Z");

      assert.strictEqual(ledger[2]!.migration_id, 3);
      assert.strictEqual(ledger[2]!.name, "normalize-active-lineage");
      assert.isTrue(ledger[2]!.created_at.length > 0);
      assert.strictEqual(ledger[3]!.migration_id, 4);
      assert.strictEqual(ledger[3]!.name, "quarantine-invalid-lineage");
      assert.strictEqual(ledger[4]!.migration_id, 5);
      assert.strictEqual(ledger[4]!.name, "analysis-run-index");
      assert.strictEqual(ledger[5]!.migration_id, 6);
      assert.strictEqual(ledger[5]!.name, "analysis-run-projection-state");
      assert.strictEqual(ledger[6]!.migration_id, 7);
      assert.strictEqual(ledger[6]!.name, "analysis-run-storage-status");
    }),
  ),
);

// ---------------------------------------------------------------------------
// VAL-MIGRATE-05: Only unapplied migrations run in order
// ---------------------------------------------------------------------------

it.effect("only unapplied migrations run in ascending order", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      // Pre-seed ledger with migration 1 only.
      yield* sql`
        CREATE TABLE scient_schema_migrations (
          migration_id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
      `;
      yield* sql`
        INSERT INTO scient_schema_migrations (migration_id, name)
        VALUES (1, 'durable-thread-forks')
      `;
      yield* sql`
        CREATE TABLE scient_thread_lineage (
          thread_id TEXT PRIMARY KEY,
          forked_from_thread_id TEXT,
          fork_point_turn_count INTEGER,
          workspace_mode TEXT,
          fidelity_mode TEXT,
          created_at TEXT
        )
      `;

      const executed = yield* runScientMigrations(sql);

      assert.deepStrictEqual(
        executed.map(([id, name]) => [id, name]),
        [
          [2, "durable-provider-bootstrap"],
          [3, "normalize-active-lineage"],
          [4, "quarantine-invalid-lineage"],
          [5, "analysis-run-index"],
          [6, "analysis-run-projection-state"],
          [7, "analysis-run-storage-status"],
        ] as const,
      );

      const ledger = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM scient_schema_migrations ORDER BY migration_id
      `;
      assert.deepStrictEqual(
        ledger.map((row) => row.migration_id),
        SCIENT_MIGRATION_IDS,
      );
    }),
  ),
);

// ---------------------------------------------------------------------------
// VAL-MIGRATE-06: Malformed rows are quarantined, not fatal
// ---------------------------------------------------------------------------

/** Helper: create a full schema with a malformed row, pre-seed ledger with 1+2. */
function setupMalformedDatabase(
  sql: SqlClient.SqlClient,
  threadId: string,
  workspaceMode: string | null,
  status: string,
  forkedFrom: string | null,
) {
  return Effect.gen(function* () {
    yield* sql`
      CREATE TABLE scient_schema_migrations (
        migration_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      )
    `;
    yield* sql`INSERT INTO scient_schema_migrations (migration_id, name) VALUES (1, 'durable-thread-forks')`;
    yield* sql`INSERT INTO scient_schema_migrations (migration_id, name) VALUES (2, 'durable-provider-bootstrap')`;

    yield* sql`
      CREATE TABLE scient_thread_lineage (
        thread_id TEXT PRIMARY KEY,
        forked_from_thread_id TEXT,
        fork_point_turn_count INTEGER,
        workspace_mode TEXT,
        fidelity_mode TEXT,
        created_at TEXT,
        provider_mode TEXT NOT NULL DEFAULT 'transcript-bootstrap',
        provider_bootstrap_status TEXT NOT NULL DEFAULT 'pending',
        attachment_copies_json TEXT NOT NULL DEFAULT '[]',
        baseline_turn_id TEXT,
        baseline_user_message_id TEXT,
        baseline_assistant_message_id TEXT,
        fork_point_turn_id TEXT,
        source_checkpoint_turn_count INTEGER,
        status TEXT NOT NULL DEFAULT 'pending',
        checkpoint_status TEXT NOT NULL DEFAULT 'pending',
        workspace_status TEXT NOT NULL DEFAULT 'pending',
        attempt_count INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        updated_at TEXT
      )
    `;
    yield* sql`
      INSERT INTO scient_thread_lineage (
        thread_id, forked_from_thread_id, fork_point_turn_count,
        workspace_mode, fidelity_mode, baseline_turn_id, created_at, status
      ) VALUES (
        ${threadId}, ${forkedFrom}, 1,
        ${workspaceMode}, 'chat-only', 'baseline-test', '2026-01-01T00:00:00.000Z', ${status}
      )
    `;
  });
}

it.effect("malformed rows are quarantined with evidence while valid rows normalize", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* setupMalformedDatabase(sql, "bad-fork", "invalid-mode", "pending", "origin");
      // A valid sibling row must survive and normalize.
      yield* sql`
        INSERT INTO scient_thread_lineage (
          thread_id, forked_from_thread_id, fork_point_turn_count,
          workspace_mode, fidelity_mode, baseline_turn_id, created_at, status
        ) VALUES (
          'good-fork', 'origin', 2, 'local', 'chat-only', 'baseline-good', '2026-01-02T00:00:00.000Z', 'pending'
        )
      `;

      // Migration 3 succeeds despite the malformed row.
      const executed = yield* runScientMigrations(sql);
      assert.deepStrictEqual(
        executed.map(([id]) => id),
        SCIENT_MIGRATIONS_AFTER_BOOTSTRAP,
      );

      // Migration 3 is recorded.
      const ledger = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM scient_schema_migrations ORDER BY migration_id
      `;
      assert.deepStrictEqual(
        ledger.map((row) => row.migration_id),
        SCIENT_MIGRATION_IDS,
      );

      // The malformed row was quarantined out of active recovery; the valid
      // sibling remains.
      const lineage = yield* sql<{ readonly thread_id: string }>`
        SELECT thread_id FROM scient_thread_lineage
      `;
      assert.deepStrictEqual(
        lineage.map((row) => row.thread_id),
        ["good-fork"],
      );

      // Quarantine preserves the pre-normalization payload with a reason and
      // a timestamp.
      const quarantined = yield* sql<{
        readonly thread_id: string;
        readonly reason: string;
        readonly payload_json: string;
        readonly quarantined_at: string;
      }>`
        SELECT thread_id, reason, payload_json, quarantined_at
        FROM scient_thread_lineage_quarantine
      `;
      assert.strictEqual(quarantined.length, 1);
      assert.strictEqual(quarantined[0]!.thread_id, "bad-fork");
      assert.isTrue(
        quarantined[0]!.reason.includes("workspace_mode 'invalid-mode' is not valid"),
        `Unexpected quarantine reason: ${quarantined[0]!.reason}`,
      );
      assert.isTrue(quarantined[0]!.quarantined_at.length > 0);
      const payload = decodeQuarantinePayload(quarantined[0]!.payload_json);
      assert.strictEqual(payload.thread_id, "bad-fork");
      assert.strictEqual(payload.workspace_mode, "invalid-mode");

      // The valid sibling normalized to canonical modes.
      const good = yield* sql<{
        readonly fidelity_mode: string;
        readonly provider_mode: string;
      }>`SELECT fidelity_mode, provider_mode FROM scient_thread_lineage WHERE thread_id = 'good-fork'`;
      assert.strictEqual(good[0]!.fidelity_mode, "transcript-bootstrap");
      assert.strictEqual(good[0]!.provider_mode, "transcript-bootstrap");
    }),
  ),
);

// ---------------------------------------------------------------------------
// VAL-MIGRATE-07: Restart is idempotent and retryable
// ---------------------------------------------------------------------------

it.effect("second clean run returns empty with no schema changes", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      const firstRun = yield* runScientMigrations(sql);
      assert.strictEqual(firstRun.length, SCIENT_MIGRATION_IDS.length);

      const beforeColumns = yield* tableInfo(sql, "scient_thread_lineage");
      const beforeLedger = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM scient_schema_migrations ORDER BY migration_id
      `;

      const secondRun = yield* runScientMigrations(sql);
      assert.strictEqual(secondRun.length, 0);

      const afterColumns = yield* tableInfo(sql, "scient_thread_lineage");
      const afterLedger = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM scient_schema_migrations ORDER BY migration_id
      `;
      assert.deepStrictEqual(
        afterColumns.map((column) => column.name),
        beforeColumns.map((column) => column.name),
      );
      assert.deepStrictEqual(beforeLedger, afterLedger);
    }),
  ),
);

it.effect("quarantine is durable across idempotent reruns", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* setupMalformedDatabase(sql, "fixable-fork", "invalid-mode", "pending", "origin");

      // First run quarantines the malformed row and records migration 3.
      const first = yield* runScientMigrations(sql);
      assert.deepStrictEqual(
        first.map(([id]) => id),
        SCIENT_MIGRATIONS_AFTER_BOOTSTRAP,
      );

      const quarantinedBefore = yield* sql<{
        readonly thread_id: string;
        readonly reason: string;
      }>`SELECT thread_id, reason FROM scient_thread_lineage_quarantine`;
      assert.strictEqual(quarantinedBefore.length, 1);
      assert.strictEqual(quarantinedBefore[0]!.thread_id, "fixable-fork");

      // Second run: nothing pending, quarantine untouched, lineage stays empty.
      const second = yield* runScientMigrations(sql);
      assert.strictEqual(second.length, 0);

      const quarantinedAfter = yield* sql<{ readonly thread_id: string }>`
        SELECT thread_id FROM scient_thread_lineage_quarantine
      `;
      assert.strictEqual(quarantinedAfter.length, 1);

      const lineage = yield* sql<{ readonly thread_id: string }>`
        SELECT thread_id FROM scient_thread_lineage
      `;
      assert.strictEqual(lineage.length, 0);
    }),
  ),
);

it.effect("migration 4 repairs databases that already recorded migration 3", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runScientMigrations(sql);

      // Reproduce the released development state: migration 3 is recorded,
      // but its later quarantine implementation never ran for this database.
      yield* sql`DELETE FROM scient_schema_migrations WHERE migration_id >= 4`;
      yield* removePostMigrationThreeAnalysisSchema(sql);
      yield* sql`DROP TABLE scient_thread_lineage_quarantine`;
      yield* sql`
        INSERT INTO scient_thread_lineage (
          thread_id, forked_from_thread_id, fork_point_turn_id, fork_point_turn_count,
          source_checkpoint_turn_count, baseline_turn_id, baseline_user_message_id,
          baseline_assistant_message_id, workspace_mode, provider_mode,
          provider_bootstrap_status, attachment_copies_json, fidelity_mode,
          status, checkpoint_status, workspace_status, attempt_count, last_error,
          created_at, updated_at
        ) VALUES (
          'existing-valid-fork', 'origin', 'turn-1', 1,
          NULL, 'baseline-1', 'user-1', 'assistant-1',
          'local', 'transcript-bootstrap', 'completed', '[]', 'transcript-bootstrap',
          'ready', 'unavailable', 'project-root', 1, NULL,
          '2026-08-08T00:00:00.000Z', '2026-08-08T00:00:01.000Z'
        )
      `;

      const executed = yield* runScientMigrations(sql);
      assert.deepStrictEqual(
        executed.map(([id]) => id),
        [4, 5, 6, 7],
      );

      const active = yield* sql<{ readonly thread_id: string }>`
        SELECT thread_id FROM scient_thread_lineage
      `;
      assert.deepStrictEqual(
        active.map((row) => row.thread_id),
        ["existing-valid-fork"],
      );
      const quarantineColumns = yield* tableInfo(sql, "scient_thread_lineage_quarantine");
      assert.deepStrictEqual(
        quarantineColumns.map((column) => column.name),
        ["quarantine_id", "source_rowid", "thread_id", "reason", "payload_json", "quarantined_at"],
      );
    }),
  ),
);

it.effect("migration 4 upgrades the legacy quarantine without losing evidence", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runScientMigrations(sql);
      yield* sql`DELETE FROM scient_schema_migrations WHERE migration_id >= 4`;
      yield* removePostMigrationThreeAnalysisSchema(sql);
      yield* sql`DROP TABLE scient_thread_lineage_quarantine`;
      yield* sql`
        CREATE TABLE scient_thread_lineage_quarantine (
          thread_id TEXT PRIMARY KEY,
          reason TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          quarantined_at TEXT NOT NULL
        )
      `;
      yield* sql`
        INSERT INTO scient_thread_lineage_quarantine (
          thread_id, reason, payload_json, quarantined_at
        ) VALUES (
          'legacy-bad-fork', 'legacy reason', '{"thread_id":"legacy-bad-fork"}',
          '2026-08-08T01:00:00.000Z'
        )
      `;

      yield* runScientMigrations(sql);

      const rows = yield* sql<{
        readonly quarantine_id: number;
        readonly source_rowid: number | null;
        readonly thread_id: string;
        readonly reason: string;
        readonly payload_json: string;
        readonly quarantined_at: string;
      }>`SELECT * FROM scient_thread_lineage_quarantine`;
      assert.deepStrictEqual(rows, [
        {
          quarantine_id: 1,
          source_rowid: null,
          thread_id: "legacy-bad-fork",
          reason: "legacy reason",
          payload_json: '{"thread_id":"legacy-bad-fork"}',
          quarantined_at: "2026-08-08T01:00:00.000Z",
        },
      ]);
    }),
  ),
);

it.effect("migration 4 quarantines decoder-invalid rows by rowid", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runScientMigrations(sql);
      yield* sql`DELETE FROM scient_schema_migrations WHERE migration_id >= 4`;
      yield* removePostMigrationThreeAnalysisSchema(sql);

      yield* sql`
        INSERT INTO scient_thread_lineage (
          thread_id, forked_from_thread_id, fork_point_turn_count, baseline_turn_id,
          workspace_mode, provider_mode, provider_bootstrap_status,
          attachment_copies_json, fidelity_mode, status, checkpoint_status,
          workspace_status, attempt_count, created_at, updated_at
        ) VALUES
          ('valid-sibling', 'origin', 1, 'baseline-valid', 'local',
           'transcript-bootstrap', 'completed', '[]', 'transcript-bootstrap',
           'ready', 'unavailable', 'project-root', 0,
           '2026-08-08T00:00:00.000Z', '2026-08-08T00:00:00.000Z'),
          (NULL, 'origin', 1, 'baseline-null-id', 'local',
           'transcript-bootstrap', 'completed', '[]', 'transcript-bootstrap',
           'ready', 'unavailable', 'project-root', 0,
           '2026-08-08T00:00:00.000Z', '2026-08-08T00:00:00.000Z'),
          ('', 'origin', 1, 'baseline-blank-id', 'local',
           'transcript-bootstrap', 'completed', '[]', 'transcript-bootstrap',
           'ready', 'unavailable', 'project-root', 0,
           '2026-08-08T00:00:00.000Z', '2026-08-08T00:00:00.000Z'),
          ('missing-baseline', 'origin', 1, NULL, 'local',
           'transcript-bootstrap', 'completed', '[]', 'transcript-bootstrap',
           'ready', 'unavailable', 'project-root', 0,
           '2026-08-08T00:00:00.000Z', '2026-08-08T00:00:00.000Z'),
          ('negative-count', 'origin', -1, 'baseline-negative', 'local',
           'transcript-bootstrap', 'completed', '[]', 'transcript-bootstrap',
           'ready', 'unavailable', 'project-root', 0,
           '2026-08-08T00:00:00.000Z', '2026-08-08T00:00:00.000Z'),
          ('bad-attachments', 'origin', 1, 'baseline-attachments', 'local',
           'transcript-bootstrap', 'completed', '{}', 'transcript-bootstrap',
           'ready', 'unavailable', 'project-root', 0,
           '2026-08-08T00:00:00.000Z', '2026-08-08T00:00:00.000Z')
      `;

      yield* runScientMigrations(sql);

      const active = yield* sql<{ readonly thread_id: string }>`
        SELECT thread_id FROM scient_thread_lineage ORDER BY thread_id
      `;
      assert.deepStrictEqual(
        active.map((row) => row.thread_id),
        ["valid-sibling"],
      );

      const quarantined = yield* sql<{
        readonly source_rowid: number;
        readonly thread_id: string | null;
        readonly reason: string;
        readonly payload_json: string;
      }>`
        SELECT source_rowid, thread_id, reason, payload_json
        FROM scient_thread_lineage_quarantine ORDER BY quarantine_id
      `;
      assert.strictEqual(quarantined.length, 5);
      assert.strictEqual(new Set(quarantined.map((row) => row.source_rowid)).size, 5);
      assert.isTrue(quarantined.some((row) => row.thread_id === null));
      assert.isTrue(
        quarantined.some(
          (row) => row.thread_id === "" && row.reason.includes("thread_id is missing or blank"),
        ),
      );
      assert.isTrue(
        quarantined.some(
          (row) =>
            row.thread_id === "missing-baseline" &&
            row.reason.includes("baseline_turn_id is missing or blank"),
        ),
      );
      assert.isTrue(
        quarantined.some(
          (row) =>
            row.thread_id === "negative-count" &&
            row.reason.includes("fork_point_turn_count is not a non-negative integer"),
        ),
      );
      assert.isTrue(
        quarantined.some(
          (row) =>
            row.thread_id === "bad-attachments" &&
            row.reason.includes("attachment_copies_json is not a valid attachment-copy array"),
        ),
      );
      const nullIdEvidence = quarantined.find((row) => row.thread_id === null);
      assert.strictEqual(decodeQuarantinePayload(nullIdEvidence!.payload_json).thread_id, null);
    }),
  ),
);

// ---------------------------------------------------------------------------
// VAL-MIGRATE-08: Concurrent startup fails closed without duplication
// ---------------------------------------------------------------------------

it.effect("concurrent runners on the same file apply each migration at most once", () => {
  const tempFile = NodePath.join(
    NodeOS.tmpdir(),
    `scient-test-concurrent-${NodeCrypto.randomUUID()}.sqlite`,
  );

  return Effect.gen(function* () {
    yield* Effect.acquireUseRelease(
      Effect.succeed(tempFile),
      (filename) =>
        Effect.gen(function* () {
          const runOnFile = Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            return yield* runScientMigrations(sql);
          }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));

          const [result1, result2] = yield* Effect.all([runOnFile, runOnFile], {
            concurrency: 2,
          });

          const totalApplied = result1.length + result2.length;
          assert.isTrue(
            totalApplied <= SCIENT_MIGRATION_IDS.length,
            `Total applied ${totalApplied} exceeds ${SCIENT_MIGRATION_IDS.length}`,
          );

          // One runner applied all, the other got empty (locked or saw committed state).
          const winner = result1.length >= result2.length ? result1 : result2;
          const loser = result1.length >= result2.length ? result2 : result1;
          assert.isTrue(winner.length > 0, "At least one runner should have applied migrations");
          assert.strictEqual(loser.length, 0);

          // Verify the final ledger with a fresh connection.
          const verify = Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            const ledger = yield* sql<{ readonly migration_id: number }>`
              SELECT migration_id FROM scient_schema_migrations ORDER BY migration_id
            `;
            assert.strictEqual(ledger.length, SCIENT_MIGRATION_IDS.length);
            assert.deepStrictEqual(
              ledger.map((row) => row.migration_id),
              SCIENT_MIGRATION_IDS,
            );

            const columns = yield* tableInfo(sql, "scient_thread_lineage");
            const columnNames = new Set(columns.map((column) => column.name));
            assert.isTrue(columnNames.has("status"));
            assert.isTrue(columnNames.has("provider_mode"));
            assert.isTrue(columnNames.has("fidelity_mode"));
          }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
          yield* verify;
        }),
      (filename) =>
        Effect.sync(() => {
          for (const suffix of ["", "-wal", "-shm"]) {
            try {
              NodeFS.unlinkSync(filename + suffix);
            } catch {
              // ignore
            }
          }
        }),
    );
  });
});

// ---------------------------------------------------------------------------
// VAL-MIGRATE-09: T3 ledger is isolated
// ---------------------------------------------------------------------------

it.effect("Scient and T3 migration ledgers remain isolated", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      // Run T3 migrations first.
      yield* runMigrations();
      // Run Scient migrations.
      yield* runScientMigrations(sql);

      const t3Ledger = yield* sql<{ readonly migration_id: number; readonly name: string }>`
        SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id
      `;
      assert.isTrue(t3Ledger.length > 0);

      const scientLedger = yield* sql<{ readonly migration_id: number; readonly name: string }>`
        SELECT migration_id, name FROM scient_schema_migrations ORDER BY migration_id
      `;
      assert.deepStrictEqual(
        scientLedger.map((row) => row.migration_id),
        SCIENT_MIGRATION_IDS,
      );

      // Neither ledger contains the other's migration names.
      const t3Names = new Set(t3Ledger.map((row) => row.name));
      const scientNames = new Set(scientLedger.map((row) => row.name));
      assert.isFalse(t3Names.has("durable-thread-forks"));
      assert.isFalse(t3Names.has("normalize-active-lineage"));
      assert.isFalse(scientNames.has("OrchestrationEvents"));
      assert.isFalse(scientNames.has("Projections"));

      // Run T3 migrations again — no new entries in either ledger.
      yield* runMigrations();
      const t3LedgerAfter = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM effect_sql_migrations ORDER BY migration_id
      `;
      assert.strictEqual(t3LedgerAfter.length, t3Ledger.length);

      const scientLedgerAfter = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM scient_schema_migrations ORDER BY migration_id
      `;
      assert.strictEqual(scientLedgerAfter.length, scientLedger.length);
    }),
  ),
);

it.effect("running Scient migrations first does not affect T3 migrations", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runScientMigrations(sql);
      yield* runMigrations();

      const t3Ledger = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM effect_sql_migrations ORDER BY migration_id
      `;
      const scientLedger = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM scient_schema_migrations ORDER BY migration_id
      `;
      assert.isTrue(t3Ledger.length > 0);
      assert.deepStrictEqual(
        scientLedger.map((row) => row.migration_id),
        SCIENT_MIGRATION_IDS,
      );
    }),
  ),
);

// ---------------------------------------------------------------------------
// VAL-MIGRATE-10: Malformed rows are quarantined (startup stays safe)
// ---------------------------------------------------------------------------

interface QuarantinedRow {
  readonly thread_id: string;
  readonly reason: string;
  readonly payload_json: string;
  readonly quarantined_at: string;
}

it.effect("invalid workspace_mode is quarantined with evidence", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* setupMalformedDatabase(sql, "malformed-1", "bad-mode", "pending", "origin");

      const executed = yield* runScientMigrations(sql);
      assert.deepStrictEqual(
        executed.map(([id]) => id),
        SCIENT_MIGRATIONS_AFTER_BOOTSTRAP,
      );

      const ledger = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM scient_schema_migrations ORDER BY migration_id
      `;
      assert.deepStrictEqual(
        ledger.map((row) => row.migration_id),
        SCIENT_MIGRATION_IDS,
      );

      const rows = yield* sql<{ readonly thread_id: string }>`
        SELECT thread_id FROM scient_thread_lineage
      `;
      assert.strictEqual(rows.length, 0);

      const quarantined = yield* sql<QuarantinedRow>`
        SELECT thread_id, reason, payload_json, quarantined_at
        FROM scient_thread_lineage_quarantine
      `;
      assert.strictEqual(quarantined.length, 1);
      assert.strictEqual(quarantined[0]!.thread_id, "malformed-1");
      assert.isTrue(
        quarantined[0]!.reason.includes("workspace_mode 'bad-mode' is not valid"),
        `Unexpected quarantine reason: ${quarantined[0]!.reason}`,
      );
      const payload = decodeQuarantinePayload(quarantined[0]!.payload_json);
      assert.strictEqual(payload.workspace_mode, "bad-mode");
    }),
  ),
);

it.effect("null forked_from_thread_id is quarantined without fabrication", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* setupMalformedDatabase(sql, "orphan-fork", "local", "pending", null);

      const executed = yield* runScientMigrations(sql);
      assert.deepStrictEqual(
        executed.map(([id]) => id),
        SCIENT_MIGRATIONS_AFTER_BOOTSTRAP,
      );

      const rows = yield* sql<{ readonly thread_id: string }>`
        SELECT thread_id FROM scient_thread_lineage
      `;
      assert.strictEqual(rows.length, 0);

      const quarantined = yield* sql<QuarantinedRow>`
        SELECT thread_id, reason, payload_json, quarantined_at
        FROM scient_thread_lineage_quarantine
      `;
      assert.strictEqual(quarantined.length, 1);
      assert.strictEqual(quarantined[0]!.thread_id, "orphan-fork");
      assert.isTrue(
        quarantined[0]!.reason.includes("forked_from_thread_id is null"),
        `Unexpected quarantine reason: ${quarantined[0]!.reason}`,
      );
      const payload = decodeQuarantinePayload(quarantined[0]!.payload_json);
      assert.strictEqual(payload.forked_from_thread_id, null);
    }),
  ),
);

it.effect("invalid status is quarantined with evidence", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* setupMalformedDatabase(sql, "bad-status", "local", "bogus", "origin");

      const executed = yield* runScientMigrations(sql);
      assert.deepStrictEqual(
        executed.map(([id]) => id),
        SCIENT_MIGRATIONS_AFTER_BOOTSTRAP,
      );

      const rows = yield* sql<{ readonly thread_id: string }>`
        SELECT thread_id FROM scient_thread_lineage
      `;
      assert.strictEqual(rows.length, 0);

      const quarantined = yield* sql<QuarantinedRow>`
        SELECT thread_id, reason, payload_json, quarantined_at
        FROM scient_thread_lineage_quarantine
      `;
      assert.strictEqual(quarantined.length, 1);
      assert.strictEqual(quarantined[0]!.thread_id, "bad-status");
      assert.isTrue(
        quarantined[0]!.reason.includes("status 'bogus' is not valid"),
        `Unexpected quarantine reason: ${quarantined[0]!.reason}`,
      );
      const payload = decodeQuarantinePayload(quarantined[0]!.payload_json);
      assert.strictEqual(payload.status, "bogus");
    }),
  ),
);

// ---------------------------------------------------------------------------
// VAL-MIGRATE-11: Canonical active defaults are universal
// ---------------------------------------------------------------------------

it.effect("all normalized rows have canonical active defaults", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* sql`
        CREATE TABLE scient_thread_lineage (
          thread_id TEXT PRIMARY KEY,
          forked_from_thread_id TEXT,
          fork_point_turn_count INTEGER,
          workspace_mode TEXT,
          fidelity_mode TEXT,
          baseline_turn_id TEXT,
          created_at TEXT
        )
      `;
      yield* sql`
        INSERT INTO scient_thread_lineage (
          thread_id, forked_from_thread_id, fork_point_turn_count,
          workspace_mode, fidelity_mode, baseline_turn_id, created_at
        ) VALUES
          ('fork-a', 'origin-a', 1, 'local', 'chat-only', 'baseline-a', '2026-01-01T00:00:00.000Z'),
          ('fork-b', 'origin-b', 2, 'new-worktree', 'replay', 'baseline-b', '2026-02-01T00:00:00.000Z'),
          ('fork-c', 'origin-c', 0, 'local', NULL, 'baseline-c', '2026-03-01T00:00:00.000Z')
      `;

      yield* runScientMigrations(sql);

      const rows = yield* sql<{
        readonly thread_id: string;
        readonly provider_mode: string;
        readonly provider_bootstrap_status: string;
        readonly attachment_copies_json: string;
        readonly fidelity_mode: string;
        readonly status: string;
        readonly checkpoint_status: string;
        readonly workspace_status: string;
        readonly attempt_count: number;
        readonly updated_at: string;
        readonly created_at: string;
      }>`
        SELECT thread_id, provider_mode, provider_bootstrap_status, attachment_copies_json,
               fidelity_mode, status, checkpoint_status, workspace_status, attempt_count,
               updated_at, created_at
        FROM scient_thread_lineage ORDER BY thread_id
      `;

      assert.strictEqual(rows.length, 3);
      for (const row of rows) {
        assert.strictEqual(row.provider_mode, "transcript-bootstrap");
        assert.strictEqual(row.fidelity_mode, "transcript-bootstrap");
        assert.strictEqual(row.provider_bootstrap_status, "pending");
        assert.strictEqual(row.attachment_copies_json, "[]");
        assert.strictEqual(row.status, "pending");
        assert.strictEqual(row.checkpoint_status, "pending");
        assert.strictEqual(row.workspace_status, "pending");
        assert.strictEqual(row.attempt_count, 0);
        assert.strictEqual(row.updated_at, row.created_at);
      }

      const protoModes = yield* sql<{ readonly cnt: number }>`
        SELECT COUNT(*) AS cnt FROM scient_thread_lineage
        WHERE provider_mode IN ('cold-start', 'chat-only', 'replay')
           OR fidelity_mode IN ('cold-start', 'chat-only', 'replay')
      `;
      assert.strictEqual(protoModes[0]!.cnt, 0);
    }),
  ),
);

// ---------------------------------------------------------------------------
// VAL-MIGRATE-12: Physical compatibility columns remain in first pass
// ---------------------------------------------------------------------------

it.effect("physical compatibility columns provider_mode and fidelity_mode remain queryable", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runScientMigrations(sql);

      const columns = yield* tableInfo(sql, "scient_thread_lineage");
      const columnNames = new Set(columns.map((column) => column.name));
      assert.isTrue(columnNames.has("provider_mode"));
      assert.isTrue(columnNames.has("fidelity_mode"));

      yield* sql`
        INSERT INTO scient_thread_lineage (
          thread_id, forked_from_thread_id, fork_point_turn_id, fork_point_turn_count,
          source_checkpoint_turn_count, baseline_turn_id, baseline_user_message_id,
          baseline_assistant_message_id, workspace_mode, provider_mode,
          provider_bootstrap_status, attachment_copies_json, fidelity_mode,
          status, checkpoint_status, workspace_status, attempt_count, last_error,
          created_at, updated_at
        ) VALUES (
          'compat-fork', 'origin', 'turn-1', 1,
          NULL, 'baseline-1', NULL, NULL,
          'local', 'transcript-bootstrap',
          'pending', '[]', 'transcript-bootstrap',
          'pending', 'pending', 'pending', 0, NULL,
          '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z'
        )
      `;
      const rows = yield* sql<{ readonly provider_mode: string; readonly fidelity_mode: string }>`
        SELECT provider_mode, fidelity_mode FROM scient_thread_lineage WHERE thread_id = 'compat-fork'
      `;
      assert.strictEqual(rows[0]!.provider_mode, "transcript-bootstrap");
      assert.strictEqual(rows[0]!.fidelity_mode, "transcript-bootstrap");
    }),
  ),
);

// ---------------------------------------------------------------------------
// VAL-MIGRATE-14: Canonically valid active lineage is preserved
// ---------------------------------------------------------------------------

it.effect("provider-bootstrap lineage is preserved across normalization", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* sql`
        CREATE TABLE scient_thread_lineage (
          thread_id TEXT PRIMARY KEY,
          forked_from_thread_id TEXT,
          fork_point_turn_count INTEGER,
          workspace_mode TEXT,
          fidelity_mode TEXT,
          baseline_turn_id TEXT,
          created_at TEXT
        )
      `;
      yield* sql`
        INSERT INTO scient_thread_lineage (
          thread_id, forked_from_thread_id, fork_point_turn_count,
          workspace_mode, fidelity_mode, baseline_turn_id, created_at
        ) VALUES
          ('keep-a', 'origin-a', 1, 'local', 'chat-only', 'baseline-a', '2026-01-01T00:00:00.000Z'),
          ('keep-b', 'origin-b', 2, 'new-worktree', 'replay', 'baseline-b', '2026-02-01T00:00:00.000Z'),
          ('keep-c', 'origin-c', 0, 'local', NULL, 'baseline-c', '2026-03-01T00:00:00.000Z')
      `;

      const beforeRows = yield* sql<{
        readonly thread_id: string;
        readonly forked_from_thread_id: string;
      }>`
        SELECT thread_id, forked_from_thread_id FROM scient_thread_lineage ORDER BY thread_id
      `;

      yield* runScientMigrations(sql);

      const afterRows = yield* sql<{
        readonly thread_id: string;
        readonly forked_from_thread_id: string;
      }>`
        SELECT thread_id, forked_from_thread_id FROM scient_thread_lineage ORDER BY thread_id
      `;

      assert.strictEqual(afterRows.length, beforeRows.length);
      assert.deepStrictEqual(afterRows, beforeRows);
    }),
  ),
);

it.effect("no valid lineage rows are discarded across current schema upgrade", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runScientMigrations(sql);
      yield* sql`DELETE FROM scient_thread_lineage`;

      yield* sql`
        INSERT INTO scient_thread_lineage (
          thread_id, forked_from_thread_id, fork_point_turn_id, fork_point_turn_count,
          source_checkpoint_turn_count, baseline_turn_id, baseline_user_message_id,
          baseline_assistant_message_id, workspace_mode, provider_mode,
          provider_bootstrap_status, attachment_copies_json, fidelity_mode,
          status, checkpoint_status, workspace_status, attempt_count, last_error,
          created_at, updated_at
        ) VALUES
          ('curr-pending', 'origin-1', 'turn-1', 2, 2, 'base-1', 'u-1', 'a-1',
           'local', 'transcript-bootstrap', 'pending', '[]', 'transcript-bootstrap',
           'pending', 'pending', 'pending', 0, NULL,
           '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z'),
          ('curr-ready', 'origin-2', 'turn-2', 1, NULL, 'base-2', NULL, 'a-2',
           'new-worktree', 'transcript-bootstrap', 'completed', '[]', 'transcript-bootstrap',
           'ready', 'ready', 'worktree', 1, NULL,
           '2026-02-01T00:00:00.000Z', '2026-02-01T12:00:00.000Z'),
          ('curr-abandoned', 'origin-3', 'turn-3', 3, 3, 'base-3', 'u-3', 'a-3',
           'local', 'transcript-bootstrap', 'pending', '[]', 'transcript-bootstrap',
           'abandoned', 'unavailable', 'shared', 3, 'permanently failed',
           '2026-03-01T00:00:00.000Z', '2026-03-01T06:00:00.000Z')
      `;

      const beforeRows = yield* sql<{ readonly thread_id: string }>`
        SELECT thread_id FROM scient_thread_lineage ORDER BY thread_id
      `;

      yield* runScientMigrations(sql);

      const afterRows = yield* sql<{ readonly thread_id: string }>`
        SELECT thread_id FROM scient_thread_lineage ORDER BY thread_id
      `;

      assert.strictEqual(afterRows.length, beforeRows.length);
      assert.deepStrictEqual(afterRows, beforeRows);
    }),
  ),
);

// ---------------------------------------------------------------------------
// VAL-MIGRATE-15: applied_at column reconciled with Effect Migrator created_at
// ---------------------------------------------------------------------------

it.effect("applied_at ledger is reconciled to created_at preserving all values", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* sql`
        CREATE TABLE scient_schema_migrations (
          migration_id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          applied_at TEXT NOT NULL
        )
      `;
      const legacyData: Array<{ id: number; name: string; ts: string }> = [
        { id: 1, name: "durable-thread-forks", ts: "2026-07-01T10:00:00.000Z" },
        { id: 2, name: "durable-provider-bootstrap", ts: "2026-07-02T12:00:00.000Z" },
      ];
      for (const entry of legacyData) {
        yield* sql`
          INSERT INTO scient_schema_migrations (migration_id, name, applied_at)
          VALUES (${entry.id}, ${entry.name}, ${entry.ts})
        `;
      }

      yield* sql`
        CREATE TABLE scient_thread_lineage (
          thread_id TEXT PRIMARY KEY,
          forked_from_thread_id TEXT,
          fork_point_turn_count INTEGER,
          workspace_mode TEXT,
          fidelity_mode TEXT,
          created_at TEXT
        )
      `;

      yield* runScientMigrations(sql);

      const ledger = yield* sql<{
        readonly migration_id: number;
        readonly name: string;
        readonly created_at: string;
        readonly applied_at: string;
      }>`SELECT migration_id, name, created_at, applied_at FROM scient_schema_migrations ORDER BY migration_id`;

      assert.strictEqual(ledger.length, SCIENT_MIGRATION_IDS.length);

      for (let i = 0; i < legacyData.length; i++) {
        const expected = legacyData[i]!;
        const row = ledger[i]!;
        assert.strictEqual(row.migration_id, expected.id);
        assert.strictEqual(row.name, expected.name);
        assert.strictEqual(
          row.created_at,
          expected.ts,
          `created_at mismatch for migration ${expected.id}`,
        );
        assert.strictEqual(
          row.applied_at,
          expected.ts,
          `applied_at not preserved for migration ${expected.id}`,
        );
      }

      assert.strictEqual(ledger[2]!.migration_id, 3);
      assert.isTrue(ledger[2]!.created_at.length > 0);
    }),
  ),
);

it.effect("fresh database uses created_at without applied_at column", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runScientMigrations(sql);

      // Fresh database: created_at exists, applied_at column doesn't exist.
      const columns = yield* tableInfo(sql, "scient_schema_migrations");
      const columnNames = new Set(columns.map((column) => column.name));
      assert.isTrue(columnNames.has("created_at"));
      assert.isFalse(columnNames.has("applied_at"));

      const ledger = yield* sql<{
        readonly migration_id: number;
        readonly name: string;
        readonly created_at: string;
      }>`SELECT migration_id, name, created_at FROM scient_schema_migrations ORDER BY migration_id`;

      assert.deepStrictEqual(
        ledger.map((row) => row.migration_id),
        SCIENT_MIGRATION_IDS,
      );
      assert.deepStrictEqual(
        ledger.map((row) => row.name),
        SCIENT_MIGRATION_NAMES,
      );
      assert.isTrue(ledger.every((row) => row.created_at.length > 0));
    }),
  ),
);

// ---------------------------------------------------------------------------
// Regression: Concurrent startup against an applied_at ledger
// ---------------------------------------------------------------------------

it.effect(
  "concurrent runners against an applied_at ledger do not fail on duplicate created_at",
  () => {
    const tempFile = NodePath.join(
      NodeOS.tmpdir(),
      `scient-test-concurrent-applied-at-${NodeCrypto.randomUUID()}.sqlite`,
    );

    return Effect.gen(function* () {
      yield* Effect.acquireUseRelease(
        Effect.succeed(tempFile),
        (filename) =>
          Effect.gen(function* () {
            // Set up a legacy applied_at ledger with entries 1 and 2, plus a
            // prototype lineage table, on a fresh file connection.
            const setup = Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`
              CREATE TABLE scient_schema_migrations (
                migration_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TEXT NOT NULL
              )
            `;
              yield* sql`INSERT INTO scient_schema_migrations (migration_id, name, applied_at) VALUES (1, 'durable-thread-forks', '2026-07-01T10:00:00.000Z')`;
              yield* sql`INSERT INTO scient_schema_migrations (migration_id, name, applied_at) VALUES (2, 'durable-provider-bootstrap', '2026-07-02T12:00:00.000Z')`;

              yield* sql`
              CREATE TABLE scient_thread_lineage (
                thread_id TEXT PRIMARY KEY,
                forked_from_thread_id TEXT,
                fork_point_turn_count INTEGER,
                workspace_mode TEXT,
                fidelity_mode TEXT,
                created_at TEXT
              )
            `;
              yield* sql`INSERT INTO scient_thread_lineage (thread_id, forked_from_thread_id, fork_point_turn_count, workspace_mode, fidelity_mode, created_at) VALUES ('proto-fork', 'origin', 1, 'local', 'chat-only', '2026-07-15T00:00:00.000Z')`;
            }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
            yield* setup;

            // Race two concurrent runners on the same file.
            const runOnFile = Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              return yield* runScientMigrations(sql);
            }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));

            const [result1, result2] = yield* Effect.all([runOnFile, runOnFile], {
              concurrency: 2,
            });

            // One runner applied migrations 3 through 7; the other got empty.
            const totalApplied = result1.length + result2.length;
            assert.isTrue(
              totalApplied <= SCIENT_MIGRATIONS_AFTER_BOOTSTRAP.length,
              `Total applied ${totalApplied} exceeds ${SCIENT_MIGRATIONS_AFTER_BOOTSTRAP.length}`,
            );
            const winner = result1.length >= result2.length ? result1 : result2;
            const loser = result1.length >= result2.length ? result2 : result1;
            assert.deepStrictEqual(
              winner.map(([id]) => id),
              SCIENT_MIGRATIONS_AFTER_BOOTSTRAP,
            );
            assert.strictEqual(loser.length, 0);

            // Verify the final ledger with a fresh connection.
            const verify = Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;

              // Legacy timestamps preserved after concurrent reconciliation.
              const ledger = yield* sql<{
                readonly migration_id: number;
                readonly name: string;
                readonly created_at: string;
                readonly applied_at: string;
              }>`SELECT migration_id, name, created_at, applied_at FROM scient_schema_migrations ORDER BY migration_id`;

              assert.strictEqual(ledger.length, SCIENT_MIGRATION_IDS.length);
              assert.strictEqual(ledger[0]!.migration_id, 1);
              assert.strictEqual(ledger[0]!.name, "durable-thread-forks");
              assert.strictEqual(ledger[0]!.applied_at, "2026-07-01T10:00:00.000Z");
              assert.strictEqual(ledger[0]!.created_at, "2026-07-01T10:00:00.000Z");

              assert.strictEqual(ledger[1]!.migration_id, 2);
              assert.strictEqual(ledger[1]!.name, "durable-provider-bootstrap");
              assert.strictEqual(ledger[1]!.applied_at, "2026-07-02T12:00:00.000Z");
              assert.strictEqual(ledger[1]!.created_at, "2026-07-02T12:00:00.000Z");

              assert.strictEqual(ledger[2]!.migration_id, 3);
              assert.strictEqual(ledger[2]!.name, "normalize-active-lineage");
              assert.isTrue(ledger[2]!.created_at.length > 0);
              assert.strictEqual(ledger[3]!.migration_id, 4);
              assert.strictEqual(ledger[3]!.name, "quarantine-invalid-lineage");
              assert.strictEqual(ledger[4]!.migration_id, 5);
              assert.strictEqual(ledger[4]!.name, "analysis-run-index");
              assert.strictEqual(ledger[5]!.migration_id, 6);
              assert.strictEqual(ledger[5]!.name, "analysis-run-projection-state");
              assert.strictEqual(ledger[6]!.migration_id, 7);
              assert.strictEqual(ledger[6]!.name, "analysis-run-storage-status");
            }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
            yield* verify;
          }),
        (filename) =>
          Effect.sync(() => {
            for (const suffix of ["", "-wal", "-shm"]) {
              try {
                NodeFS.unlinkSync(filename + suffix);
              } catch {
                // ignore
              }
            }
          }),
      );
    });
  },
);

// ---------------------------------------------------------------------------
// Regression: Crash/retry-safe timestamp preservation
// ---------------------------------------------------------------------------

it.effect("partial reconciliation is recovered on retry without timestamp loss", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      // Create a legacy applied_at ledger with known timestamps.
      yield* sql`
        CREATE TABLE scient_schema_migrations (
          migration_id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          applied_at TEXT NOT NULL
        )
      `;
      const legacyData: Array<{ id: number; name: string; ts: string }> = [
        { id: 1, name: "durable-thread-forks", ts: "2026-07-01T10:00:00.000Z" },
        { id: 2, name: "durable-provider-bootstrap", ts: "2026-07-02T12:00:00.000Z" },
      ];
      for (const entry of legacyData) {
        yield* sql`
          INSERT INTO scient_schema_migrations (migration_id, name, applied_at)
          VALUES (${entry.id}, ${entry.name}, ${entry.ts})
        `;
      }

      // Simulate a partial reconciliation: the ALTER succeeded (created_at
      // column was added) but the UPDATE did not (created_at values remain
      // NULL). This is the crash window between ALTER and UPDATE.
      yield* sql.unsafe("ALTER TABLE scient_schema_migrations ADD COLUMN created_at TEXT");

      // Verify the partial state: created_at exists but is NULL for all rows.
      const partial = yield* sql<{
        readonly migration_id: number;
        readonly created_at: string | null;
      }>`SELECT migration_id, created_at FROM scient_schema_migrations ORDER BY migration_id`;
      assert.strictEqual(partial[0]!.created_at, null);
      assert.strictEqual(partial[1]!.created_at, null);

      // Also create the lineage table so migration 3 doesn't fail on missing table.
      yield* sql`
        CREATE TABLE scient_thread_lineage (
          thread_id TEXT PRIMARY KEY,
          forked_from_thread_id TEXT,
          fork_point_turn_count INTEGER,
          workspace_mode TEXT,
          fidelity_mode TEXT,
          created_at TEXT
        )
      `;
      yield* sql`INSERT INTO scient_thread_lineage (thread_id, forked_from_thread_id, fork_point_turn_count, workspace_mode, fidelity_mode, created_at) VALUES ('proto-fork', 'origin', 1, 'local', 'chat-only', '2026-07-15T00:00:00.000Z')`;

      // Retry: the runner should detect both columns exist, skip the ALTER,
      // and still backfill the missing created_at values from applied_at.
      const executed = yield* runScientMigrations(sql);
      assert.deepStrictEqual(
        executed.map(([id]) => id),
        SCIENT_MIGRATIONS_AFTER_BOOTSTRAP,
      );

      // All legacy timestamps are preserved in both columns.
      const ledger = yield* sql<{
        readonly migration_id: number;
        readonly name: string;
        readonly created_at: string;
        readonly applied_at: string;
      }>`SELECT migration_id, name, created_at, applied_at FROM scient_schema_migrations ORDER BY migration_id`;

      assert.strictEqual(ledger.length, SCIENT_MIGRATION_IDS.length);
      for (let i = 0; i < legacyData.length; i++) {
        const expected = legacyData[i]!;
        const row = ledger[i]!;
        assert.strictEqual(row.migration_id, expected.id);
        assert.strictEqual(row.name, expected.name);
        assert.strictEqual(row.created_at, expected.ts);
        assert.strictEqual(row.applied_at, expected.ts);
      }
      assert.strictEqual(ledger[2]!.migration_id, 3);
      assert.isTrue(ledger[2]!.created_at.length > 0);
    }),
  ),
);

it.effect("reconciled ledger with all created_at populated is idempotent on retry", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      // Create a fully reconciled ledger (applied_at + created_at both populated).
      yield* sql`
        CREATE TABLE scient_schema_migrations (
          migration_id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          applied_at TEXT NOT NULL,
          created_at TEXT
        )
      `;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name, applied_at, created_at) VALUES (1, 'durable-thread-forks', '2026-07-01T10:00:00.000Z', '2026-07-01T10:00:00.000Z')`;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name, applied_at, created_at) VALUES (2, 'durable-provider-bootstrap', '2026-07-02T12:00:00.000Z', '2026-07-02T12:00:00.000Z')`;

      yield* sql`
        CREATE TABLE scient_thread_lineage (
          thread_id TEXT PRIMARY KEY,
          forked_from_thread_id TEXT,
          fork_point_turn_count INTEGER,
          workspace_mode TEXT,
          fidelity_mode TEXT,
          created_at TEXT
        )
      `;
      yield* sql`INSERT INTO scient_thread_lineage (thread_id, forked_from_thread_id, fork_point_turn_count, workspace_mode, fidelity_mode, created_at) VALUES ('proto-fork', 'origin', 1, 'local', 'chat-only', '2026-07-15T00:00:00.000Z')`;

      // Retry: reconciliation should be a no-op (backfill finds nothing to do).
      const executed = yield* runScientMigrations(sql);
      assert.deepStrictEqual(
        executed.map(([id]) => id),
        SCIENT_MIGRATIONS_AFTER_BOOTSTRAP,
      );

      // Timestamps unchanged.
      const ledger = yield* sql<{
        readonly migration_id: number;
        readonly created_at: string;
        readonly applied_at: string;
      }>`SELECT migration_id, created_at, applied_at FROM scient_schema_migrations ORDER BY migration_id`;
      assert.strictEqual(ledger[0]!.created_at, "2026-07-01T10:00:00.000Z");
      assert.strictEqual(ledger[0]!.applied_at, "2026-07-01T10:00:00.000Z");
      assert.strictEqual(ledger[1]!.created_at, "2026-07-02T12:00:00.000Z");
      assert.strictEqual(ledger[1]!.applied_at, "2026-07-02T12:00:00.000Z");
    }),
  ),
);

// ---------------------------------------------------------------------------
// Regression: NULL workspace_mode is quarantined without fabrication
// ---------------------------------------------------------------------------

it.effect("NULL workspace_mode is quarantined without fabrication", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* setupMalformedDatabase(sql, "null-mode-fork", null, "pending", "origin");

      const executed = yield* runScientMigrations(sql);
      assert.deepStrictEqual(
        executed.map(([id]) => id),
        SCIENT_MIGRATIONS_AFTER_BOOTSTRAP,
      );

      // Migration 3 is recorded; the row is quarantined, not fabricated.
      const ledger = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM scient_schema_migrations ORDER BY migration_id
      `;
      assert.deepStrictEqual(
        ledger.map((row) => row.migration_id),
        SCIENT_MIGRATION_IDS,
      );

      const rows = yield* sql<{ readonly thread_id: string }>`
        SELECT thread_id FROM scient_thread_lineage
      `;
      assert.strictEqual(rows.length, 0);

      const quarantined = yield* sql<QuarantinedRow>`
        SELECT thread_id, reason, payload_json, quarantined_at
        FROM scient_thread_lineage_quarantine
      `;
      assert.strictEqual(quarantined.length, 1);
      assert.strictEqual(quarantined[0]!.thread_id, "null-mode-fork");
      assert.isTrue(
        quarantined[0]!.reason.includes("workspace_mode is null"),
        `Unexpected quarantine reason: ${quarantined[0]!.reason}`,
      );
      const payload = decodeQuarantinePayload(quarantined[0]!.payload_json);
      assert.strictEqual(payload.workspace_mode, null);
    }),
  ),
);

// ---------------------------------------------------------------------------
// VAL-MIGRATE-05 (duplicate check): Manifest has no duplicates
// ---------------------------------------------------------------------------

it.effect("manifest has no duplicate migration IDs", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      const ids = SCIENT_MIGRATIONS.map((migration) => migration.id);
      assert.strictEqual(new Set(ids).size, ids.length);

      const executed = yield* runScientMigrations(sql);
      assert.strictEqual(executed.length, SCIENT_MIGRATIONS.length);
    }),
  ),
);

// ---------------------------------------------------------------------------
// Ledger integrity preflight: gaps, name mismatches, and future IDs fail
// closed before the standard Migrator's high-water mark can hide them.
// ---------------------------------------------------------------------------

it.effect("a gapped ledger fails closed before any migration runs", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`
        CREATE TABLE scient_schema_migrations (
          migration_id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
      `;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name) VALUES (1, 'durable-thread-forks')`;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name) VALUES (3, 'normalize-active-lineage')`;

      const error = yield* Effect.flip(runScientMigrations(sql));
      if (error._tag !== "ScientMigrationError") {
        assert.fail(`Expected ScientMigrationError, got ${error._tag}`);
      } else {
        assert.strictEqual(error.kind, "BadState");
        assert.isTrue(
          error.message.includes("contiguous prefix"),
          `Unexpected message: ${error.message}`,
        );
      }

      // Nothing ran: ledger unchanged, lineage table never created.
      const ledger = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM scient_schema_migrations ORDER BY migration_id
      `;
      assert.deepStrictEqual(
        ledger.map((row) => row.migration_id),
        [1, 3],
      );
      const tables = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'scient_thread_lineage'
      `;
      assert.strictEqual(tables.length, 0);
    }),
  ),
);

it.effect("a renamed ledger entry fails closed even through legacy reconciliation", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      // Legacy applied_at ledger with a hand-modified name.
      yield* sql`
        CREATE TABLE scient_schema_migrations (
          migration_id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          applied_at TEXT NOT NULL
        )
      `;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name, applied_at) VALUES (1, 'renamed-by-hand', '2026-07-01T10:00:00.000Z')`;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name, applied_at) VALUES (2, 'durable-provider-bootstrap', '2026-07-02T12:00:00.000Z')`;

      const error = yield* Effect.flip(runScientMigrations(sql));
      if (error._tag !== "ScientMigrationError") {
        assert.fail(`Expected ScientMigrationError, got ${error._tag}`);
      } else {
        assert.strictEqual(error.kind, "BadState");
        assert.isTrue(
          error.message.includes("renamed-by-hand"),
          `Unexpected message: ${error.message}`,
        );
      }

      // Reconciliation still preserved the recorded history exactly.
      const ledger = yield* sql<{
        readonly migration_id: number;
        readonly name: string;
        readonly created_at: string;
        readonly applied_at: string;
      }>`SELECT migration_id, name, created_at, applied_at FROM scient_schema_migrations ORDER BY migration_id`;
      assert.strictEqual(ledger.length, 2);
      assert.strictEqual(ledger[0]!.name, "renamed-by-hand");
      assert.strictEqual(ledger[0]!.created_at, "2026-07-01T10:00:00.000Z");
      assert.strictEqual(ledger[0]!.applied_at, "2026-07-01T10:00:00.000Z");
    }),
  ),
);

it.effect("a ledger from a newer build (unknown future ID) fails closed", () =>
  withMemory(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`
        CREATE TABLE scient_schema_migrations (
          migration_id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
      `;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name) VALUES (1, 'durable-thread-forks')`;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name) VALUES (2, 'durable-provider-bootstrap')`;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name) VALUES (3, 'normalize-active-lineage')`;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name) VALUES (4, 'quarantine-invalid-lineage')`;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name) VALUES (5, 'analysis-run-index')`;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name) VALUES (6, 'analysis-run-projection-state')`;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name) VALUES (7, 'analysis-run-storage-status')`;
      yield* sql`INSERT INTO scient_schema_migrations (migration_id, name) VALUES (8, 'future-migration')`;

      const error = yield* Effect.flip(runScientMigrations(sql));
      if (error._tag !== "ScientMigrationError") {
        assert.fail(`Expected ScientMigrationError, got ${error._tag}`);
      } else {
        assert.strictEqual(error.kind, "BadState");
        assert.isTrue(
          error.message.includes("unknown migration 8"),
          `Unexpected message: ${error.message}`,
        );
      }

      // Ledger untouched, including the unknown future entry.
      const ledger = yield* sql<{ readonly migration_id: number }>`
        SELECT migration_id FROM scient_schema_migrations ORDER BY migration_id
      `;
      assert.deepStrictEqual(
        ledger.map((row) => row.migration_id),
        [1, 2, 3, 4, 5, 6, 7, 8],
      );
    }),
  ),
);
