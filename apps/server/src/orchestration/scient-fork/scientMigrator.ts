/**
 * Scient-owned versioned migration runner.
 *
 * Built on the standard Effect SQL Migrator (`Migrator.make`) with a separate
 * `scient_schema_migrations` ledger table. T3's `effect_sql_migrations` table
 * and numbering are never modified.
 *
 * Before delegating to the standard Migrator, the runner performs two
 * Scient-owned preflight passes:
 *
 * 1. Ledger reconciliation. Existing development databases created by the
 *    legacy `ensureScientForkSchema` recorded the ledger with an
 *    `applied_at TEXT NOT NULL` column and no `created_at`. The standard
 *    Migrator expects the canonical `(migration_id, created_at, name)` shape
 *    and inserts only `(migration_id, name)`, relying on
 *    `DEFAULT current_timestamp`. The preflight rebuilds a legacy ledger into
 *    that canonical shape in a single transaction, copying `applied_at` into
 *    `created_at` and keeping `applied_at` as a nullable historical-residue
 *    column. No timestamp value, ledger ID, or migration name is lost or
 *    altered. Fresh databases skip reconciliation; the Migrator creates the
 *    canonical table directly.
 *
 * 2. Strict ledger integrity validation. The standard Migrator treats the
 *    latest recorded ID as a high-water mark and never inspects earlier rows,
 *    so a gapped, renamed, or newer-than-this-build ledger would be silently
 *    accepted. Scient additionally requires the recorded ledger to be a
 *    contiguous prefix of the manifest with exact name matches: no gaps, no
 *    unknown future IDs, no mismatched names. Violations fail closed with a
 *    `ScientMigrationError` of kind `BadState` before any migration runs.
 *
 * SCIENT-OWNED.
 */
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Migrator from "effect/unstable/sql/Migrator";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import Migration001 from "./migrations/001_DurableThreadForks.ts";
import Migration002 from "./migrations/002_DurableProviderBootstrap.ts";
import Migration003 from "./migrations/003_NormalizeActiveLineage.ts";
import Migration004 from "./migrations/004_QuarantineInvalidLineage.ts";
import Migration005 from "./migrations/005_AnalysisRunIndex.ts";
import Migration006 from "./migrations/006_AnalysisRunProjectionState.ts";
import Migration007 from "./migrations/007_AnalysisRunStorageStatus.ts";

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

export class ScientMigrationError extends Data.TaggedError("ScientMigrationError")<{
  readonly kind: "BadState" | "Duplicates" | "Failed" | "ImportError" | "Locked";
  readonly message: string;
  readonly cause?: unknown;
}> {}

const fromMigrationError = (error: Migrator.MigrationError): ScientMigrationError =>
  new ScientMigrationError({
    kind: error.kind,
    message: error.message,
    cause: error.cause,
  });

// ---------------------------------------------------------------------------
// Migration manifest
// ---------------------------------------------------------------------------

export interface ScientMigration {
  readonly id: number;
  readonly name: string;
  // Migration effects use SqlClient and fail with SqlError. The runner wraps
  // all failures into ScientMigrationError.
  readonly effect: Effect.Effect<void, SqlError | Error, SqlClient.SqlClient>;
}

/**
 * Explicit, ordered Scient migration manifest.
 *
 * IDs 1 and 2 match the names recorded by the legacy `ensureScientForkSchema`
 * function. Existing databases that already have these ledger entries will not
 * re-run them. Migration 3 is the historical normalization pass; migration 4
 * finalizes decoder-aligned quarantine for databases that already recorded 3.
 */
export const SCIENT_MIGRATIONS: ReadonlyArray<ScientMigration> = [
  { id: 1, name: "durable-thread-forks", effect: Migration001 },
  { id: 2, name: "durable-provider-bootstrap", effect: Migration002 },
  { id: 3, name: "normalize-active-lineage", effect: Migration003 },
  { id: 4, name: "quarantine-invalid-lineage", effect: Migration004 },
  { id: 5, name: "analysis-run-index", effect: Migration005 },
  { id: 6, name: "analysis-run-projection-state", effect: Migration006 },
  { id: 7, name: "analysis-run-storage-status", effect: Migration007 },
] as const;

const loader = Migrator.fromRecord(
  Object.fromEntries(
    SCIENT_MIGRATIONS.map((migration) => [`${migration.id}_${migration.name}`, migration.effect]),
  ),
);

// ---------------------------------------------------------------------------
// Preflight: legacy ledger reconciliation (applied_at -> created_at)
// ---------------------------------------------------------------------------

const LEDGER_TABLE = "scient_schema_migrations";

interface TableColumn {
  readonly name: string;
  readonly notnull: number;
}

/**
 * Rebuild a legacy `applied_at` ledger into the canonical Effect Migrator
 * shape, transactionally.
 *
 * Detection key: a legacy ledger declares `applied_at TEXT NOT NULL`. A
 * ledger that already went through reconciliation keeps `applied_at` as a
 * *nullable* residue column, so `notnull = 0` marks a completed rebuild and
 * makes the pass idempotent. Both a missing `created_at` (pure legacy) and a
 * partially populated one (crash between an earlier runner's ALTER and
 * backfill) are handled by reading `COALESCE(created_at, applied_at)` when
 * the column exists.
 */
const reconcileLedger = Effect.fn("reconcileScientLedger")(function* (sql: SqlClient.SqlClient) {
  const tables = yield* sql<{ readonly name: string }>`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'scient_schema_migrations'
  `;
  if (tables.length === 0) return; // Fresh database — the Migrator creates the canonical table.

  const columns = yield* sql<TableColumn>`PRAGMA table_info(scient_schema_migrations)`;
  const appliedAt = columns.find((column) => column.name === "applied_at");
  if (!appliedAt || appliedAt.notnull === 0) return; // Canonical or already reconciled.

  const hasCreatedAt = columns.some((column) => column.name === "created_at");
  const createdAtExpr = hasCreatedAt ? "COALESCE(created_at, applied_at)" : "applied_at";

  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql.unsafe(`CREATE TABLE scient_schema_migrations_rebuild (
  migration_id integer PRIMARY KEY NOT NULL,
  created_at datetime NOT NULL DEFAULT current_timestamp,
  name VARCHAR(255) NOT NULL,
  applied_at TEXT
)`);
      yield* sql.unsafe(`INSERT INTO scient_schema_migrations_rebuild (migration_id, name, created_at, applied_at)
  SELECT migration_id, name, ${createdAtExpr}, applied_at FROM scient_schema_migrations`);
      yield* sql.unsafe(`DROP TABLE scient_schema_migrations`);
      yield* sql.unsafe(
        `ALTER TABLE scient_schema_migrations_rebuild RENAME TO scient_schema_migrations`,
      );
    }),
  );
});

// ---------------------------------------------------------------------------
// Preflight: strict ledger integrity validation
// ---------------------------------------------------------------------------

/**
 * Validate that the recorded ledger is a contiguous prefix of
 * `SCIENT_MIGRATIONS` with exact name matches.
 *
 * - A recorded ID beyond the manifest means the database was migrated by a
 *   newer build; running older migrations against it must fail closed.
 * - A recorded ID that skips a manifest entry means the ledger was gapped or
 *   hand-modified; the high-water mark alone would silently ignore the hole.
 * - A recorded name that differs from the manifest means the ledger no longer
 *   describes the migrations this build would have run.
 */
const validateLedger = Effect.fn("validateScientLedger")(function* (sql: SqlClient.SqlClient) {
  const tables = yield* sql<{ readonly name: string }>`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'scient_schema_migrations'
  `;
  if (tables.length === 0) return; // Fresh database — nothing recorded yet.

  const rows = yield* sql<{ readonly migration_id: number; readonly name: string }>`
    SELECT migration_id, name FROM scient_schema_migrations ORDER BY migration_id
  `;

  const latestKnown = SCIENT_MIGRATIONS[SCIENT_MIGRATIONS.length - 1]!;
  for (const [index, row] of rows.entries()) {
    const expected = SCIENT_MIGRATIONS[index];
    if (!expected) {
      return yield* new ScientMigrationError({
        kind: "BadState",
        message:
          `Scient migration ledger records unknown migration ${row.migration_id} ("${row.name}") ` +
          `beyond the latest known migration ${latestKnown.id} ("${latestKnown.name}"). ` +
          `The database was migrated by a newer build; refusing to run this build's migrations against it.`,
      });
    }
    if (row.migration_id !== expected.id) {
      return yield* new ScientMigrationError({
        kind: "BadState",
        message:
          `Scient migration ledger is not a contiguous prefix of the manifest: ` +
          `position ${index + 1} records ${row.migration_id} ("${row.name}") but expected ` +
          `${expected.id} ("${expected.name}"). The ledger has a gap or was modified; refusing to continue.`,
      });
    }
    if (row.name !== expected.name) {
      return yield* new ScientMigrationError({
        kind: "BadState",
        message:
          `Scient migration ledger records migration ${row.migration_id} as "${row.name}" ` +
          `but the manifest names it "${expected.name}". The ledger was modified; refusing to continue.`,
      });
    }
  }
});

// ---------------------------------------------------------------------------
// Public runner
// ---------------------------------------------------------------------------

const migrator = Migrator.make({});

/**
 * Run all pending Scient schema migrations.
 *
 * 1. Reconcile a legacy `applied_at` ledger into the canonical shape.
 * 2. Validate ledger integrity (contiguous prefix, exact names, no unknown
 *    IDs).
 * 3. Delegate to the standard Effect SQL Migrator with the
 *    `scient_schema_migrations` table. The Migrator creates the ledger on
 *    fresh databases, inserts reservation rows for all pending migrations
 *    first (doubling as the concurrent-runner lock), runs each migration in
 *    ascending order inside one transaction, and treats a reservation
 *    conflict from a concurrent runner as a locked no-op.
 *
 * @returns Array of `[id, name]` tuples for migrations that were run.
 */
export const runScientMigrations = Effect.fn("runScientMigrations")(function* (
  sql: SqlClient.SqlClient,
) {
  yield* reconcileLedger(sql);
  yield* validateLedger(sql);

  const executed = yield* migrator({ loader, table: LEDGER_TABLE }).pipe(
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.mapError((error) =>
      error instanceof Migrator.MigrationError ? fromMigrationError(error) : error,
    ),
    // The standard Migrator dies with a MigrationError when a migration
    // effect fails; surface it as a typed ScientMigrationError instead.
    Effect.catchDefect((defect) =>
      defect instanceof Migrator.MigrationError
        ? Effect.fail(fromMigrationError(defect))
        : Effect.die(defect),
    ),
  );

  yield* executed.length === 0
    ? Effect.logDebug("Scient schema is current")
    : Effect.log("Scient migrations ran successfully").pipe(
        Effect.annotateLogs({
          migrations: executed.map(([id, name]) => `${id}_${name}`),
        }),
      );

  return executed;
});
