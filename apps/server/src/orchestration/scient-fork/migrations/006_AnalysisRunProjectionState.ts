import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/** Durable rebuild state and queryable support fields for the analysis receipt projection. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`ALTER TABLE scient_analysis_run_index ADD COLUMN source_revision TEXT NOT NULL DEFAULT ''`;
  yield* sql`ALTER TABLE scient_analysis_run_index ADD COLUMN runtime_id TEXT NOT NULL DEFAULT ''`;
  yield* sql`ALTER TABLE scient_analysis_run_index ADD COLUMN runtime_release TEXT`;
  yield* sql`ALTER TABLE scient_analysis_run_index ADD COLUMN action TEXT NOT NULL DEFAULT 'run-file'`;
  yield* sql`ALTER TABLE scient_analysis_run_index ADD COLUMN finished_at TEXT`;
  yield* sql`ALTER TABLE scient_analysis_run_index ADD COLUMN artifact_count INTEGER NOT NULL DEFAULT 0`;
  yield* sql`ALTER TABLE scient_analysis_run_index ADD COLUMN diagnostic_count INTEGER NOT NULL DEFAULT 0`;
  yield* sql`ALTER TABLE scient_analysis_run_index ADD COLUMN retained_output_bytes INTEGER NOT NULL DEFAULT 0`;
  yield* sql`ALTER TABLE scient_analysis_run_index ADD COLUMN retained_artifact_bytes INTEGER NOT NULL DEFAULT 0`;
  yield* sql`ALTER TABLE scient_analysis_run_index ADD COLUMN retained_bytes INTEGER NOT NULL DEFAULT 0`;
  yield* sql`
    CREATE TABLE scient_analysis_run_index_state (
      project_id TEXT PRIMARY KEY NOT NULL,
      revision INTEGER NOT NULL,
      clean INTEGER NOT NULL CHECK (clean IN (0, 1)),
      indexed_at TEXT
    )
  `;
});
