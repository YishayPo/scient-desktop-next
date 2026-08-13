import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/** Rebuildable projection over canonical analysis run receipts on disk. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE scient_analysis_run_index (
      project_id TEXT NOT NULL,
      run_id TEXT NOT NULL,
      relative_path TEXT NOT NULL,
      started_at TEXT NOT NULL,
      status TEXT NOT NULL,
      summary_json TEXT NOT NULL,
      PRIMARY KEY (project_id, run_id)
    )
  `;
  yield* sql`
    CREATE INDEX scient_analysis_run_index_project_started
    ON scient_analysis_run_index(project_id, started_at DESC, run_id DESC)
  `;
  yield* sql`
    CREATE INDEX scient_analysis_run_index_file_started
    ON scient_analysis_run_index(project_id, relative_path, started_at DESC, run_id DESC)
  `;
});
