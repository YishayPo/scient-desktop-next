import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/** Queryable retention status for explicit, metadata-preserving analysis cleanup. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`ALTER TABLE scient_analysis_run_index ADD COLUMN storage_status TEXT NOT NULL DEFAULT 'retained'`;
  yield* sql`
    CREATE INDEX scient_analysis_run_index_storage
    ON scient_analysis_run_index(project_id, storage_status, status)
  `;
});
