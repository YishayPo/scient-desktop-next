import type { ProjectReadFileResult } from "@t3tools/contracts";

/**
 * A refreshed query can arrive while an optimistic editor buffer is masking
 * it. Only different contents at a different revision are a real conflict;
 * the file watcher also observes Scient's own successful atomic save.
 */
export function hasExternalFileConflict(input: {
  readonly authoritative: ProjectReadFileResult | null;
  readonly optimistic: ProjectReadFileResult | null;
  readonly lastConfirmedSave?: { readonly contents: string; readonly revision: string } | null;
  readonly pending: boolean;
}): boolean {
  return (
    input.pending &&
    input.authoritative !== null &&
    input.optimistic !== null &&
    !(
      input.lastConfirmedSave?.revision === input.authoritative.revision &&
      input.lastConfirmedSave.contents === input.authoritative.contents
    ) &&
    input.authoritative.revision !== input.optimistic.revision &&
    input.authoritative.contents !== input.optimistic.contents
  );
}
