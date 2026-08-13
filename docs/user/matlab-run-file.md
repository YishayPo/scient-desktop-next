# Run a MATLAB file

Status: candidate behavior under review; not yet a released Scient capability.

Scient can open and edit a project-owned `.m` file even when MATLAB is not installed. When a
user-installed MATLAB is available on the same environment as the Scient server, the file viewer
adds a compact **Run file** panel.

## Use the candidate

1. Open an initialized Scient project and select a text `.m` file.
2. Wait for any pending save to finish. Scient runs the exact saved revision, not an unsaved or
   stale buffer.
3. If MATLAB was not found, choose **Set up MATLAB**, enter the path to the MATLAB executable, and
   choose **Use path**. You can also ask Scient to scan again.
4. **Verify** is optional: use it to check that MATLAB can actually start before a run or to
   troubleshoot setup. Scient reports the detected
   release, architecture, Java runtime, and toolbox count, or explains whether sign-in, licensing,
   a missing local dependency, startup failure, or a timeout prevented readiness. **Details**
   also lets you change the executable or scan again.
5. Choose **Run file**. Standard output, errors, status, a local per-file run history, and any
   captured figures appear in the panel.
6. If another MATLAB run is active, the new run waits in the runtime queue and shows its position.
   **Stop** cancels a waiting run without launching it, or stops the active MATLAB process tree.
7. Select a figure and choose its reliable static view or floating card from the compact menu.
   When a future producer supplies an interactive representation, the same menu can expose it
   without changing the artifact contract. You can also drag the thumbnail directly into the chat
   to create a floating card at the drop point. Floating cards can be moved and resized from every
   edge and corner. Download the FIG representation when you want to continue working in MATLAB.
8. Select **Load older runs** to page through long local history without loading every receipt or
   output journal into memory.
9. Use **Remove this run** or **Clean up project** when retained output and figure files are no
   longer needed. Scient confirms the measured byte count and keeps status, diagnostics, hashes,
   source/runtime provenance, and artifact descriptions in metadata-only history.

On macOS, a typical executable is
`/Applications/MATLAB_R2025b.app/bin/matlab`. Windows and Linux paths depend on the installed
release. Scient uses the user's installed and licensed MATLAB; it does not bundle MATLAB or grant
a license.

Viewing and editing never launches MATLAB. A missing or invalid runtime is a setup state, not a
file-viewer failure. A folder opened without Scient project setup remains viewable and editable,
but Run file stays disabled until the folder is set up. Output is capped with an explicit
truncation notice, and an interrupted run is shown as lost after Scient restarts.

When MATLAB reports an `MException`, Scient shows the structured error separately from raw output.
Click a project-local file and line to open it, or choose **Ask agent** to place a focused repair
request in the chat composer for review before sending.

If another program or an agent changes the open file, Scient rereads it automatically. When local
edits are still pending, Scient does not silently replace either version: choose whether to reload
the disk version or explicitly overwrite it with the local edits. Figure collection failure is
reported separately, so a successful MATLAB calculation is not mislabeled as failed and a broken
capture is not mistaken for “this run produced no figures.”

This first candidate runs a complete `.m` file in noninteractive batch mode. Selection/section
execution, variables, optional SVG/PDF/interactive figure exports, `.mlx` and `.mat` viewers,
debugging, MATLAB tests, notebooks, agent/MCP initiation, remote runtimes, and automatic retention
policies remain future work.
