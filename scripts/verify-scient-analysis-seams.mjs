#!/usr/bin/env node

import * as NodeFS from "node:fs";

const MANIFEST_PATH = "scient-analysis-seams.json";

export function verifyScientAnalysisSeams() {
  const manifest = JSON.parse(NodeFS.readFileSync(MANIFEST_PATH, "utf8"));
  const failures = [];
  if (manifest.schemaVersion !== 1) failures.push("unsupported seam manifest schemaVersion");
  if (manifest.owner !== "ScientFactory")
    failures.push("seam manifest owner must be ScientFactory");

  for (const path of [...(manifest.ownedRoots ?? []), ...(manifest.ownedFiles ?? [])]) {
    if (!NodeFS.existsSync(path)) failures.push(`owned analysis path does not exist: ${path}`);
  }

  const mountPaths = new Set();
  for (const mount of manifest.upstreamMounts ?? []) {
    if (mountPaths.has(mount.path)) failures.push(`duplicate analysis mount: ${mount.path}`);
    mountPaths.add(mount.path);
    if (!NodeFS.existsSync(mount.path)) {
      failures.push(`analysis mount does not exist: ${mount.path}`);
      continue;
    }
    if (!NodeFS.readFileSync(mount.path, "utf8").includes(mount.anchor)) {
      failures.push(`analysis mount ${mount.path} is missing anchor ${mount.anchor}`);
    }
  }

  if (failures.length > 0) {
    for (const failure of failures) process.stderr.write(`${failure}\n`);
    process.exitCode = 1;
    return;
  }
  process.stdout.write(
    `Scient analysis seam check passed: ${String(manifest.ownedRoots.length)} owned roots, ${String(manifest.ownedFiles.length)} owned files, ${String(mountPaths.size)} upstream mounts.\n`,
  );
}

if (import.meta.main) verifyScientAnalysisSeams();
