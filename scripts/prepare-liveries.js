#!/usr/bin/env node

const fs = require("fs/promises");
const path = require("path");

const defaultSourceDir = path.join(
  process.env.HOME || "",
  "Downloads",
  "Airline-Liveries-535-v.3"
);
const sourceDir = path.resolve(process.argv[2] || defaultSourceDir);
const targetDir = path.resolve(
  process.argv[3] || path.join(__dirname, "..", "assets", "liveries")
);
const imageExtensions = new Set([".png", ".jpg", ".jpeg"]);

async function main() {
  const candidates = await collectCandidates(sourceDir);
  const selected = selectBestByAirlineCode(candidates);

  if (selected.size === 0) {
    console.log(`No livery images found in ${sourceDir}`);
    return;
  }

  await fs.mkdir(targetDir, { recursive: true });
  await cleanTargetDir(targetDir);

  const manifest = [];
  for (const [code, candidate] of [...selected.entries()].sort()) {
    const extension = path.extname(candidate.file).toLowerCase();
    const destination = path.join(targetDir, `${code}${extension}`);
    await fs.copyFile(candidate.file, destination);
    manifest.push({
      code,
      file: path.basename(destination),
      source: path.relative(sourceDir, candidate.file).split(path.sep).join("/")
    });
  }

  await fs.writeFile(
    path.join(targetDir, "manifest.json"),
    `${JSON.stringify(manifest, null, 2)}\n`
  );

  console.log(`Prepared ${manifest.length} livery image(s) in ${targetDir}`);
}

async function collectCandidates(dir) {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  const candidates = [];

  for (const entry of entries) {
    if (entry.name.startsWith(".")) {
      continue;
    }

    if (entry.isDirectory() && entry.name === "` Flat versions") {
      continue;
    }

    const entryPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      candidates.push(...await collectCandidates(entryPath));
      continue;
    }

    if (!entry.isFile()) {
      continue;
    }

    const extension = path.extname(entry.name).toLowerCase();
    if (!imageExtensions.has(extension)) {
      continue;
    }

    const match = entry.name.match(/^([A-Z0-9]{2})_/);
    if (!match) {
      continue;
    }

    candidates.push({
      code: match[1].toUpperCase(),
      file: entryPath,
      score: scoreCandidate(entryPath, extension)
    });
  }

  return candidates;
}

function selectBestByAirlineCode(candidates) {
  const selected = new Map();

  for (const candidate of candidates) {
    const current = selected.get(candidate.code);
    if (
      !current ||
      candidate.score > current.score ||
      (candidate.score === current.score && candidate.file.localeCompare(current.file) < 0)
    ) {
      selected.set(candidate.code, candidate);
    }
  }

  return selected;
}

function scoreCandidate(file, extension) {
  let score = 0;
  const normalized = file.split(path.sep).join("/");

  if (!/\/Ceased Operations?\//i.test(normalized)) {
    score += 20;
  }

  if (extension === ".png") {
    score += 10;
  } else if (extension === ".jpg" || extension === ".jpeg") {
    score += 5;
  }

  return score;
}

async function cleanTargetDir(dir) {
  const entries = await fs.readdir(dir, { withFileTypes: true });

  for (const entry of entries) {
    if (!entry.isFile()) {
      continue;
    }

    const entryPath = path.join(dir, entry.name);
    const extension = path.extname(entry.name).toLowerCase();
    if (imageExtensions.has(extension) || entry.name === "manifest.json") {
      await fs.unlink(entryPath);
    }
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
