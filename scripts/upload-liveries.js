#!/usr/bin/env node

const fs = require("fs/promises");
const path = require("path");
const dotenv = require("dotenv");

dotenv.config({ path: path.join(__dirname, "..", ".env") });

const bucket = "flight-liveries";
const defaultSourceDir = path.join(__dirname, "..", "assets", "liveries");
const sourceDir = path.resolve(process.argv[2] || defaultSourceDir);
const supabaseURL = String(process.env.SUPABASE_URL || "").trim().replace(/\/+$/, "");
const serviceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY || "").trim();
const allowedExtensions = new Map([
  [".webp", "image/webp"],
  [".png", "image/png"],
  [".jpg", "image/jpeg"],
  [".jpeg", "image/jpeg"]
]);

async function main() {
  if (!supabaseURL) {
    throw new Error("SUPABASE_URL is required in .env");
  }

  if (!serviceRoleKey) {
    throw new Error("SUPABASE_SERVICE_ROLE_KEY is required in .env");
  }

  const files = await collectImageFiles(sourceDir);
  if (files.length === 0) {
    console.log(`No livery images found in ${sourceDir}`);
    return;
  }

  console.log(`Uploading ${files.length} livery image(s) to ${bucket}...`);

  const uploaded = [];
  for (const file of files) {
    const relativePath = path.relative(sourceDir, file).split(path.sep).join("/");
    const extension = path.extname(file).toLowerCase();
    const contentType = allowedExtensions.get(extension);
    const data = await fs.readFile(file);
    const objectPath = encodeObjectPath(relativePath);
    const uploadURL = `${supabaseURL}/storage/v1/object/${bucket}/${objectPath}`;

    const response = await fetch(uploadURL, {
      method: "POST",
      headers: {
        apikey: serviceRoleKey,
        authorization: `Bearer ${serviceRoleKey}`,
        "content-type": contentType,
        "x-upsert": "true",
        "cache-control": "public, max-age=31536000, immutable"
      },
      body: data
    });

    if (!response.ok) {
      const body = await response.text();
      throw new Error(`Upload failed for ${relativePath}: ${response.status} ${body}`);
    }

    const publicURL = `${supabaseURL}/storage/v1/object/public/${bucket}/${objectPath}`;
    uploaded.push({ path: relativePath, publicURL });
    console.log(`Uploaded ${relativePath}`);
  }

  console.log("\nPublic URLs:");
  for (const item of uploaded) {
    console.log(`${item.path}: ${item.publicURL}`);
  }
}

async function collectImageFiles(dir) {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    if (entry.name.startsWith(".")) {
      continue;
    }

    const entryPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...await collectImageFiles(entryPath));
      continue;
    }

    if (!entry.isFile()) {
      continue;
    }

    const extension = path.extname(entry.name).toLowerCase();
    if (allowedExtensions.has(extension)) {
      files.push(entryPath);
    }
  }

  return files.sort((a, b) => a.localeCompare(b));
}

function encodeObjectPath(value) {
  return value
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
}

main().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
