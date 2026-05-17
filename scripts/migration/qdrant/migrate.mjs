#!/usr/bin/env node
// Qdrant → NornicDB migration (Node).
//
// NornicDB exposes Qdrant compatibility on gRPC. The Node ecosystem does
// not have a maintained gRPC Qdrant client; the official
// `@qdrant/js-client-rest` talks to Qdrant's REST surface. This script
// reads from a source Qdrant via REST and writes into NornicDB via REST as
// well — NornicDB also exposes a REST surface on port 7474, separate from
// gRPC.
//
// If your NornicDB deployment uses the gRPC port for ingestion, prefer the
// Python or Go scripts in this directory.
//
// Install:
//   npm install @qdrant/js-client-rest
//
// Run:
//   node migrate.mjs \
//     --source-url http://qdrant.local:6333 \
//     --target-url http://nornicdb.local:7474 \
//     --batch-size 256
//
// Optional flags: --collections coll_a,coll_b   --skip-existing   --dry-run
//                 --source-api-key TOKEN        --target-api-key TOKEN

import { QdrantClient } from "@qdrant/js-client-rest";

function parseArgs(argv) {
  const args = {
    sourceUrl: "http://localhost:6333",
    targetUrl: "http://localhost:7474",
    sourceApiKey: undefined,
    targetApiKey: undefined,
    collections: "",
    batchSize: 256,
    skipExisting: false,
    dryRun: false,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    const next = () => argv[++i];
    switch (a) {
      case "--source-url":     args.sourceUrl = next(); break;
      case "--target-url":     args.targetUrl = next(); break;
      case "--source-api-key": args.sourceApiKey = next(); break;
      case "--target-api-key": args.targetApiKey = next(); break;
      case "--collections":    args.collections = next(); break;
      case "--batch-size":     args.batchSize = parseInt(next(), 10); break;
      case "--skip-existing":  args.skipExisting = true; break;
      case "--dry-run":        args.dryRun = true; break;
      default:
        console.error(`unknown flag: ${a}`);
        process.exit(2);
    }
  }
  return args;
}

async function collectionExists(client, name) {
  try {
    await client.getCollection(name);
    return true;
  } catch (e) {
    if (String(e?.status ?? "").startsWith("4")) return false;
    if (/not found|doesn't exist/i.test(String(e?.message ?? e))) return false;
    throw e;
  }
}

async function replicateCollectionConfig(src, dst, name, dryRun) {
  const info = await src.getCollection(name);
  const params = info.config.params;
  const vectorsConfig = params.vectors;

  console.log(`  → create collection "${name}"`);
  if (dryRun) return;
  await dst.createCollection(name, { vectors: vectorsConfig });
}

async function migratePoints(src, dst, name, batchSize, dryRun) {
  let total = 0;
  let offset;
  while (true) {
    const { points, next_page_offset } = await src.scroll(name, {
      limit: batchSize,
      offset,
      with_payload: true,
      with_vector: true,
    });
    if (!points || points.length === 0) break;

    const upserts = points.map((p) => ({
      id: p.id,
      vector: p.vector,
      payload: p.payload ?? {},
    }));

    if (!dryRun) {
      await dst.upsert(name, { wait: true, points: upserts });
    }
    total += upserts.length;
    console.log(`  · ${name}: upserted ${total} points`);

    if (!next_page_offset) break;
    offset = next_page_offset;
  }
  return total;
}

async function main() {
  const args = parseArgs(process.argv);

  const src = new QdrantClient({ url: args.sourceUrl, apiKey: args.sourceApiKey });
  const dst = new QdrantClient({ url: args.targetUrl, apiKey: args.targetApiKey });

  const requested = new Set(
    args.collections
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean),
  );

  const { collections } = await src.getCollections();
  let names = collections.map((c) => c.name);
  if (requested.size > 0) names = names.filter((n) => requested.has(n));

  if (names.length === 0) {
    console.error("No collections to migrate.");
    process.exit(1);
  }

  console.log(`Migrating ${names.length} collection(s): ${JSON.stringify(names)}`);
  const started = Date.now();

  for (const name of names) {
    console.log(`\n[${name}]`);
    const exists = await collectionExists(dst, name);
    if (exists && args.skipExisting) {
      console.log(`  · target already has "${name}", --skip-existing → skipping`);
      continue;
    }
    if (!exists) {
      await replicateCollectionConfig(src, dst, name, args.dryRun);
    }

    const srcCount = (await src.count(name, { exact: true })).count;
    const moved = await migratePoints(src, dst, name, args.batchSize, args.dryRun);

    if (!args.dryRun) {
      const dstCount = (await dst.count(name, { exact: true })).count;
      const marker = srcCount === dstCount ? "✓" : "✗";
      console.log(`  ${marker} source=${srcCount} target=${dstCount} (migrated ${moved})`);
      if (srcCount !== dstCount) {
        console.error("    !! count mismatch — investigate before proceeding");
      }
    }
  }

  console.log(`\nDone in ${((Date.now() - started) / 1000).toFixed(1)}s`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
