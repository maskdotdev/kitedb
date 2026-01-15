import { openGraphDB, closeGraphDB, beginTx, createNode, defineEtype, commit, stats } from "./src/index.ts";
import { isCheckpointRunning, getCheckpointState } from "./src/ray/graph-db/checkpoint.ts";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { stat, rm } from "node:fs/promises";
import { existsSync } from "node:fs";

const filePath = join(tmpdir(), `ray-bench-sf-${Date.now()}.raydb`);
console.log("Database path:", filePath);

try {
  const db = await openGraphDB(filePath, {
    autoCheckpoint: true,
    checkpointThreshold: 0.8,
  });
  console.log("Database opened");

  // Define edge types like benchmark
  const tx0 = beginTx(db);
  const etypes = {
    calls: defineEtype(tx0, "CALLS"),
    references: defineEtype(tx0, "REFERENCES"),
    imports: defineEtype(tx0, "IMPORTS"),
    extends: defineEtype(tx0, "EXTENDS"),
  };
  await commit(tx0);

  // Create nodes in batches - EXACTLY like the benchmark
  const batchSize = 5000;
  const totalNodes = 20000;
  const nodeIds: number[] = [];
  const nodeKeys: string[] = [];
  const outDegree = new Map<number, number>();
  const inDegree = new Map<number, number>();

  console.log("  Creating nodes...");
  for (let batch = 0; batch < totalNodes; batch += batchSize) {
    const tx = beginTx(db);
    const end = Math.min(batch + batchSize, totalNodes);
    
    for (let i = batch; i < end; i++) {
      const key = `pkg.module${Math.floor(i / 100)}.Class${i % 100}`;
      const nodeId = createNode(tx, { key });
      nodeIds.push(nodeId);
      nodeKeys.push(key);
      outDegree.set(nodeId, 0);
      inDegree.set(nodeId, 0);
    }
    
    // Progress output like benchmark
    process.stdout.write(`\r  Created ${end} / ${totalNodes} nodes`);
    
    const cpState = getCheckpointState(db);
    console.log(`  [Pre-commit] checkpoint state: ${cpState.status}, file exists: ${existsSync(filePath)}`);
    
    await commit(tx);
    
    console.log(`  [Post-commit] file exists: ${existsSync(filePath)}`);
  }
  console.log();

  console.log("\nAll node creation completed successfully");
  console.log("Stats:", stats(db));
  await closeGraphDB(db);
  
} catch (e: any) {
  console.error("\nError:", e.message);
  console.log("File exists after error:", existsSync(filePath));
} finally {
  if (existsSync(filePath)) {
    const s = await stat(filePath);
    console.log("Final file size:", s.size);
    await rm(filePath, { force: true });
  }
}
