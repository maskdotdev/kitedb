/**
 * MVCC Performance Benchmark
 *
 * Compares performance with MVCC enabled vs disabled to verify no regression.
 *
 * Usage:
 *   bun run bench/benchmark-mvcc.ts [options]
 *
 * Options:
 *   --nodes N         Number of nodes (default: 10000)
 *   --edges M         Number of edges (default: 50000)
 *   --iterations I    Iterations for latency benchmarks (default: 10000)
 *   --output FILE     Output file path (default: bench/results/benchmark-mvcc-<timestamp>.txt)
 *   --no-output       Disable file output
 *   --keep-db         Keep the database directory after benchmark (prints path)
 */

import {
  appendFile,
  mkdir,
  mkdtemp,
  readdir,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import {
  type GraphDB,
  type NodeID,
  addEdge,
  beginTx,
  closeGraphDB,
  commit,
  createNode,
  defineEtype,
  edgeExists,
  getNeighborsIn,
  getNeighborsOut,
  getNodeByKey,
  openGraphDB,
  optimize,
  stats,
} from "../src/index.ts";

// =============================================================================
// Configuration
// =============================================================================

interface BenchConfig {
  nodes: number;
  edges: number;
  iterations: number;
  outputFile: string | null;
  keepDb: boolean;
}

function parseArgs(): BenchConfig {
  const args = process.argv.slice(2);

  // Generate default output filename with timestamp
  const now = new Date();
  const timestamp = now.toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const defaultOutput = join(
    dirname(import.meta.path),
    "results",
    `benchmark-mvcc-${timestamp}.txt`,
  );

  const config: BenchConfig = {
    nodes: 10000,
    edges: 50000,
    iterations: 10000,
    outputFile: defaultOutput,
    keepDb: false,
  };

  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case "--nodes":
        config.nodes = Number.parseInt(args[++i] || "10000", 10);
        break;
      case "--edges":
        config.edges = Number.parseInt(args[++i] || "50000", 10);
        break;
      case "--iterations":
        config.iterations = Number.parseInt(args[++i] || "10000", 10);
        break;
      case "--output":
        config.outputFile = args[++i] || null;
        break;
      case "--no-output":
        config.outputFile = null;
        break;
      case "--keep-db":
        config.keepDb = true;
        break;
    }
  }

  return config;
}

// =============================================================================
// Output Logger
// =============================================================================

class Logger {
  private outputFile: string | null;
  private buffer: string[] = [];

  constructor(outputFile: string | null) {
    this.outputFile = outputFile;
  }

  log(message = ""): void {
    console.log(message);
    this.buffer.push(message);
  }

  progress(message: string): void {
    process.stdout.write(message);
  }

  async flush(): Promise<void> {
    if (this.outputFile && this.buffer.length > 0) {
      await mkdir(dirname(this.outputFile), { recursive: true });
      await writeFile(this.outputFile, `${this.buffer.join("\n")}\n`);
    }
  }

  getOutputPath(): string | null {
    return this.outputFile;
  }
}

let logger: Logger;

// =============================================================================
// Latency Tracking
// =============================================================================

interface LatencyStats {
  count: number;
  min: number;
  max: number;
  sum: number;
  p50: number;
  p95: number;
  p99: number;
}

class LatencyTracker {
  private samples: number[] = [];

  record(latencyNs: number): void {
    this.samples.push(latencyNs);
  }

  getStats(): LatencyStats {
    if (this.samples.length === 0) {
      return { count: 0, min: 0, max: 0, sum: 0, p50: 0, p95: 0, p99: 0 };
    }

    const sorted = [...this.samples].sort((a, b) => a - b);
    const count = sorted.length;

    return {
      count,
      min: sorted[0]!,
      max: sorted[count - 1]!,
      sum: sorted.reduce((a, b) => a + b, 0),
      p50: sorted[Math.floor(count * 0.5)]!,
      p95: sorted[Math.floor(count * 0.95)]!,
      p99: sorted[Math.floor(count * 0.99)]!,
    };
  }

  clear(): void {
    this.samples = [];
  }
}

function formatLatency(ns: number): string {
  if (ns < 1000) return `${ns.toFixed(0)}ns`;
  if (ns < 1_000_000) return `${(ns / 1000).toFixed(2)}µs`;
  return `${(ns / 1_000_000).toFixed(2)}ms`;
}

function formatNumber(n: number): string {
  return n.toLocaleString("en-US");
}

// =============================================================================
// Graph Structure
// =============================================================================

interface GraphData {
  nodeIds: NodeID[];
  nodeKeys: string[];
  etypes: {
    calls: number;
  };
}

async function buildGraph(
  db: GraphDB,
  config: BenchConfig,
): Promise<GraphData> {
  const nodeIds: NodeID[] = [];
  const nodeKeys: string[] = [];
  const batchSize = 5000;
  let etypes: GraphData["etypes"] | undefined;

  logger.progress("  Creating nodes...");
  for (let batch = 0; batch < config.nodes; batch += batchSize) {
    const tx = beginTx(db);

    if (batch === 0) {
      etypes = {
        calls: defineEtype(tx, "CALLS"),
      };
    }

    const end = Math.min(batch + batchSize, config.nodes);
    for (let i = batch; i < end; i++) {
      const key = `pkg.module${Math.floor(i / 100)}.Class${i % 100}`;
      const nodeId = createNode(tx, { key });
      nodeIds.push(nodeId);
      nodeKeys.push(key);
    }
    await commit(tx);
    logger.progress(`\r  Created ${end} / ${config.nodes} nodes`);
  }
  logger.log();

  logger.progress("  Creating edges...");
  let edgesCreated = 0;
  const maxAttempts = config.edges * 3;

  while (edgesCreated < config.edges) {
    const tx = beginTx(db);
    const batchTarget = Math.min(edgesCreated + batchSize, config.edges);

    while (edgesCreated < batchTarget && edgesCreated < maxAttempts) {
      const src = nodeIds[Math.floor(Math.random() * nodeIds.length)]!;
      const dst = nodeIds[Math.floor(Math.random() * nodeIds.length)]!;

      if (src !== dst) {
        addEdge(tx, src, etypes!.calls, dst);
        edgesCreated++;
      }
    }
    await commit(tx);
    logger.progress(`\r  Created ${edgesCreated} / ${config.edges} edges`);
  }
  logger.log();

  return {
    nodeIds,
    nodeKeys,
    etypes: etypes!,
  };
}

// =============================================================================
// Benchmark Reporting
// =============================================================================

function printLatencyTable(name: string, stats: LatencyStats): void {
  const opsPerSec =
    stats.sum > 0 ? stats.count / (stats.sum / 1_000_000_000) : 0;
  logger.log(
    `${name.padEnd(45)} p50=${formatLatency(stats.p50).padStart(10)} p95=${formatLatency(stats.p95).padStart(10)} p99=${formatLatency(stats.p99).padStart(10)} max=${formatLatency(stats.max).padStart(10)} (${formatNumber(Math.round(opsPerSec))} ops/sec)`,
  );
}

function compareResults(
  name: string,
  baseline: LatencyStats,
  mvcc: LatencyStats,
): void {
  const overhead = ((mvcc.p50 - baseline.p50) / baseline.p50) * 100;
  const overhead95 = ((mvcc.p95 - baseline.p95) / baseline.p95) * 100;
  const overhead99 = ((mvcc.p99 - baseline.p99) / baseline.p99) * 100;

  logger.log(`\n${name}:`);
  logger.log(
    `  Baseline p50: ${formatLatency(baseline.p50)}, MVCC p50: ${formatLatency(mvcc.p50)}, Overhead: ${overhead.toFixed(2)}%`,
  );
  logger.log(
    `  Baseline p95: ${formatLatency(baseline.p95)}, MVCC p95: ${formatLatency(mvcc.p95)}, Overhead: ${overhead95.toFixed(2)}%`,
  );
  logger.log(
    `  Baseline p99: ${formatLatency(baseline.p99)}, MVCC p99: ${formatLatency(mvcc.p99)}, Overhead: ${overhead99.toFixed(2)}%`,
  );

  if (overhead > 10) {
    logger.log(`  ⚠️  WARNING: Overhead exceeds 10% threshold!`);
  } else {
    logger.log(`  ✓ Overhead within acceptable range (<10%)`);
  }
}

// =============================================================================
// Benchmark Functions
// =============================================================================

function benchmarkKeyLookups(
  db: GraphDB,
  graph: GraphData,
  iterations: number,
): LatencyStats {
  const tracker = new LatencyTracker();
  for (let i = 0; i < iterations; i++) {
    const key =
      graph.nodeKeys[Math.floor(Math.random() * graph.nodeKeys.length)]!;
    const start = Bun.nanoseconds();
    getNodeByKey(db, key);
    tracker.record(Bun.nanoseconds() - start);
  }
  return tracker.getStats();
}

function benchmarkTraversals(
  db: GraphDB,
  graph: GraphData,
  iterations: number,
): LatencyStats {
  const tracker = new LatencyTracker();
  for (let i = 0; i < iterations; i++) {
    const node =
      graph.nodeIds[Math.floor(Math.random() * graph.nodeIds.length)]!;
    const start = Bun.nanoseconds();
    let count = 0;
    for (const _ of getNeighborsOut(db, node)) count++;
    tracker.record(Bun.nanoseconds() - start);
  }
  return tracker.getStats();
}

function benchmarkEdgeExists(
  db: GraphDB,
  graph: GraphData,
  iterations: number,
): LatencyStats {
  const tracker = new LatencyTracker();
  for (let i = 0; i < iterations; i++) {
    const src =
      graph.nodeIds[Math.floor(Math.random() * graph.nodeIds.length)]!;
    const dst =
      graph.nodeIds[Math.floor(Math.random() * graph.nodeIds.length)]!;
    const start = Bun.nanoseconds();
    edgeExists(db, src, graph.etypes.calls, dst);
    tracker.record(Bun.nanoseconds() - start);
  }
  return tracker.getStats();
}

async function benchmarkWrites(
  db: GraphDB,
  iterations: number,
): Promise<LatencyStats> {
  const tracker = new LatencyTracker();
  const batchSize = 100;

  for (let i = 0; i < Math.min(iterations, 100); i++) {
    const start = Bun.nanoseconds();
    const tx = beginTx(db);
    for (let j = 0; j < batchSize; j++) {
      createNode(tx, { key: `bench:mvcc:${i}:${j}` });
    }
    await commit(tx);
    tracker.record(Bun.nanoseconds() - start);
  }

  return tracker.getStats();
}

async function benchmarkTransactionLatency(
  db: GraphDB,
  iterations: number,
): Promise<LatencyStats> {
  const tracker = new LatencyTracker();

  for (let i = 0; i < iterations; i++) {
    const start = Bun.nanoseconds();
    const tx = beginTx(db);
    createNode(tx, { key: `tx:${i}` });
    await commit(tx);
    tracker.record(Bun.nanoseconds() - start);
  }

  return tracker.getStats();
}

async function benchmarkConcurrentTransactions(
  db: GraphDB,
  iterations: number,
): Promise<number> {
  const start = performance.now();
  const promises: Promise<void>[] = [];

  for (let i = 0; i < iterations; i++) {
    promises.push(
      (async () => {
        const tx = beginTx(db);
        createNode(tx, { key: `concurrent:${i}` });
        await commit(tx);
      })(),
    );
  }

  await Promise.all(promises);
  const elapsed = performance.now() - start;

  return (iterations / elapsed) * 1000; // transactions per second
}

// =============================================================================
// Main Benchmark Runner
// =============================================================================

async function runBenchmarks(config: BenchConfig, mvccEnabled: boolean) {
  const testDir = await mkdtemp(join(tmpdir(), `ray-bench-${mvccEnabled ? "mvcc" : "baseline"}-`));

  try {
    logger.log(`\n[${mvccEnabled ? "MVCC" : "Baseline"}] Building graph...`);
    const db = await openGraphDB(testDir, { mvcc: mvccEnabled });
    const startBuild = performance.now();
    const graph = await buildGraph(db, config);
    logger.log(`  Built in ${(performance.now() - startBuild).toFixed(0)}ms`);

    logger.log(`\n[${mvccEnabled ? "MVCC" : "Baseline"}] Compacting...`);
    const startCompact = performance.now();
    await optimize(db);
    logger.log(
      `  Compacted in ${(performance.now() - startCompact).toFixed(0)}ms`,
    );

    const dbStats = stats(db);
    logger.log(
      `  Snapshot: ${formatNumber(Number(dbStats.snapshotNodes))} nodes, ${formatNumber(Number(dbStats.snapshotEdges))} edges`,
    );

    if (mvccEnabled && dbStats.mvccStats) {
      logger.log(
        `  MVCC Stats: ${dbStats.mvccStats.activeTransactions} active transactions, ${formatNumber(Number(dbStats.mvccStats.versionsPruned))} versions pruned`,
      );
    }

    logger.log(`\n[${mvccEnabled ? "MVCC" : "Baseline"}] Key lookup benchmarks...`);
    const keyLookupStats = benchmarkKeyLookups(db, graph, config.iterations);
    printLatencyTable("Key lookups", keyLookupStats);

    logger.log(`\n[${mvccEnabled ? "MVCC" : "Baseline"}] Traversal benchmarks...`);
    const traversalStats = benchmarkTraversals(db, graph, config.iterations);
    printLatencyTable("Traversals", traversalStats);

    logger.log(`\n[${mvccEnabled ? "MVCC" : "Baseline"}] Edge existence benchmarks...`);
    const edgeExistsStats = benchmarkEdgeExists(db, graph, config.iterations);
    printLatencyTable("Edge exists", edgeExistsStats);

    logger.log(`\n[${mvccEnabled ? "MVCC" : "Baseline"}] Write benchmarks...`);
    const writeStats = await benchmarkWrites(db, config.iterations);
    printLatencyTable("Batch writes (100 nodes)", writeStats);

    if (mvccEnabled) {
      logger.log(`\n[MVCC] Transaction latency benchmarks...`);
      const txLatencyStats = await benchmarkTransactionLatency(db, config.iterations);
      printLatencyTable("Transaction begin+commit", txLatencyStats);

      logger.log(`\n[MVCC] Concurrent transaction throughput...`);
      const concurrentTps = await benchmarkConcurrentTransactions(db, 100);
      logger.log(`  Concurrent transactions: ${concurrentTps.toFixed(0)} tx/sec`);
    }

    await closeGraphDB(db);

    return {
      keyLookupStats,
      traversalStats,
      edgeExistsStats,
      writeStats,
    };
  } finally {
    if (config.keepDb) {
      logger.log(`\nDatabase preserved at: ${testDir}`);
    } else {
      await rm(testDir, { recursive: true, force: true });
    }
  }
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  const config = parseArgs();

  // Initialize logger
  logger = new Logger(config.outputFile);

  const now = new Date();
  logger.log("=".repeat(120));
  logger.log("MVCC Performance Benchmark - Regression Testing");
  logger.log("=".repeat(120));
  logger.log(`Date: ${now.toISOString()}`);
  logger.log(`Nodes: ${formatNumber(config.nodes)}`);
  logger.log(`Edges: ${formatNumber(config.edges)}`);
  logger.log(`Iterations: ${formatNumber(config.iterations)}`);
  logger.log("=".repeat(120));

  // Run baseline (no MVCC)
  logger.log("\n" + "=".repeat(120));
  logger.log("BASELINE (No MVCC)");
  logger.log("=".repeat(120));
  const baseline = await runBenchmarks(config, false);

  // Run MVCC
  logger.log("\n" + "=".repeat(120));
  logger.log("MVCC ENABLED");
  logger.log("=".repeat(120));
  const mvcc = await runBenchmarks(config, true);

  // Compare results
  logger.log("\n" + "=".repeat(120));
  logger.log("PERFORMANCE COMPARISON");
  logger.log("=".repeat(120));
  compareResults("Key Lookups", baseline.keyLookupStats, mvcc.keyLookupStats);
  compareResults("Traversals", baseline.traversalStats, mvcc.traversalStats);
  compareResults("Edge Exists", baseline.edgeExistsStats, mvcc.edgeExistsStats);
  compareResults("Batch Writes", baseline.writeStats, mvcc.writeStats);

  logger.log(`\n${"=".repeat(120)}`);
  logger.log("Benchmark complete.");
  logger.log("=".repeat(120));

  // Write results to file
  await logger.flush();
  if (logger.getOutputPath()) {
    console.log(`\nResults saved to: ${logger.getOutputPath()}`);
  }
}

main().catch((err) => {
  console.error("Benchmark failed:", err);
  process.exit(1);
});

