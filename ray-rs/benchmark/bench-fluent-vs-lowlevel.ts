/**
 * KiteDB TypeScript Fluent API vs Low-Level API Benchmark
 *
 * Compares the fluent API (from ts/index.ts) against the low-level native bindings
 * to measure the overhead of the TypeScript wrapper layer.
 *
 * Usage:
 *   npx tsx benchmark/bench-fluent-vs-lowlevel.ts [options]
 *
 * Options:
 *   --nodes N         Number of nodes (default: 1000)
 *   --edges M         Number of edges (default: 5000)
 *   --iterations I    Iterations for latency benchmarks (default: 1000)
 */

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Bench } from "tinybench";

// Low-level API (native bindings)
import {
	Database,
	JsTraversalDirection,
	PropType,
	pathConfig,
	traversalStep,
} from "../index.js";

// Fluent API (TypeScript wrapper)
import { edge, int, kiteSync, node, optional, string } from "../dist/index.js";

// =============================================================================
// Configuration
// =============================================================================

interface BenchConfig {
	nodes: number;
	edges: number;
	iterations: number;
}

function parseArgs(): BenchConfig {
	const args = process.argv.slice(2);
	const config: BenchConfig = {
		nodes: 1000,
		edges: 5000,
		iterations: 1000,
	};

	for (let i = 0; i < args.length; i++) {
		if (args[i] === "--nodes" && args[i + 1]) {
			config.nodes = Number.parseInt(args[i + 1], 10);
			i++;
		} else if (args[i] === "--edges" && args[i + 1]) {
			config.edges = Number.parseInt(args[i + 1], 10);
			i++;
		} else if (args[i] === "--iterations" && args[i + 1]) {
			config.iterations = Number.parseInt(args[i + 1], 10);
			i++;
		}
	}

	return config;
}

// =============================================================================
// Schema Definition (for fluent API)
// =============================================================================

const User = node("user", {
	key: (id: string) => `user:${id}`,
	props: {
		name: string("name"),
		email: string("email"),
		age: optional(int("age")),
	},
});

const knows = edge("knows", {
	since: int("since"),
});

// =============================================================================
// Helpers
// =============================================================================

function makeTempDir(): string {
	return fs.mkdtempSync(path.join(os.tmpdir(), "kitedb-bench-"));
}

function formatNumber(n: number): string {
	return n.toLocaleString();
}

function formatLatency(ns: number): string {
	if (ns < 1000) return `${ns.toFixed(0)}ns`;
	if (ns < 1_000_000) return `${(ns / 1000).toFixed(2)}us`;
	return `${(ns / 1_000_000).toFixed(2)}ms`;
}

// =============================================================================
// Latency Tracking
// =============================================================================

interface LatencyStats {
	count: number;
	minNs: number;
	maxNs: number;
	sumNs: number;
	p50: number;
	p95: number;
	p99: number;
	avgNs: number;
	opsPerSec: number;
}

class LatencyTracker {
	private samples: number[] = [];

	record(latencyNs: number): void {
		this.samples.push(latencyNs);
	}

	getStats(): LatencyStats {
		if (this.samples.length === 0) {
			return {
				count: 0,
				minNs: 0,
				maxNs: 0,
				sumNs: 0,
				p50: 0,
				p95: 0,
				p99: 0,
				avgNs: 0,
				opsPerSec: 0,
			};
		}

		const sorted = [...this.samples].sort((a, b) => a - b);
		const count = sorted.length;
		const sumNs = sorted.reduce((a, b) => a + b, 0);

		return {
			count,
			minNs: sorted[0],
			maxNs: sorted[count - 1],
			sumNs,
			p50: sorted[Math.floor(count * 0.5)],
			p95: sorted[Math.floor(count * 0.95)],
			p99: sorted[Math.floor(count * 0.99)],
			avgNs: sumNs / count,
			opsPerSec: count / (sumNs / 1_000_000_000),
		};
	}
}

function printComparison(
	name: string,
	lowLevel: LatencyStats,
	fluent: LatencyStats,
): void {
	const overhead = lowLevel.p50 > 0 ? fluent.p50 / lowLevel.p50 : 0;
	console.log(
		`${name.padEnd(40)} low-level p50=${formatLatency(lowLevel.p50).padStart(10)}  fluent p50=${formatLatency(fluent.p50).padStart(10)}  overhead=${overhead.toFixed(2)}x`,
	);
}

// =============================================================================
// Benchmark Functions
// =============================================================================

function benchmarkLowLevelInserts(
	db: Database,
	config: BenchConfig,
): LatencyStats {
	const tracker = new LatencyTracker();
	const nameKey = db.getOrCreatePropkey("name");
	const emailKey = db.getOrCreatePropkey("email");
	const ageKey = db.getOrCreatePropkey("age");

	for (let i = 0; i < config.iterations; i++) {
		const start = process.hrtime.bigint();
		db.begin();
		const nodeId = db.createNode(`bench:low:${i}`);
		db.setNodeProp(nodeId, nameKey, {
			propType: PropType.String,
			stringValue: `User ${i}`,
		});
		db.setNodeProp(nodeId, emailKey, {
			propType: PropType.String,
			stringValue: `user${i}@example.com`,
		});
		db.setNodeProp(nodeId, ageKey, {
			propType: PropType.Int,
			intValue: 20 + (i % 50),
		});
		db.commit();
		tracker.record(Number(process.hrtime.bigint() - start));
	}

	return tracker.getStats();
}

function benchmarkFluentInserts(
	db: ReturnType<typeof kiteSync>,
	config: BenchConfig,
): LatencyStats {
	const tracker = new LatencyTracker();

	for (let i = 0; i < config.iterations; i++) {
		const start = process.hrtime.bigint();
		db.insert(User)
			.values(`bench:fluent:${i}`, {
				name: `User ${i}`,
				email: `user${i}@example.com`,
				age: 20 + (i % 50),
			})
			.returning();
		tracker.record(Number(process.hrtime.bigint() - start));
	}

	return tracker.getStats();
}

function benchmarkLowLevelKeyLookup(
	db: Database,
	keys: string[],
	iterations: number,
): LatencyStats {
	const tracker = new LatencyTracker();

	for (let i = 0; i < iterations; i++) {
		const key = keys[Math.floor(Math.random() * keys.length)];
		const start = process.hrtime.bigint();
		db.getNodeByKey(key);
		tracker.record(Number(process.hrtime.bigint() - start));
	}

	return tracker.getStats();
}

function benchmarkFluentKeyLookup(
	db: ReturnType<typeof kiteSync>,
	keyArgs: string[],
	iterations: number,
): LatencyStats {
	const tracker = new LatencyTracker();

	for (let i = 0; i < iterations; i++) {
		const keyArg = keyArgs[Math.floor(Math.random() * keyArgs.length)];
		const start = process.hrtime.bigint();
		db.get(User, keyArg);
		tracker.record(Number(process.hrtime.bigint() - start));
	}

	return tracker.getStats();
}

function benchmarkFluentGetRef(
	db: ReturnType<typeof kiteSync>,
	keyArgs: string[],
	iterations: number,
): LatencyStats {
	const tracker = new LatencyTracker();

	for (let i = 0; i < iterations; i++) {
		const keyArg = keyArgs[Math.floor(Math.random() * keyArgs.length)];
		const start = process.hrtime.bigint();
		db.getRef(User, keyArg);
		tracker.record(Number(process.hrtime.bigint() - start));
	}

	return tracker.getStats();
}

function benchmarkFluentGetId(
	db: ReturnType<typeof kiteSync>,
	keyArgs: string[],
	iterations: number,
): LatencyStats {
	const tracker = new LatencyTracker();

	for (let i = 0; i < iterations; i++) {
		const keyArg = keyArgs[Math.floor(Math.random() * keyArgs.length)];
		const start = process.hrtime.bigint();
		db.getId(User, keyArg);
		tracker.record(Number(process.hrtime.bigint() - start));
	}

	return tracker.getStats();
}

function benchmarkLowLevelTraversal(
	db: Database,
	nodeIds: number[],
	etypeId: number,
	iterations: number,
): LatencyStats {
	const tracker = new LatencyTracker();
	const step = traversalStep(JsTraversalDirection.Out, etypeId);

	for (let i = 0; i < iterations; i++) {
		const nodeId = nodeIds[Math.floor(Math.random() * nodeIds.length)];
		const start = process.hrtime.bigint();
		db.traverseNodeIds([nodeId], [step]);
		tracker.record(Number(process.hrtime.bigint() - start));
	}

	return tracker.getStats();
}

function benchmarkFluentTraversal(
	db: ReturnType<typeof kiteSync>,
	userRefs: Array<{ id: number }>,
	iterations: number,
): LatencyStats {
	const tracker = new LatencyTracker();

	for (let i = 0; i < iterations; i++) {
		const userRef = userRefs[Math.floor(Math.random() * userRefs.length)];
		const start = process.hrtime.bigint();
		db.from(userRef).out(knows).count();
		tracker.record(Number(process.hrtime.bigint() - start));
	}

	return tracker.getStats();
}

function benchmarkFluentTraversalNodes(
	db: ReturnType<typeof kiteSync>,
	userRefs: Array<{ id: number }>,
	iterations: number,
): LatencyStats {
	const tracker = new LatencyTracker();

	for (let i = 0; i < iterations; i++) {
		const userRef = userRefs[Math.floor(Math.random() * userRefs.length)];
		const start = process.hrtime.bigint();
		db.from(userRef).out(knows).nodes();
		tracker.record(Number(process.hrtime.bigint() - start));
	}

	return tracker.getStats();
}

function benchmarkFluentTraversalToArray(
	db: ReturnType<typeof kiteSync>,
	userRefs: Array<{ id: number }>,
	iterations: number,
): LatencyStats {
	const tracker = new LatencyTracker();

	for (let i = 0; i < iterations; i++) {
		const userRef = userRefs[Math.floor(Math.random() * userRefs.length)];
		const start = process.hrtime.bigint();
		db.from(userRef).out(knows).toArray();
		tracker.record(Number(process.hrtime.bigint() - start));
	}

	return tracker.getStats();
}

function benchmarkLowLevelPathfinding(
	db: Database,
	nodeIds: number[],
	etypeId: number,
	iterations: number,
): LatencyStats {
	const tracker = new LatencyTracker();

	for (let i = 0; i < iterations; i++) {
		const src = nodeIds[Math.floor(Math.random() * nodeIds.length)];
		let dst = nodeIds[Math.floor(Math.random() * nodeIds.length)];
		while (dst === src) {
			dst = nodeIds[Math.floor(Math.random() * nodeIds.length)];
		}

		const start = process.hrtime.bigint();
		const config = pathConfig(src, dst);
		config.allowedEdgeTypes = [etypeId];
		config.maxDepth = 5;
		db.bfs(config);
		tracker.record(Number(process.hrtime.bigint() - start));
	}

	return tracker.getStats();
}

function benchmarkFluentPathfinding(
	db: ReturnType<typeof kiteSync>,
	userRefs: Array<{ id: number }>,
	iterations: number,
): LatencyStats {
	const tracker = new LatencyTracker();

	for (let i = 0; i < iterations; i++) {
		const src = userRefs[Math.floor(Math.random() * userRefs.length)];
		let dst = userRefs[Math.floor(Math.random() * userRefs.length)];
		while (dst.id === src.id) {
			dst = userRefs[Math.floor(Math.random() * userRefs.length)];
		}

		const start = process.hrtime.bigint();
		db.path(src, dst).via(knows).maxDepth(5).bfs();
		tracker.record(Number(process.hrtime.bigint() - start));
	}

	return tracker.getStats();
}

// =============================================================================
// Main
// =============================================================================

async function runBenchmarks(config: BenchConfig): Promise<void> {
	const now = new Date();
	console.log("=".repeat(100));
	console.log("KiteDB TypeScript Fluent API vs Low-Level API Benchmark");
	console.log("=".repeat(100));
	console.log(`Date: ${now.toISOString()}`);
	console.log(`Nodes: ${formatNumber(config.nodes)}`);
	console.log(`Edges: ${formatNumber(config.edges)}`);
	console.log(`Iterations: ${formatNumber(config.iterations)}`);
	console.log("=".repeat(100));

	// Create temporary directories for both databases
	const lowLevelDir = makeTempDir();
	const fluentDir = makeTempDir();

	try {
		// =================================================================
		// Setup Phase
		// =================================================================
		console.log("\n[1/7] Setting up databases...");

		// Low-level database setup
		const lowLevelDb = Database.open(path.join(lowLevelDir, "test.kitedb"));
		const knowsEtype = lowLevelDb.getOrCreateEtype("knows");
		const nameKey = lowLevelDb.getOrCreatePropkey("name");
		const emailKey = lowLevelDb.getOrCreatePropkey("email");
		const ageKey = lowLevelDb.getOrCreatePropkey("age");

		// Fluent database setup
		const fluentDb = kiteSync(path.join(fluentDir, "test.kitedb"), {
			nodes: [User],
			edges: [knows],
		});

		// =================================================================
		// Build Test Data
		// =================================================================
		console.log("\n[2/7] Building test data...");

		// Low-level: create nodes
		const lowLevelNodeIds: number[] = [];
		const lowLevelKeys: string[] = [];

		lowLevelDb.begin();
		for (let i = 0; i < config.nodes; i++) {
			const key = `user:${i}`;
			const nodeId = lowLevelDb.createNode(key);
			lowLevelDb.setNodeProp(nodeId, nameKey, {
				propType: PropType.String,
				stringValue: `User ${i}`,
			});
			lowLevelDb.setNodeProp(nodeId, emailKey, {
				propType: PropType.String,
				stringValue: `user${i}@example.com`,
			});
			lowLevelDb.setNodeProp(nodeId, ageKey, {
				propType: PropType.Int,
				intValue: 20 + (i % 50),
			});
			lowLevelNodeIds.push(nodeId);
			lowLevelKeys.push(key);
		}
		lowLevelDb.commit();

		// Low-level: create edges
		lowLevelDb.begin();
		for (let i = 0; i < config.edges; i++) {
			const src =
				lowLevelNodeIds[Math.floor(Math.random() * lowLevelNodeIds.length)];
			const dst =
				lowLevelNodeIds[Math.floor(Math.random() * lowLevelNodeIds.length)];
			if (src !== dst) {
				lowLevelDb.addEdge(src, knowsEtype, dst);
			}
		}
		lowLevelDb.commit();

		console.log(
			`  Low-level: ${lowLevelNodeIds.length} nodes, ${config.edges} edges`,
		);

		// Fluent: create nodes
		const fluentUserRefs: Array<{ id: number }> = [];
		const fluentKeyArgs: string[] = [];

		for (let i = 0; i < config.nodes; i++) {
			const keyArg = String(i);
			const userRef = fluentDb
				.insert(User)
				.values(keyArg, {
					name: `User ${i}`,
					email: `user${i}@example.com`,
					age: 20 + (i % 50),
				})
				.returning();
			fluentUserRefs.push(userRef);
			fluentKeyArgs.push(keyArg);
		}

		// Fluent: create edges
		for (let i = 0; i < config.edges; i++) {
			const src =
				fluentUserRefs[Math.floor(Math.random() * fluentUserRefs.length)];
			const dst =
				fluentUserRefs[Math.floor(Math.random() * fluentUserRefs.length)];
			if (src.id !== dst.id) {
				fluentDb.link(src, knows, dst, { since: 2020 });
			}
		}

		console.log(
			`  Fluent: ${fluentUserRefs.length} nodes, ${config.edges} edges`,
		);

		// Optimize both databases
		lowLevelDb.optimize();

		// =================================================================
		// Benchmark: Insert Operations
		// =================================================================
		console.log("\n[3/7] Benchmarking insert operations...");

		const lowLevelInsertStats = benchmarkLowLevelInserts(lowLevelDb, config);
		const fluentInsertStats = benchmarkFluentInserts(fluentDb, config);

		// =================================================================
		// Benchmark: Key Lookups
		// =================================================================
		console.log("\n[4/7] Benchmarking key lookups...");

		const lowLevelLookupStats = benchmarkLowLevelKeyLookup(
			lowLevelDb,
			lowLevelKeys,
			config.iterations,
		);
		const fluentGetStats = benchmarkFluentKeyLookup(
			fluentDb,
			fluentKeyArgs,
			config.iterations,
		);
		const fluentGetRefStats = benchmarkFluentGetRef(
			fluentDb,
			fluentKeyArgs,
			config.iterations,
		);
		const fluentGetIdStats = benchmarkFluentGetId(
			fluentDb,
			fluentKeyArgs,
			config.iterations,
		);

		// =================================================================
		// Benchmark: Traversals
		// =================================================================
		console.log("\n[5/7] Benchmarking traversals...");

		const lowLevelTraversalStats = benchmarkLowLevelTraversal(
			lowLevelDb,
			lowLevelNodeIds,
			knowsEtype,
			config.iterations,
		);
		const fluentTraversalStats = benchmarkFluentTraversal(
			fluentDb,
			fluentUserRefs,
			config.iterations,
		);
		const fluentTraversalNodesStats = benchmarkFluentTraversalNodes(
			fluentDb,
			fluentUserRefs,
			config.iterations,
		);
		const fluentTraversalToArrayStats = benchmarkFluentTraversalToArray(
			fluentDb,
			fluentUserRefs,
			config.iterations,
		);

		// =================================================================
		// Benchmark: Pathfinding
		// =================================================================
		console.log("\n[6/7] Benchmarking pathfinding...");

		const lowLevelPathStats = benchmarkLowLevelPathfinding(
			lowLevelDb,
			lowLevelNodeIds,
			knowsEtype,
			Math.min(config.iterations, 500), // Pathfinding is slower
		);
		const fluentPathStats = benchmarkFluentPathfinding(
			fluentDb,
			fluentUserRefs,
			Math.min(config.iterations, 500),
		);

		// =================================================================
		// Results
		// =================================================================
		console.log("\n[7/7] Results");
		console.log("=".repeat(100));
		console.log(
			"\n=== Comparison (lower latency is better, overhead closer to 1.0x is better) ===\n",
		);

		printComparison(
			"Insert (single node + props)",
			lowLevelInsertStats,
			fluentInsertStats,
		);
		printComparison(
			"Key lookup (raw vs get with props)",
			lowLevelLookupStats,
			fluentGetStats,
		);
		printComparison(
			"Key lookup (raw vs getRef, no props)",
			lowLevelLookupStats,
			fluentGetRefStats,
		);
		printComparison(
			"Key lookup (raw vs getId, id-only)",
			lowLevelLookupStats,
			fluentGetIdStats,
		);
		printComparison(
			"1-hop traversal (count)",
			lowLevelTraversalStats,
			fluentTraversalStats,
		);
		printComparison(
			"1-hop traversal (nodes/ids)",
			lowLevelTraversalStats,
			fluentTraversalNodesStats,
		);
		printComparison(
			"1-hop traversal (toArray with props)",
			lowLevelTraversalStats,
			fluentTraversalToArrayStats,
		);
		printComparison(
			"Pathfinding BFS (max depth 5)",
			lowLevelPathStats,
			fluentPathStats,
		);

		console.log("\n--- Detailed Statistics ---\n");

		console.log("Insert Operations:");
		console.log(
			`  Low-level:  p50=${formatLatency(lowLevelInsertStats.p50).padStart(10)}  p95=${formatLatency(lowLevelInsertStats.p95).padStart(10)}  (${formatNumber(Math.floor(lowLevelInsertStats.opsPerSec))} ops/sec)`,
		);
		console.log(
			`  Fluent:     p50=${formatLatency(fluentInsertStats.p50).padStart(10)}  p95=${formatLatency(fluentInsertStats.p95).padStart(10)}  (${formatNumber(Math.floor(fluentInsertStats.opsPerSec))} ops/sec)`,
		);

		console.log("\nKey Lookups:");
		console.log(
			`  Low-level:  p50=${formatLatency(lowLevelLookupStats.p50).padStart(10)}  p95=${formatLatency(lowLevelLookupStats.p95).padStart(10)}  (${formatNumber(Math.floor(lowLevelLookupStats.opsPerSec))} ops/sec)`,
		);
		console.log(
			`  Fluent get: p50=${formatLatency(fluentGetStats.p50).padStart(10)}  p95=${formatLatency(fluentGetStats.p95).padStart(10)}  (${formatNumber(Math.floor(fluentGetStats.opsPerSec))} ops/sec)`,
		);
		console.log(
			`  Fluent ref: p50=${formatLatency(fluentGetRefStats.p50).padStart(10)}  p95=${formatLatency(fluentGetRefStats.p95).padStart(10)}  (${formatNumber(Math.floor(fluentGetRefStats.opsPerSec))} ops/sec)`,
		);
		console.log(
			`  Fluent id:  p50=${formatLatency(fluentGetIdStats.p50).padStart(10)}  p95=${formatLatency(fluentGetIdStats.p95).padStart(10)}  (${formatNumber(Math.floor(fluentGetIdStats.opsPerSec))} ops/sec)`,
		);

		console.log("\nTraversals (1-hop):");
		console.log(
			`  Low-level:      p50=${formatLatency(lowLevelTraversalStats.p50).padStart(10)}  p95=${formatLatency(lowLevelTraversalStats.p95).padStart(10)}  (${formatNumber(Math.floor(lowLevelTraversalStats.opsPerSec))} ops/sec)`,
		);
		console.log(
			`  Fluent count:   p50=${formatLatency(fluentTraversalStats.p50).padStart(10)}  p95=${formatLatency(fluentTraversalStats.p95).padStart(10)}  (${formatNumber(Math.floor(fluentTraversalStats.opsPerSec))} ops/sec)`,
		);
		console.log(
			`  Fluent nodes:   p50=${formatLatency(fluentTraversalNodesStats.p50).padStart(10)}  p95=${formatLatency(fluentTraversalNodesStats.p95).padStart(10)}  (${formatNumber(Math.floor(fluentTraversalNodesStats.opsPerSec))} ops/sec)`,
		);
		console.log(
			`  Fluent toArray: p50=${formatLatency(fluentTraversalToArrayStats.p50).padStart(10)}  p95=${formatLatency(fluentTraversalToArrayStats.p95).padStart(10)}  (${formatNumber(Math.floor(fluentTraversalToArrayStats.opsPerSec))} ops/sec)`,
		);

		console.log("\nPathfinding (BFS, max depth 5):");
		console.log(
			`  Low-level:  p50=${formatLatency(lowLevelPathStats.p50).padStart(10)}  p95=${formatLatency(lowLevelPathStats.p95).padStart(10)}  (${formatNumber(Math.floor(lowLevelPathStats.opsPerSec))} ops/sec)`,
		);
		console.log(
			`  Fluent:     p50=${formatLatency(fluentPathStats.p50).padStart(10)}  p95=${formatLatency(fluentPathStats.p95).padStart(10)}  (${formatNumber(Math.floor(fluentPathStats.opsPerSec))} ops/sec)`,
		);

		// =================================================================
		// Tinybench comparison (for more accurate microbenchmarks)
		// =================================================================
		console.log("\n" + "=".repeat(100));
		console.log("Tinybench Microbenchmarks (more accurate, with warmup)");
		console.log("=".repeat(100));

		const bench = new Bench({ iterations: 100, warmupIterations: 10 });

		// Pick a random key/ref for consistent benchmarking
		const randomKey = lowLevelKeys[Math.floor(lowLevelKeys.length / 2)];
		const randomKeyArg = fluentKeyArgs[Math.floor(fluentKeyArgs.length / 2)];
		const randomNodeId =
			lowLevelNodeIds[Math.floor(lowLevelNodeIds.length / 2)];
		const randomUserRef = fluentUserRefs[Math.floor(fluentUserRefs.length / 2)];
		const batchSize = 16;
		const batchCenter = Math.floor(fluentUserRefs.length / 2);
		const batchStart = Math.max(0, batchCenter - batchSize / 2);
		const batchEnd = Math.min(fluentUserRefs.length, batchCenter + batchSize / 2);
		const batchIds = fluentUserRefs
			.slice(batchStart, batchEnd)
			.map((ref) => ref.id);

		bench
			.add("Low-level: key lookup", () => {
				lowLevelDb.getNodeByKey(randomKey);
			})
			.add("Fluent: get (with props)", () => {
				fluentDb.get(User, randomKeyArg);
			})
			.add("Fluent: getRef (no props)", () => {
				fluentDb.getRef(User, randomKeyArg);
			})
			.add("Fluent: getId (id-only)", () => {
				fluentDb.getId(User, randomKeyArg);
			})
			.add("Fluent: getById (with props)", () => {
				fluentDb.getById(randomUserRef.id);
			})
			.add("Fluent: getById x16 (loop)", () => {
				for (const id of batchIds) {
					fluentDb.getById(id);
				}
			})
			.add("Fluent: getByIds (batch 16)", () => {
				fluentDb.getByIds(batchIds);
			})
			.add("Low-level: traverseNodeIds", () => {
				lowLevelDb.traverseNodeIds(
					[randomNodeId],
					[traversalStep(JsTraversalDirection.Out, knowsEtype)],
				);
			})
			.add("Fluent: from().out().count()", () => {
				fluentDb.from(randomUserRef).out(knows).count();
			})
			.add("Fluent: from().out().nodes()", () => {
				fluentDb.from(randomUserRef).out(knows).nodes();
			})
			.add("Fluent: from().out().nodesWithProps()", () => {
				fluentDb.from(randomUserRef).out(knows).nodesWithProps();
			});

		await bench.run();
		console.table(bench.table());

		// Cleanup
		lowLevelDb.close();
		fluentDb.close();
	} finally {
		// Clean up temp directories
		fs.rmSync(lowLevelDir, { recursive: true, force: true });
		fs.rmSync(fluentDir, { recursive: true, force: true });
	}

	console.log("\n" + "=".repeat(100));
	console.log("Benchmark complete.");
	console.log("=".repeat(100));
}

// Run benchmarks
const config = parseArgs();
runBenchmarks(config).catch(console.error);
