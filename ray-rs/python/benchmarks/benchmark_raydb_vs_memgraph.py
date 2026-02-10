#!/usr/bin/env python3
"""
RayDB vs Memgraph 1-hop traversal benchmark.

Workload:
  - Build identical graph shape in both databases
  - 10k nodes, 20k edges (defaults)
  - Query equivalent to `db.from(alice).out(Knows).toArray()`
  - Alice fan-out defaults to 10 (inside the requested 5-20 range)

Prerequisites:
  - RayDB python bindings installed (`maturin develop --features python`)
  - Memgraph running and reachable via Bolt
  - Neo4j python driver installed (`pip install neo4j`)
"""

from __future__ import annotations

import argparse
import random
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
  from kitedb import Database, define_edge, define_node, kite
except ImportError:
  print("Error: kitedb module not found. Build the Python bindings first:")
  print("  maturin develop --features python")
  sys.exit(1)

try:
  from neo4j import GraphDatabase
except ImportError:
  print("Error: neo4j driver not found. Install it with:")
  print("  pip install neo4j")
  sys.exit(1)


@dataclass
class BenchConfig:
  nodes: int
  edges: int
  query_results: int
  iterations: int
  warmup: int
  seed: int
  batch_size: int
  memgraph_uri: str
  memgraph_user: str
  memgraph_password: str
  memgraph_database: Optional[str]
  keep_raydb: bool


@dataclass
class LatencyStats:
  count: int
  min_ns: int
  max_ns: int
  sum_ns: int
  p50_ns: int
  p95_ns: int
  p99_ns: int

  @property
  def ops_per_sec(self) -> float:
    if self.sum_ns <= 0:
      return 0.0
    return self.count / (self.sum_ns / 1_000_000_000.0)


class LatencyTracker:
  def __init__(self):
    self.samples_ns: List[int] = []

  def record(self, latency_ns: int):
    self.samples_ns.append(latency_ns)

  def stats(self) -> LatencyStats:
    if not self.samples_ns:
      return LatencyStats(0, 0, 0, 0, 0, 0, 0)

    sorted_samples = sorted(self.samples_ns)
    count = len(sorted_samples)
    return LatencyStats(
      count=count,
      min_ns=sorted_samples[0],
      max_ns=sorted_samples[-1],
      sum_ns=sum(sorted_samples),
      p50_ns=sorted_samples[int(count * 0.50)],
      p95_ns=sorted_samples[int(count * 0.95)],
      p99_ns=sorted_samples[int(count * 0.99)],
    )


def parse_args() -> BenchConfig:
  parser = argparse.ArgumentParser(description="RayDB vs Memgraph traversal benchmark")
  parser.add_argument("--nodes", type=int, default=10_000)
  parser.add_argument("--edges", type=int, default=20_000)
  parser.add_argument(
    "--query-results",
    type=int,
    default=10,
    help="Exact outgoing neighbors from alice in generated graph",
  )
  parser.add_argument("--iterations", type=int, default=5_000)
  parser.add_argument("--warmup", type=int, default=500)
  parser.add_argument("--seed", type=int, default=42)
  parser.add_argument("--batch-size", type=int, default=1_000)
  parser.add_argument("--memgraph-uri", type=str, default="bolt://127.0.0.1:7687")
  parser.add_argument("--memgraph-user", type=str, default="")
  parser.add_argument("--memgraph-password", type=str, default="")
  parser.add_argument("--memgraph-database", type=str, default="")
  parser.add_argument("--keep-raydb", action="store_true")

  args = parser.parse_args()

  if args.nodes < 2:
    raise ValueError("--nodes must be >= 2")
  if args.edges < 1:
    raise ValueError("--edges must be >= 1")
  if args.query_results < 1:
    raise ValueError("--query-results must be >= 1")
  if args.query_results >= args.nodes:
    raise ValueError("--query-results must be < --nodes")
  if args.query_results > args.edges:
    raise ValueError("--query-results must be <= --edges")
  if args.iterations < 1:
    raise ValueError("--iterations must be >= 1")
  if args.warmup < 0:
    raise ValueError("--warmup must be >= 0")
  if args.batch_size < 1:
    raise ValueError("--batch-size must be >= 1")

  return BenchConfig(
    nodes=args.nodes,
    edges=args.edges,
    query_results=args.query_results,
    iterations=args.iterations,
    warmup=args.warmup,
    seed=args.seed,
    batch_size=args.batch_size,
    memgraph_uri=args.memgraph_uri,
    memgraph_user=args.memgraph_user,
    memgraph_password=args.memgraph_password,
    memgraph_database=args.memgraph_database or None,
    keep_raydb=args.keep_raydb,
  )


def format_latency(ns: int) -> str:
  if ns < 1_000:
    return f"{ns}ns"
  if ns < 1_000_000:
    return f"{ns / 1_000.0:.2f}us"
  return f"{ns / 1_000_000.0:.2f}ms"


def format_number(value: int) -> str:
  return f"{value:,}"


def build_workload(
  nodes: int,
  edges: int,
  query_results: int,
  seed: int,
) -> Tuple[List[str], List[Tuple[int, int]]]:
  keys = ["user:alice"] + [f"user:u{i}" for i in range(1, nodes)]

  edge_set: set[Tuple[int, int]] = set()

  # Guarantee exact fan-out from alice (node 0) for query sanity.
  for dst in range(1, query_results + 1):
    edge_set.add((0, dst))

  rng = random.Random(seed)
  while len(edge_set) < edges:
    src = rng.randrange(1, nodes)  # keep alice fan-out stable
    dst = rng.randrange(0, nodes)
    if src == dst:
      continue
    edge_set.add((src, dst))

  return keys, list(edge_set)


def ingest_raydb(
  raydb_path: str,
  keys: Sequence[str],
  edges: Sequence[Tuple[int, int]],
  batch_size: int,
) -> float:
  started = time.perf_counter_ns()
  db = Database(raydb_path)
  try:
    etype = db.get_or_create_etype("knows")
    node_ids: List[int] = []

    for offset in range(0, len(keys), batch_size):
      db.begin_bulk()
      batch_keys = keys[offset : offset + batch_size]
      batch_ids = db.create_nodes_batch(list(batch_keys))
      node_ids.extend(batch_ids)
      db.commit()

    for offset in range(0, len(edges), batch_size):
      db.begin_bulk()
      batch_edges = []
      for src_index, dst_index in edges[offset : offset + batch_size]:
        batch_edges.append((node_ids[src_index], etype, node_ids[dst_index]))
      db.add_edges_batch(batch_edges)
      db.commit()
  finally:
    db.close()

  return (time.perf_counter_ns() - started) / 1_000_000.0


def benchmark_raydb_query(
  raydb_path: str,
  iterations: int,
  warmup: int,
) -> Tuple[LatencyStats, int]:
  user = define_node(
    "user",
    key=lambda key: f"user:{key}",
    props={},
  )
  knows = define_edge("knows", {})

  tracker = LatencyTracker()
  result_len = 0

  with kite(raydb_path, nodes=[user], edges=[knows]) as db:
    alice = db.get_ref(user, "alice")

    for _ in range(warmup):
      db.from_(alice).out(knows).to_list()

    for _ in range(iterations):
      start = time.perf_counter_ns()
      result = db.from_(alice).out(knows).to_list()
      tracker.record(time.perf_counter_ns() - start)
      result_len = len(result)

  return tracker.stats(), result_len


def new_memgraph_driver(config: BenchConfig):
  auth = None
  if config.memgraph_user or config.memgraph_password:
    auth = (config.memgraph_user, config.memgraph_password)
  return GraphDatabase.driver(config.memgraph_uri, auth=auth)


def session_for(driver, database: Optional[str]):
  if database:
    return driver.session(database=database)
  return driver.session()


def ingest_memgraph(
  driver,
  keys: Sequence[str],
  edges: Sequence[Tuple[int, int]],
  batch_size: int,
  database: Optional[str],
) -> float:
  started = time.perf_counter_ns()

  with session_for(driver, database) as session:
    session.run("MATCH (n) DETACH DELETE n").consume()

    try:
      session.run("CREATE INDEX ON :User(key)").consume()
    except Exception:
      # Index may already exist (from previous runs).
      pass

    for offset in range(0, len(keys), batch_size):
      rows = [{"key": key} for key in keys[offset : offset + batch_size]]
      session.run(
        "UNWIND $rows AS row CREATE (:User {key: row.key})",
        rows=rows,
      ).consume()

    for offset in range(0, len(edges), batch_size):
      rows = []
      for src_index, dst_index in edges[offset : offset + batch_size]:
        rows.append({"src": keys[src_index], "dst": keys[dst_index]})
      session.run(
        """
        UNWIND $rows AS row
        MATCH (s:User {key: row.src})
        MATCH (d:User {key: row.dst})
        CREATE (s)-[:KNOWS]->(d)
        """,
        rows=rows,
      ).consume()

  return (time.perf_counter_ns() - started) / 1_000_000.0


def benchmark_memgraph_query(
  driver,
  iterations: int,
  warmup: int,
  database: Optional[str],
) -> Tuple[LatencyStats, int]:
  tracker = LatencyTracker()
  result_len = 0
  query = "MATCH (a:User {key: $key})-[:KNOWS]->(b) RETURN b.key AS key"

  with session_for(driver, database) as session:
    for _ in range(warmup):
      list(session.run(query, key="user:alice"))

    for _ in range(iterations):
      start = time.perf_counter_ns()
      rows = list(session.run(query, key="user:alice"))
      tracker.record(time.perf_counter_ns() - start)
      result_len = len(rows)

  return tracker.stats(), result_len


def print_stats(label: str, stats: LatencyStats):
  print(
    f"{label:<10} p50={format_latency(stats.p50_ns):>10} "
    f"p95={format_latency(stats.p95_ns):>10} "
    f"p99={format_latency(stats.p99_ns):>10} "
    f"max={format_latency(stats.max_ns):>10} "
    f"({format_number(int(stats.ops_per_sec))} ops/sec)"
  )


def main():
  config = parse_args()
  keys, edges = build_workload(
    nodes=config.nodes,
    edges=config.edges,
    query_results=config.query_results,
    seed=config.seed,
  )

  raydb_dir = tempfile.mkdtemp(prefix="raydb-vs-memgraph-")
  raydb_path = str(Path(raydb_dir) / "benchmark.kitedb")

  print("RayDB vs Memgraph: 1-hop traversal benchmark")
  print(f"Nodes: {format_number(config.nodes)}")
  print(f"Edges: {format_number(config.edges)}")
  print(f"Alice expected results: {config.query_results}")
  print(f"Iterations: {format_number(config.iterations)} (warmup {format_number(config.warmup)})")
  print("")

  try:
    raydb_ingest_ms = ingest_raydb(raydb_path, keys, edges, config.batch_size)
    driver = new_memgraph_driver(config)
    try:
      memgraph_ingest_ms = ingest_memgraph(
        driver,
        keys,
        edges,
        config.batch_size,
        config.memgraph_database,
      )
      raydb_stats, raydb_results = benchmark_raydb_query(
        raydb_path,
        config.iterations,
        config.warmup,
      )
      memgraph_stats, memgraph_results = benchmark_memgraph_query(
        driver,
        config.iterations,
        config.warmup,
        config.memgraph_database,
      )
    finally:
      driver.close()

    if raydb_results != config.query_results:
      raise RuntimeError(
        f"RayDB returned {raydb_results} rows, expected {config.query_results}"
      )
    if memgraph_results != config.query_results:
      raise RuntimeError(
        f"Memgraph returned {memgraph_results} rows, expected {config.query_results}"
      )

    print("Setup times (not included in query latency):")
    print(f"  RayDB ingest:    {raydb_ingest_ms:.2f}ms")
    print(f"  Memgraph ingest: {memgraph_ingest_ms:.2f}ms")
    print("")
    print("Query latency (equivalent to from(alice).out(Knows).toArray):")
    print_stats("RayDB", raydb_stats)
    print_stats("Memgraph", memgraph_stats)

    if raydb_stats.p50_ns > 0:
      p50_ratio = memgraph_stats.p50_ns / raydb_stats.p50_ns
      p95_ratio = memgraph_stats.p95_ns / raydb_stats.p95_ns if raydb_stats.p95_ns > 0 else 0.0
      print("")
      print(f"Memgraph/RayDB ratio: p50={p50_ratio:.2f}x p95={p95_ratio:.2f}x")
  finally:
    if config.keep_raydb:
      print(f"\nRayDB dataset kept at: {raydb_path}")
    else:
      shutil.rmtree(raydb_dir, ignore_errors=True)


if __name__ == "__main__":
  main()
