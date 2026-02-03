# KiteDB Benchmarks

This document summarizes **measured** benchmark results. Raw outputs live in
`docs/benchmarks/results/` so we can trace every number back to an actual run.

> All numbers below were captured on **February 3, 2026**. If you need fresh
> numbers, rerun the commands in the next section and update this doc with the
> new output files.

## Test Environment

- Apple M4 (16GB)
- macOS 15.3 (Darwin 25.3.0)
- Rust 1.88.0
- Node 24.12.0
- Bun 1.3.5
- Python 3.12.8

## Running Benchmarks

### Rust (core, single-file raw)

```bash
cd ray-rs
cargo run --release --example single_file_raw_bench --no-default-features -- \
  --nodes 10000 --edges 50000 --iterations 10000
```

### Python bindings (single-file raw)

```bash
cd ray-rs/python/benchmarks
python3 benchmark_single_file_raw.py \
  --nodes 10000 --edges 50000 --iterations 10000
```

### TypeScript API overhead (fluent vs low-level)

```bash
cd ray-rs
node --import @oxc-node/core/register benchmark/bench-fluent-vs-lowlevel.ts
```

### Vector index (Rust)

```bash
cd ray-rs
cargo run --release --example vector_bench --no-default-features -- \
  --vectors 10000 --dimensions 768 --iterations 1000 --k 10 --n-probe 10
```

## Latest Results (2026-02-03)

Raw logs:

- `docs/benchmarks/results/2026-02-03-single-file-raw-rust.txt`
- `docs/benchmarks/results/2026-02-03-single-file-raw-python.txt`
- `docs/benchmarks/results/2026-02-03-bench-fluent-vs-lowlevel.txt`
- `docs/benchmarks/results/2026-02-03-vector-bench-rust.txt`

### Single-File Raw (Rust Core)

Config: 10k nodes, 50k edges, 10k iterations, vector dims=128, vector count=1k.

| Operation | p50 | p95 |
|-----------|-----|-----|
| Key lookup (random existing) | 125ns | 167ns |
| 1-hop traversal (out) | 208ns | 334ns |
| Edge exists (random) | 83ns | 125ns |
| Batch write (100 nodes) | 45.62us | 58.75us |
| get_node_vector() | 84ns | 209ns |
| has_node_vector() | 42ns | 84ns |
| Set vectors (batch 100) | 147.25us | 214.21us |

### Single-File Raw (Python Bindings)

Config: 10k nodes, 50k edges, 10k iterations, vector dims=128, vector count=1k.

| Operation | p50 | p95 |
|-----------|-----|-----|
| Key lookup (random existing) | 208ns | 375ns |
| 1-hop traversal (out) | 375ns | 583ns |
| Edge exists (random) | 125ns | 167ns |
| Batch write (100 nodes) | 253.08us | 5.78ms |
| get_node_vector() | 1.21us | 1.54us |
| has_node_vector() | 166ns | 167ns |
| Set vectors (batch 100) | 3.61ms | 6.23ms |

### TypeScript Fluent API vs Low-Level (NAPI)

Config: 1k nodes, 5k edges, 1k iterations.

| Operation | Low-level p50 | Fluent p50 | Overhead |
|-----------|---------------|------------|----------|
| Insert (single node + props) | 115.25us | 36.83us | 0.32x |
| Key lookup (get w/ props) | 208ns | 1.63us | 7.81x |
| Key lookup (getRef) | 208ns | 791ns | 3.80x |
| Key lookup (getId) | 208ns | 333ns | 1.60x |
| 1-hop traversal (count) | 1.21us | 5.75us | 4.76x |
| 1-hop traversal (nodes) | 1.21us | 5.83us | 4.83x |
| 1-hop traversal (toArray) | 1.21us | 10.38us | 8.59x |
| Pathfinding BFS (depth 5) | 170.79us | 167.71us | 0.98x |

### Vector Index (Rust)

Config: 10k vectors, 768 dims, 1k iterations, k=10, nProbe=10.

| Operation | p50 | p95 |
|-----------|-----|-----|
| Set vectors (10k) | 833ns | 2.12us |
| build_index() | 801.95ms | 801.95ms |
| get (random) | 167ns | 459ns |
| search (k=10, nProbe=10) | 557.54us | 918.79us |

## Notes

- These are **local** results. Expect variation across machines and datasets.
- We do **not** publish third-party comparisons here; run those yourself if needed.
