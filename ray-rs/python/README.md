# KiteDB for Python

KiteDB is a high-performance embedded graph database with built-in vector search.
This package provides the Python bindings to the Rust core.

## Features

- ACID transactions with commit/rollback
- Node and edge CRUD operations with properties
- Labels, edge types, and property keys
- Fluent traversal and pathfinding (BFS, Dijkstra, A\*)
- Vector embeddings with IVF and IVF-PQ indexes
- Single-file storage format

## Install

### From PyPI

```bash
pip install kitedb
```

### From source

```bash
# Install maturin (Rust extension build tool)
python -m pip install -U maturin

# Build and install in development mode
maturin develop --features python

# Or build a wheel
maturin build --features python --release
pip install target/wheels/kitedb-*.whl
```

## Quick start (fluent API)

The fluent API provides a high-level, type-safe interface:

```python
from kitedb import kite, node, edge, prop, optional

# Define your schema
User = node("user",
    key=lambda id: f"user:{id}",
    props={
        "name": prop.string("name"),
        "email": prop.string("email"),
        "age": optional(prop.int("age")),
    }
)

Knows = edge("knows", {
    "since": prop.int("since"),
})

# Open database
with kite("./social.kitedb", nodes=[User], edges=[Knows]) as db:
    # Insert nodes
    alice = db.insert(User).values(key="alice", name="Alice", email="alice@example.com").returning()
    bob = db.insert(User).values(key="bob", name="Bob", email="bob@example.com").returning()

    # Create edges
    db.link(alice, Knows, bob, since=2024)

    # Traverse
    friends = db.from_(alice).out(Knows).nodes().to_list()

    # Pathfinding
    path = db.shortest_path(alice).via(Knows).to(bob).dijkstra()
```

## Quick start (low-level API)

For direct control, use the low-level `Database` class:

```python
from kitedb import Database, PropValue

with Database("my_graph.kitedb") as db:
    db.begin()

    alice = db.create_node("user:alice")
    bob = db.create_node("user:bob")

    name_key = db.get_or_create_propkey("name")
    db.set_node_prop(alice, name_key, PropValue.string("Alice"))
    db.set_node_prop(bob, name_key, PropValue.string("Bob"))

    knows = db.get_or_create_etype("knows")
    db.add_edge(alice, knows, bob)

    db.commit()

    print("nodes:", db.count_nodes())
    print("edges:", db.count_edges())
```

## Bulk ingest (max throughput)

Use bulk-load transactions + batch APIs to maximize write throughput.
Bulk-load disables MVCC, so avoid concurrent readers/writers while it runs.

```python
from kitedb import Database

db = Database("my_graph.kitedb")
db.begin_bulk()

node_ids = db.create_nodes_batch(keys)  # keys: List[Optional[str]]
db.add_edges_batch(edges)               # edges: List[Tuple[int, int, int]]
db.add_edges_with_props_batch(edges_with_props)

db.commit()
```

## Fluent traversal

```python
from kitedb import TraverseOptions

friends = db.from_(alice).out(knows).to_list()

results = db.from_(alice).traverse(
    knows,
    TraverseOptions(max_depth=3, min_depth=1, direction="out", unique=True),
).to_list()
```

## Concurrent Access

KiteDB supports concurrent read operations from multiple threads. Read operations don't block each other:

```python
import threading
from concurrent.futures import ThreadPoolExecutor

# Multiple threads can read concurrently
def read_user(key):
    return db.get_node_by_key(key)

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(read_user, f"user:{i}") for i in range(100)]
    results = [f.result() for f in futures]

# Or with asyncio (reads run concurrently)
import asyncio

async def read_users():
    loop = asyncio.get_event_loop()
    tasks = [
        loop.run_in_executor(None, db.get_node_by_key, f"user:{i}")
        for i in range(100)
    ]
    return await asyncio.gather(*tasks)
```

**Concurrency model:**

- **Reads are concurrent**: Multiple `get_node_by_key()`, `get_neighbors()`, traversals, etc. can run in parallel
- **Writes are exclusive**: Write operations (`create_node()`, `add_edge()`, etc.) require exclusive access
- **Thread safety**: The `Database` object is safe to share across threads

Note: Python's GIL is released during Rust operations, allowing true parallelism for I/O-bound database access.

## Vector search

```python
from kitedb import IvfIndex, IvfConfig, SearchOptions

index = IvfIndex(dimensions=128, config=IvfConfig(n_clusters=100))

training_data = [0.1] * (128 * 1000)
index.add_training_vectors(training_data, num_vectors=1000)
index.train()

index.insert(vector_id=1, vector=[0.1] * 128)

results = index.search(
    manifest_json='{"vectors": {...}}',
    query=[0.1] * 128,
    k=10,
    options=SearchOptions(n_probe=20),
)

for result in results:
    print(result.node_id, result.distance)
```

## Replication admin (low-level API)

Phase D replication controls are available on `Database`:

```python
from kitedb import (
    Database,
    OpenOptions,
    collect_replication_log_transport_json,
    collect_replication_metrics_otel_json,
    collect_replication_metrics_prometheus,
    collect_replication_snapshot_transport_json,
    push_replication_metrics_otel_json,
)

primary = Database(
    "cluster-primary.kitedb",
    OpenOptions(
        replication_role="primary",
        replication_sidecar_path="./cluster-primary.sidecar",
        replication_segment_max_bytes=64 * 1024 * 1024,
        replication_retention_min_entries=1024,
    ),
)

primary.begin()
primary.create_node("n:1")
token = primary.commit_with_token()

primary.primary_report_replica_progress("replica-a", 1, 42)
pruned_segments, retained_floor = primary.primary_run_retention()
primary_status = primary.primary_replication_status()

replica = Database(
    "cluster-replica.kitedb",
    OpenOptions(
        replication_role="replica",
        replication_sidecar_path="./cluster-replica.sidecar",
        replication_source_db_path="cluster-primary.kitedb",
        replication_source_sidecar_path="./cluster-primary.sidecar",
    ),
)

replica.replica_bootstrap_from_snapshot()
replica.replica_catch_up_once(256)
if token:
    replica.wait_for_token(token, 2000)
replica_status = replica.replica_replication_status()
if replica_status and replica_status["needs_reseed"]:
    replica.replica_reseed_from_snapshot()

prometheus = collect_replication_metrics_prometheus(primary)
print(prometheus)

otel_json = collect_replication_metrics_otel_json(primary)
print(otel_json)

status_code, response_body = push_replication_metrics_otel_json(
    primary,
    "http://127.0.0.1:4318/v1/metrics",
    timeout_ms=5000,
)
print(status_code, response_body)

secure_status, secure_body = push_replication_metrics_otel_json(
    primary,
    "https://collector.internal:4318/v1/metrics",
    timeout_ms=5000,
    https_only=True,
    ca_cert_pem_path="./tls/collector-ca.pem",
    client_cert_pem_path="./tls/client.pem",
    client_key_pem_path="./tls/client-key.pem",
)
print(secure_status, secure_body)

snapshot_json = collect_replication_snapshot_transport_json(primary, include_data=False)
print(snapshot_json)

log_json = collect_replication_log_transport_json(
    primary,
    cursor=None,
    max_frames=128,
    max_bytes=1024 * 1024,
    include_payload=False,
)
print(log_json)

replica.close()
primary.close()
```

## Documentation

```text
https://kitedb.vercel.com/docs
```

## License

MIT License - see the main project LICENSE file for details.
