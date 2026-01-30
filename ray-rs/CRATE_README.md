# RayDB Core (Rust)

RayDB is a high-performance embedded graph database with built-in vector search.
This crate provides the Rust core and the high-level Ray API.

## Features

- ACID transactions with WAL-based durability
- Node and edge CRUD with properties
- Labels, edge types, and schema helpers
- Fluent traversal and pathfinding (BFS, Dijkstra, Yen)
- Vector embeddings with IVF and IVF-PQ indexes
- Single-file and multi-file storage formats

## Install

```toml
[dependencies]
raydb-core = "0.1"
```

## Quick start (Ray API)

```rust
use raydb_core::api::ray::{EdgeDef, NodeDef, PropDef, Ray, RayOptions};
use raydb_core::types::PropValue;
use std::collections::HashMap;

fn main() -> raydb_core::error::Result<()> {
  let user = NodeDef::new("User", "user:")
    .prop(PropDef::string("name").required());
  let knows = EdgeDef::new("KNOWS");

  let mut ray = Ray::open("my_graph.raydb", RayOptions::new().node(user).edge(knows))?;

  let mut alice_props = HashMap::new();
  alice_props.insert("name".to_string(), PropValue::String("Alice".into()));
  let alice = ray.create_node("User", "alice", alice_props)?;

  let bob = ray.create_node("User", "bob", HashMap::new())?;
  ray.link(alice.id, "KNOWS", bob.id)?;

  let friends = ray.neighbors_out(alice.id, Some("KNOWS"))?;
  println!("friends: {friends:?}");

  Ok(())
}
```

## Lower-level API

If you want direct access to graph primitives, use `raydb_core::graph::db::open_graph_db`
and the modules under `raydb_core::graph`, `raydb_core::vector`, and `raydb_core::core`.

## Documentation

```text
https://ray-kwaf.vercel.app/docs
```

## License

MIT License - see the main project LICENSE file for details.
