# @ray-db/core

RayDB native bindings for Node.js (and WASI/browser builds), powered by Rust + N-API.

Docs: https://ray-kwaf.vercel.app/docs

## Install

```bash
npm install @ray-db/core
# or
pnpm add @ray-db/core
# or
yarn add @ray-db/core
```

This package ships prebuilt binaries for major platforms. If a prebuild isn't available for your target, you'll need a Rust toolchain to build from source.

## Quickstart (graph basics)

```ts
import {
  Database,
  JsTraversalDirection,
  PropType,
  pathConfig,
  traversalStep,
} from '@ray-db/core'

const db = Database.open('example.raydb', { createIfMissing: true })

// Transactions are explicit for write operations
db.begin()
const alice = db.createNode('user:alice')
const bob = db.createNode('user:bob')

const knows = db.getOrCreateEtype('knows')
const weight = db.getOrCreatePropkey('weight')

db.addEdge(alice, knows, bob)

// Set a typed edge property
db.setEdgeProp(alice, knows, bob, weight, {
  propType: PropType.Int,
  intValue: 1,
})

db.commit()

// Traverse
const oneHop = db.traverseSingle([alice], JsTraversalDirection.Out, knows)
console.log(oneHop)

// Multi-hop traversal
const steps = [
  traversalStep(JsTraversalDirection.Out, knows),
  traversalStep(JsTraversalDirection.Out, knows),
]
const twoHop = db.traverse([alice], steps)
console.log(twoHop)

// Pathfinding
const config = pathConfig(alice, bob)
config.allowedEdgeTypes = [knows]
const shortest = db.bfs(config)
console.log(shortest)

db.close()
```

## Backups and health checks

```ts
import { createBackup, restoreBackup, healthCheck } from '@ray-db/core'

const backup = createBackup(db, 'backups/graph')
const restoredPath = restoreBackup(backup.path, 'restored/graph')

const health = healthCheck(db)
console.log(health.healthy)
```

## Vector search

```ts
import { createVectorIndex } from '@ray-db/core'

const index = createVectorIndex({ dimensions: 3 })
index.set(1, [0.1, 0.2, 0.3])
index.set(2, [0.1, 0.25, 0.35])
index.buildIndex()

const hits = index.search([0.1, 0.2, 0.3], { k: 5 })
console.log(hits)
```

## Browser/WASI builds

This package exposes a WASI-compatible build via the `browser` export for bundlers, backed by `@ray-db/core-wasm32-wasi`. If you need to import it directly:

```ts
import { Database } from '@ray-db/core-wasm32-wasi'
```

## API surface

The Node bindings expose both low-level graph primitives (`Database`) and higher-level APIs (Ray) for schema-driven workflows, plus metrics, backups, traversal, and vector search. For full API details and guides, see the docs:

https://ray-kwaf.vercel.app/docs

## License

MIT
