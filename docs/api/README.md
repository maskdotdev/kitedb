# Ray High-Level API

The `api` module provides a **Drizzle-style, type-safe API** for Ray, a high-performance embedded graph database. It wraps the lower-level database primitives with a fluent, ergonomic interface featuring full TypeScript type inference.

## Overview

The API consists of five main modules:

- **`ray.ts`** - Main entry point and database context
- **`schema.ts`** - Schema definition with property and relation types
- **`builders.ts`** - Query builders for insert, update, delete operations
- **`traversal.ts`** - Graph traversal with filtering and aggregation
- **`index.ts`** - Barrel export of public types and functions

## Key Concepts

### Schema-First Design

Define your graph structure upfront using `defineNode()` and `defineEdge()`:

```typescript
import { defineNode, defineEdge, prop, optional } from '@ray-db/ray';

// Define node types
const user = defineNode('user', {
  key: (id: string) => `user:${id}`,
  props: {
    name: prop.string('name'),
    email: prop.string('email'),
    age: optional(prop.int('age')),
  },
});

const company = defineNode('company', {
  key: (id: string) => `company:${id}`,
  props: {
    name: prop.string('name'),
  },
});

// Define edge types
const knows = defineEdge('knows', {
  since: prop.int('since'),
  strength: prop.float('strength').optional(),
});

const worksAt = defineEdge('worksAt', {
  startDate: prop.int('startDate'),
});
```

### Type Inference

The API uses advanced TypeScript inference to automatically derive types:

- `InferNodeInsert<N>` - Type for inserting a node
- `InferNode<N>` - Type for returned nodes (includes `$id` and `$key`)
- `InferEdgeProps<E>` - Type for edge properties

```typescript
type InsertUser = InferNodeInsert<typeof user>;
// Result: { key: string; name: string; email: string; age?: bigint; }

type ReturnedUser = InferNode<typeof user>;
// Result: { $id: bigint; $key: string; name: string; email: string; age?: bigint; }
```

## Module Reference

### `ray.ts` - Main Database API

#### `ray(path, options): Promise<Ray>`

Opens or creates a database with the given schema.

**Parameters:**
- `path: string` - Directory path for database files
- `options: RayOptions` - Schema and database options
  - `nodes: NodeDef[]` - Node type definitions
  - `edges: EdgeDef[]` - Edge type definitions
  - `readOnly?: boolean` - Open in read-only mode
  - `createIfMissing?: boolean` - Create if doesn't exist (default: true)
  - `lockFile?: boolean` - Use file locking (default: true)

**Example:**
```typescript
const db = await ray('./my-graph', {
  nodes: [user, company],
  edges: [knows, worksAt],
});
```

#### `Ray` Class

The main database context with methods for CRUD operations, transactions, and maintenance.

**Node Operations:**

- `insert<N>(node: N): InsertBuilder<N>` - Insert one or more nodes
- `update<N>(node: N): UpdateBuilder<N>` - Update nodes by definition with WHERE
- `update<N>(nodeRef: NodeRef<N>): UpdateByRefBuilder<N>` - Update a specific node
- `delete<N>(node: N): DeleteBuilder<N>` - Delete nodes by definition
- `delete<N>(nodeRef: NodeRef<N>): Promise<boolean>` - Delete a specific node
- `get<N>(node: N, key: any): Promise<NodeRef<N> & InferNode<N> | null>` - Look up node by key
- `exists(nodeRef: NodeRef): Promise<boolean>` - Check if node exists
- `all<N>(nodeDef: N): AsyncGenerator<NodeRef<N> & InferNode<N>>` - List all nodes of type
- `count<N>(nodeDef?: N): Promise<number>` - Count nodes (optionally filtered by type)

**Edge Operations:**

- `link<E>(src: NodeRef, edge: E, dst: NodeRef, props?): Promise<void>` - Create edge
- `unlink<E>(src: NodeRef, edge: E, dst: NodeRef): Promise<void>` - Delete edge
- `hasEdge<E>(src: NodeRef, edge: E, dst: NodeRef): Promise<boolean>` - Check edge exists
- `updateEdge<E>(src, edge, dst): UpdateEdgeBuilder<E>` - Update edge properties
- `allEdges<E>(edgeDef?: E): AsyncGenerator<EdgeData>` - List all edges (optionally filtered by type)
- `countEdges<E>(edgeDef?: E): Promise<number>` - Count edges (optionally filtered by type)

**Traversal:**

- `from<N>(node: NodeRef<N>): TraversalBuilder<N>` - Start traversal from a node

**Batch Operations:**

- `batch<T>(operations): Promise<Results>` - Execute multiple ops in single transaction

**Transactions:**

- `transaction<T>(fn): Promise<T>` - Execute operations in explicit transaction

**Maintenance:**

- `stats(): Promise<DbStats>` - Get database statistics
- `check(): Promise<CheckResult>` - Verify database integrity
- `optimize(): Promise<void>` - Compact snapshots with deltas
- `close(): Promise<void>` - Close database
- `$raw: GraphDB` - Escape hatch to raw database

### `schema.ts` - Schema Definition

#### Property Types

```typescript
prop.string(name: string): PropBuilder<"string">
prop.int(name: string): PropBuilder<"int">       // Stored as bigint
prop.float(name: string): PropBuilder<"float">   // f64
prop.bool(name: string): PropBuilder<"bool">
```

All property builders support `.optional()` or `optional(prop)` helper:

```typescript
const email = optional(prop.string('email'));
// or
const email = prop.string('email').optional();
```

#### `defineNode<Name, KeyArg, Props>(name, config): NodeDef`

Defines a node type with schema.

**Parameters:**
- `name: string` - Node type name
- `config: NodeConfig<KeyArg, Props>`
  - `key: (id: KeyArg) => string` - Key generation function
  - `props: PropsSchema` - Property definitions

```typescript
const user = defineNode('user', {
  key: (id: string) => `user:${id}`,
  props: {
    name: prop.string('name'),
    email: prop.string('email'),
  },
});
```

#### `defineEdge<Name, Props>(name, props?): EdgeDef`

Defines an edge type with optional properties.

```typescript
// Edge with properties
const knows = defineEdge('knows', {
  since: prop.int('since'),
  weight: optional(prop.float('weight')),
});

// Edge without properties
const follows = defineEdge('follows');
```

### `builders.ts` - Query Builders

#### Insert

```typescript
// Single insert
const alice = await db
  .insert(user)
  .values({ key: 'alice', name: 'Alice', email: 'alice@example.com' })
  .returning();

// Bulk insert
const [bob, charlie] = await db
  .insert(user)
  .values([
    { key: 'bob', name: 'Bob', email: 'bob@example.com' },
    { key: 'charlie', name: 'Charlie', email: 'charlie@example.com' },
  ])
  .returning();

// Insert without returning
await db
  .insert(user)
  .values({ key: 'dave', name: 'Dave', email: 'dave@example.com' })
  .execute();
```

#### Update by Definition

```typescript
await db
  .update(user)
  .set({ name: 'Alice Updated', email: 'newemail@example.com' })
  .where({ $key: 'user:alice' })
  .execute();
```

#### Update by Reference

```typescript
const alice = await db.get(user, 'alice');
await db
  .update(alice)
  .set({ name: 'Alice V2' })
  .execute();
```

#### Delete by Definition

```typescript
const deleted = await db
  .delete(user)
  .where({ $key: 'user:alice' })
  .execute();
```

#### Delete by Reference

```typescript
const alice = await db.get(user, 'alice');
const success = await db.delete(alice);
```

#### Link (Create Edge)

```typescript
const alice = await db.get(user, 'alice');
const bob = await db.get(user, 'bob');

await db.link(alice, knows, bob, { since: 2020 });
```

#### Unlink (Delete Edge)

```typescript
await db.unlink(alice, knows, bob);
```

#### Update Edge

```typescript
const edge = await db.updateEdge(alice, knows, bob);
await edge.set({ weight: 0.95 }).execute();
```

### `traversal.ts` - Graph Traversal

#### TraversalBuilder

Start traversal from a node and chain operations:

```typescript
const alice = await db.get(user, 'alice');

// Get all friends of Alice
const friends = await db
  .from(alice)
  .out(knows)
  .nodes()
  .toArray();

// Get friends' friends (depth 2)
const foaf = await db
  .from(alice)
  .out(knows)
  .out(knows)
  .nodes()
  .toArray();

// Multi-directional traversal
const connections = await db
  .from(alice)
  .both(knows)
  .nodes()
  .toArray();

// Variable-depth traversal with limits
const nearby = await db
  .from(alice)
  .traverse(knows, { direction: 'out', maxDepth: 3, unique: true })
  .nodes()
  .toArray();
```

#### Filtering

```typescript
// Filter by edge properties
await db
  .from(alice)
  .out(knows)
  .whereEdge(edge => edge.since < 2020)
  .nodes()
  .toArray();

// Filter by node properties
await db
  .from(alice)
  .out(knows)
  .whereNode(node => node.age > 25)
  .nodes()
  .toArray();
```

#### Results

All traversal results are **lazy async iterables**:

```typescript
const results = db.from(alice).out(knows).nodes();

// Iterate
for await (const friend of results) {
  console.log(friend.name);
}

// Or collect
const all = await results.toArray();
const first = await results.first();
const count = await results.count();
```

#### Edges

Get edges instead of nodes:

```typescript
const edges = await db
  .from(alice)
  .out(knows)
  .edges()
  .toArray();

// Each edge has: $src, $dst, $etype, and properties
for (const edge of edges) {
  console.log(`${edge.$src} --${edge.$etype}--> ${edge.$dst}`);
}
```

### Listing and Counting

List and count all nodes or edges with optional type filtering:

```typescript
// List all users (async generator - memory efficient)
for await (const user of db.all(user)) {
  console.log(user.name, user.$key);
}

// Count all nodes in database (fast - O(1) when no filter)
const totalNodes = await db.count();

// Count users only (requires iteration - O(n))
const userCount = await db.count(user);

// List all edges
for await (const edge of db.allEdges()) {
  console.log(`${edge.src.$id} -> ${edge.dst.$id}`);
}

// List only "knows" edges
for await (const edge of db.allEdges(knows)) {
  console.log(`${edge.src.$key} knows ${edge.dst.$key}`, edge.props);
}

// Count all edges (fast - O(1) when no filter)
const totalEdges = await db.countEdges();

// Count edges of specific type
const knowsCount = await db.countEdges(knows);
```

## Advanced Patterns

### Transactions

Execute multiple operations atomically:

```typescript
const result = await db.transaction(async (ctx) => {
  // All operations in this callback are in a single transaction
  const alice = await ctx.insert(user).values({ key: 'alice', name: 'Alice' }).returning();
  const bob = await ctx.insert(user).values({ key: 'bob', name: 'Bob' }).returning();
  
  await ctx.link(alice, knows, bob);
  
  return { alice, bob };
});
```

### Batch Operations

Combine multiple operations into one transaction:

```typescript
const [result1, result2, result3] = await db.batch([
  db.insert(user).values({ key: 'user1', name: 'User 1' }),
  db.insert(user).values({ key: 'user2', name: 'User 2' }),
  db.insert(company).values({ key: 'acme', name: 'ACME Corp' }),
]);
```

### Key Lookups

Keys are the primary way to look up nodes:

```typescript
// Define key schema
const user = defineNode('user', {
  key: (id: string) => `user:${id}`,
  // ...
});

// Later, look up by key
const alice = await db.get(user, 'alice'); // Uses key 'user:alice' internally
```

### Property Type Conversion

Properties are stored with type tags and converted automatically:

- **string** → `string`
- **int** → `bigint` (64-bit signed)
- **float** → `number` (f64)
- **bool** → `boolean`

```typescript
const user = defineNode('user', {
  key: (id: string) => `user:${id}`,
  props: {
    age: prop.int('age'), // Stored as bigint, passed as number
    score: prop.float('score'), // Stored as f64
  },
});

const alice = await db.insert(user).values({
  key: 'alice',
  age: 30, // OK: number → bigint
  score: 95.5,
}).returning();

console.log(typeof alice.age); // 'bigint'
console.log(typeof alice.score); // 'number'
```

## Error Handling

Most operations throw on error. Key error cases:

- **Node not found** - `get()` returns `null`, but WHERE clauses throw
- **Type mismatch** - Property type violations throw
- **Transaction rollback** - Errors in `transaction()` or `batch()` automatically rollback

```typescript
try {
  await db.transaction(async (ctx) => {
    const user = await ctx.insert(user).values({ /* ... */ }).returning();
    throw new Error('Oops!');
  });
} catch (e) {
  // Transaction is automatically rolled back
}
```

## Performance Tips

1. **Use batches** for bulk operations - they're in a single transaction
2. **Use transactions** instead of separate operations for atomicity
3. **Filter early** in traversals to reduce memory usage
4. **Use unique: true** (default) in variable-depth traversals to avoid revisiting nodes
5. **Call `optimize()`** periodically to merge deltas into snapshots
6. **Close connections** with `db.close()` to flush writes

## Comparison with Lower-Level API

The high-level API (`src/api/`) is built on top of the lower-level database API (`src/db/`):

| Task | Low-Level | High-Level |
|------|-----------|-----------|
| Define types | Manual IDs | `defineNode`, `defineEdge` |
| Type safety | Manual | Full TypeScript inference |
| Insert nodes | `createNode()` + `setNodeProp()` | `insert().values()` |
| Query nodes | Raw ID lookups | `get()`, `traversals` |
| Transactions | `beginTx()`, `commit()`, `rollback()` | `transaction()`, `batch()` |

The low-level API is useful when you need escape-hatch access via `db.$raw`.
