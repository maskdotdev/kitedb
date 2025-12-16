# Ray API - Quick Start Guide

Get up and running with Ray in 5 minutes.

## Installation

```bash
bun add @ray-db/ray
# or
npm add @ray-db/ray
```

## Basic Setup

```typescript
import { ray, defineNode, defineEdge, prop, optional } from '@ray-db/ray';

// 1. Define your schema
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

const knows = defineEdge('knows', {
  since: prop.int('since'),
});

const worksAt = defineEdge('worksAt');

// 2. Open database
const db = await ray('./my-database', {
  nodes: [user, company],
  edges: [knows, worksAt],
});

// 3. Create nodes
const alice = await db
  .insert(user)
  .values({
    key: 'alice',
    name: 'Alice',
    email: 'alice@example.com',
    age: 30,
  })
  .returning();

const bob = await db
  .insert(user)
  .values({
    key: 'bob',
    name: 'Bob',
    email: 'bob@example.com',
  })
  .returning();

// 4. Create relationships
await db.link(alice, knows, bob, { since: 2020 });

// 5. Query graph
const friends = await db
  .from(alice)
  .out(knows)
  .nodes()
  .toArray();

console.log('Alice knows:', friends.map(f => f.name));
// Output: Alice knows: [ 'Bob' ]

// 6. Close database
await db.close();
```

## Common Operations

### Insert Nodes

```typescript
// Single insert
const user1 = await db
  .insert(user)
  .values({ key: 'user1', name: 'User One', email: 'one@example.com' })
  .returning();

// Bulk insert
const [user2, user3] = await db
  .insert(user)
  .values([
    { key: 'user2', name: 'User Two', email: 'two@example.com' },
    { key: 'user3', name: 'User Three', email: 'three@example.com' },
  ])
  .returning();
```

### Get Node by Key

```typescript
const alice = await db.get(user, 'alice');
// Returns: { $id: 1n, $key: 'user:alice', name: 'Alice', email: '...', age: 30 }

if (!alice) {
  console.log('User not found');
}
```

### Update Node

```typescript
// By reference
const updated = await db
  .update(alice)
  .set({ name: 'Alice Updated', age: 31 })
  .execute();

// Or by key
await db
  .update(user)
  .set({ email: 'newemail@example.com' })
  .where({ $key: 'user:alice' })
  .execute();
```

### Delete Node

```typescript
// By reference
const deleted = await db.delete(alice);
// Returns: true if deleted, false if not found

// Or by key
const deleted = await db
  .delete(user)
  .where({ $key: 'user:alice' })
  .execute();
```

### Create Relationships (Edges)

```typescript
const alice = await db.get(user, 'alice');
const bob = await db.get(user, 'bob');

// Link with properties
await db.link(alice, knows, bob, { since: 2020 });

// Link without properties
await db.link(alice, worksAt, acme);

// Check if edge exists
const hasEdge = await db.hasEdge(alice, knows, bob);
```

### Remove Relationships

```typescript
await db.unlink(alice, knows, bob);
```

### Traverse the Graph

```typescript
const alice = await db.get(user, 'alice');

// Get immediate neighbors
const friends = await db
  .from(alice)
  .out(knows)
  .nodes()
  .toArray();

// Get friends of friends
const foaf = await db
  .from(alice)
  .out(knows)
  .out(knows)
  .nodes()
  .toArray();

// Bidirectional traversal
const connections = await db
  .from(alice)
  .both(knows)
  .nodes()
  .toArray();

// Variable-depth traversal
const nearby = await db
  .from(alice)
  .traverse(knows, {
    direction: 'out',
    maxDepth: 3,
    minDepth: 1,
    unique: true,
  })
  .nodes()
  .toArray();

// With filters
const youngFriends = await db
  .from(alice)
  .out(knows)
  .whereNode(n => (n.age ?? 0) < 35)
  .nodes()
  .toArray();

// Get edges instead of nodes
const knowsEdges = await db
  .from(alice)
  .out(knows)
  .edges()
  .toArray();

// Lazy iteration
for await (const friend of db.from(alice).out(knows).nodes()) {
  console.log(friend.name);
}

// First/count
const firstFriend = await db.from(alice).out(knows).first();
const friendCount = await db.from(alice).out(knows).count();
```

### Transactions

```typescript
const result = await db.transaction(async (ctx) => {
  const alice = await ctx
    .insert(user)
    .values({
      key: 'alice',
      name: 'Alice',
      email: 'alice@example.com',
    })
    .returning();

  const bob = await ctx
    .insert(user)
    .values({
      key: 'bob',
      name: 'Bob',
      email: 'bob@example.com',
    })
    .returning();

  await ctx.link(alice, knows, bob, { since: 2020 });

  return { alice, bob };
});

// If any operation throws, entire transaction is rolled back
```

### Batch Operations

```typescript
const [result1, result2, result3] = await db.batch([
  db.insert(user).values({ key: 'u1', name: 'User 1', email: 'u1@example.com' }),
  db.insert(user).values({ key: 'u2', name: 'User 2', email: 'u2@example.com' }),
  db.insert(company).values({ key: 'acme', name: 'ACME Corp' }),
]);
```

## Tips & Tricks

### Property Types

- `prop.string()` → `string`
- `prop.int()` → `bigint` (64-bit signed)
- `prop.float()` → `number` (f64)
- `prop.bool()` → `boolean`

Optional properties:

```typescript
const age = optional(prop.int('age'));
// or
const age = prop.int('age').optional();
```

### Keys

Keys are how you look up nodes. Design them carefully:

```typescript
// Good: hierarchical, human-readable
const user = defineNode('user', {
  key: (id: string) => `user:${id}`,
});

const comment = defineNode('comment', {
  key: (id: number) => `comment:${id}`,
});

const postComment = defineNode('postComment', {
  key: (postId: number, commentId: number) => `post:${postId}:comment:${commentId}`,
});
```

### Escape Hatch

If you need low-level access:

```typescript
const rawDb: GraphDB = db.$raw;
// Use raw graph-db.ts API
```

### Maintenance

```typescript
// Get stats
const stats = await db.stats();
console.log('Nodes:', stats.snapshotNodes);
console.log('Edges:', stats.snapshotEdges);
console.log('Should compact:', stats.recommendCompact);

// Check integrity
const check = await db.check();
if (!check.valid) {
  console.error('Database errors:', check.errors);
}

// Compact (merge deltas into snapshots)
await db.optimize();

// Close
await db.close();
```

## Common Patterns

### Many-to-Many Relationship

```typescript
const person = defineNode('person', {
  key: (id: string) => `person:${id}`,
  props: { name: prop.string('name') },
});

const project = defineNode('project', {
  key: (id: string) => `project:${id}`,
  props: { title: prop.string('title') },
});

const contributesTo = defineEdge('contributesTo', {
  role: prop.string('role'),
  hours: prop.float('hours'),
});

// Create relationships
const alice = await db.get(person, 'alice');
const projectA = await db.get(project, 'projectA');
const projectB = await db.get(project, 'projectB');

await db.link(alice, contributesTo, projectA, { role: 'lead', hours: 40.0 });
await db.link(alice, contributesTo, projectB, { role: 'contributor', hours: 20.0 });

// Query
const projects = await db
  .from(alice)
  .out(contributesTo)
  .nodes()
  .toArray();

const contributorCount = await db
  .from(projectA)
  .in(contributesTo)
  .count();
```

### Hierarchical Structure

```typescript
const category = defineNode('category', {
  key: (id: string) => `category:${id}`,
  props: { name: prop.string('name') },
});

const parentOf = defineEdge('parentOf');

const root = await db.get(category, 'root');
const allDescendants = await db
  .from(root)
  .traverse(parentOf, { direction: 'out', maxDepth: 10 })
  .nodes()
  .toArray();
```

### Time-Indexed Relationships

```typescript
const event = defineEdge('event', {
  timestamp: prop.int('timestamp'),
});

const recentEvents = await db
  .from(node)
  .out(event)
  .whereEdge(e => e.timestamp > Date.now() - 86400000) // Last 24h
  .nodes()
  .toArray();
```

## Error Handling

```typescript
try {
  const result = await db.transaction(async (ctx) => {
    // Operations...
  });
} catch (e) {
  // Transaction automatically rolled back
  console.error('Transaction failed:', e);
}

// Keys & IDs
const node = await db.get(user, 'alice'); // Returns null if not found
if (!node) {
  console.log('User not found');
}
```

## Performance Tips

1. **Use batches** for bulk inserts
2. **Use transactions** for atomicity
3. **Filter early** in traversals to reduce memory
4. **Call `optimize()`** periodically
5. **Close connections** with `db.close()` when done
6. **Use `unique: true`** (default) in variable-depth traversals

## See Also

- [Full API Documentation](./README.md)
- [Architecture Overview](../API.md)
- [Type Inference Guide](#type-inference)

## Type Inference

The API automatically infers correct types:

```typescript
const user = defineNode('user', {
  key: (id: string) => `user:${id}`,
  props: {
    name: prop.string('name'),
    age: optional(prop.int('age')),
  },
});

// Insert type (what you pass to .values())
type InsertUser = InferNodeInsert<typeof user>;
// { key: string; name: string; age?: bigint; }

// Return type (what you get back)
type User = InferNode<typeof user>;
// { $id: bigint; $key: string; name: string; age?: bigint; }

// Edge props
const knows = defineEdge('knows', { since: prop.int('since') });
type KnowsProps = InferEdgeProps<typeof knows>;
// { since: bigint; }
```

## Troubleshooting

**Q: "Unknown edge type" error**
A: Make sure you passed all edge definitions to `ray()` options

**Q: Node not found in updates**
A: WHERE clause throws if node doesn't exist. Use `get()` first if unsure

**Q: Type errors with properties**
A: Properties are stored as `bigint` (int), `number` (float), `string`, or `boolean`. No `Date` objects

**Q: Traversal seems slow**
A: Call `optimize()` to compact snapshots. Check `stats()` to see if you need compaction
