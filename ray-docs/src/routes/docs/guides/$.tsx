import { createFileRoute, useLocation } from '@tanstack/solid-router'
import { Show } from 'solid-js'
import DocPage from '~/components/doc-page'
import CodeBlock from '~/components/code-block'
import { findDocBySlug } from '~/lib/docs'

export const Route = createFileRoute('/docs/guides/$')({
  component: GuidesSplatPage,
})

function GuidesSplatPage() {
  // Parse slug from URL path since params seem to not work
  const location = useLocation()
  const slug = () => {
    const path = location().pathname
    // Extract everything after /docs/
    const match = path.match(/^\/docs\/(.+)$/)
    return match ? match[1] : ''
  }
  const doc = () => findDocBySlug(slug())

  return (
    <Show
      when={doc()}
      fallback={<DocNotFound slug={slug()} />}
    >
      <DocPageContent slug={slug()} />
    </Show>
  )
}

function DocNotFound(props: { slug: string }) {
  return (
    <div class="max-w-4xl mx-auto px-6 py-12">
      <div class="text-center">
        <h1 class="text-4xl font-extrabold text-slate-900 dark:text-white mb-4">
          Page Not Found
        </h1>
        <p class="text-lg text-slate-600 dark:text-slate-400 mb-8">
          The guide <code class="px-2 py-1 bg-slate-100 dark:bg-slate-800 rounded">{props.slug}</code> doesn't exist yet.
        </p>
        <a
          href="/docs"
          class="inline-flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-cyan-500 to-violet-500 text-white font-semibold rounded-xl hover:shadow-lg hover:shadow-cyan-500/25 transition-all duration-200"
        >
          Back to Documentation
        </a>
      </div>
    </div>
  )
}

function DocPageContent(props: { slug: string }) {
  const slug = props.slug

  if (slug === 'guides/schema') {
    return (
      <DocPage slug={slug}>
        <p>
          RayDB schemas provide type-safe definitions for your graph data. 
          This guide covers all the ways to define nodes, edges, and properties.
        </p>

        <h2 id="defining-nodes">Defining Nodes</h2>
        <p>
          Nodes are the vertices in your graph. Each node type needs a unique name 
          and a key function that generates unique identifiers.
        </p>
        <CodeBlock
          code={`import { defineNode, prop } from '@ray-db/ray';

const article = defineNode('article', {
  // Key function receives your input and returns a unique key
  key: (id: string) => \`article:\${id}\`,
  
  // Properties with their types
  props: {
    title: prop.string('title'),
    content: prop.text('content'),
    published: prop.boolean('published'),
    views: prop.integer('views'),
    rating: prop.float('rating'),
    tags: prop.array('tags', prop.string()),
    metadata: prop.json('metadata'),
    createdAt: prop.date('created_at'),
  },
});`}
          language="typescript"
          filename="nodes.ts"
        />

        <h2 id="property-types">Property Types</h2>
        <p>RayDB supports the following property types:</p>
        <ul>
          <li><code>prop.string()</code> – Text strings</li>
          <li><code>prop.text()</code> – Long text content</li>
          <li><code>prop.integer()</code> – Whole numbers</li>
          <li><code>prop.float()</code> – Decimal numbers</li>
          <li><code>prop.boolean()</code> – True/false values</li>
          <li><code>prop.date()</code> – Date/time values</li>
          <li><code>prop.array()</code> – Arrays of any type</li>
          <li><code>prop.json()</code> – Arbitrary JSON data</li>
          <li><code>prop.vector()</code> – Float32 embedding vectors</li>
        </ul>

        <h2 id="defining-edges">Defining Edges</h2>
        <p>
          Edges connect nodes and can have their own properties.
        </p>
        <CodeBlock
          code={`import { defineEdge, prop } from '@ray-db/ray';

const authored = defineEdge('authored', {
  from: user,
  to: article,
  props: {
    role: prop.string('role'), // 'author' | 'contributor'
  },
});

const likes = defineEdge('likes', {
  from: user,
  to: article,
  props: {
    likedAt: prop.date('liked_at'),
  },
});`}
          language="typescript"
          filename="edges.ts"
        />

        <h2 id="next-steps">Next Steps</h2>
        <ul>
          <li><a href="/docs/guides/queries">Queries & CRUD</a> – Perform operations on your schema</li>
          <li><a href="/docs/guides/vectors">Vector Search</a> – Add embedding vectors</li>
        </ul>
      </DocPage>
    )
  }

  if (slug === 'guides/queries') {
    return (
      <DocPage slug={slug}>
        <p>
          Learn how to create, read, update, and delete data in RayDB with the 
          high-level fluent API.
        </p>

        <h2 id="create">Creating Nodes</h2>
        <CodeBlock
          code={`// Create a single node
const alice = await db.node(user).create({
  id: 'alice',
  name: 'Alice Chen',
  email: 'alice@example.com',
});

// Create multiple nodes
const users = await db.node(user).createMany([
  { id: 'bob', name: 'Bob', email: 'bob@example.com' },
  { id: 'carol', name: 'Carol', email: 'carol@example.com' },
]);`}
          language="typescript"
        />

        <h2 id="read">Reading Data</h2>
        <CodeBlock
          code={`// Get by key
const user = await db.node(user).get('user:alice');

// Find with conditions
const activeUsers = await db.node(user)
  .where({ status: 'active' })
  .all();

// Get first match
const admin = await db.node(user)
  .where({ role: 'admin' })
  .first();

// Count matches
const count = await db.node(user)
  .where({ verified: true })
  .count();`}
          language="typescript"
        />

        <h2 id="update">Updating Data</h2>
        <CodeBlock
          code={`// Update by key
await db.node(user)
  .update('user:alice', { name: 'Alice C.' });

// Update with conditions
await db.node(user)
  .where({ status: 'pending' })
  .update({ status: 'active' });`}
          language="typescript"
        />

        <h2 id="delete">Deleting Data</h2>
        <CodeBlock
          code={`// Delete by key
await db.node(user).delete('user:alice');

// Delete with conditions
await db.node(user)
  .where({ deleted: true })
  .delete();`}
          language="typescript"
        />

        <h2 id="next-steps">Next Steps</h2>
        <ul>
          <li><a href="/docs/guides/traversal">Graph Traversal</a> – Navigate relationships</li>
          <li><a href="/docs/guides/transactions">Transactions</a> – ACID guarantees</li>
        </ul>
      </DocPage>
    )
  }

  if (slug === 'guides/traversal') {
    return (
      <DocPage slug={slug}>
        <p>
          RayDB provides powerful graph traversal capabilities to navigate 
          relationships between nodes.
        </p>

        <h2 id="basic-traversal">Basic Traversal</h2>
        <CodeBlock
          code={`// Find all users that Alice follows
const following = await db
  .node(user)
  .traverse(follows)
  .from('user:alice')
  .all();

// Find all followers of Bob
const followers = await db
  .node(user)
  .traverse(follows)
  .to('user:bob')
  .all();`}
          language="typescript"
        />

        <h2 id="multi-hop">Multi-Hop Traversal</h2>
        <CodeBlock
          code={`// Find friends of friends (2-hop)
const friendsOfFriends = await db
  .node(user)
  .traverse(follows)
  .from('user:alice')
  .traverse(follows)
  .all();

// With depth limit
const network = await db
  .node(user)
  .traverse(follows)
  .from('user:alice')
  .depth({ min: 1, max: 3 })
  .all();`}
          language="typescript"
        />

        <h2 id="filtering">Filtering During Traversal</h2>
        <CodeBlock
          code={`// Find active users that Alice follows
const activeFollowing = await db
  .node(user)
  .traverse(follows)
  .from('user:alice')
  .where({ status: 'active' })
  .all();`}
          language="typescript"
        />

        <h2 id="next-steps">Next Steps</h2>
        <ul>
          <li><a href="/docs/guides/vectors">Vector Search</a> – Combine with semantic search</li>
          <li><a href="/docs/api/high-level">API Reference</a> – Full traversal API</li>
        </ul>
      </DocPage>
    )
  }

  if (slug === 'guides/vectors') {
    return (
      <DocPage slug={slug}>
        <p>
          RayDB includes built-in vector search for semantic similarity queries. 
          Store embeddings alongside your graph data and find similar nodes.
        </p>

        <h2 id="adding-vectors">Adding Vector Properties</h2>
        <CodeBlock
          code={`import { defineNode, prop } from '@ray-db/ray';

const document = defineNode('document', {
  key: (id: string) => \`doc:\${id}\`,
  props: {
    title: prop.string('title'),
    content: prop.text('content'),
    // 1536-dimensional embedding (OpenAI ada-002)
    embedding: prop.vector('embedding', 1536),
  },
});`}
          language="typescript"
        />

        <h2 id="storing-embeddings">Storing Embeddings</h2>
        <CodeBlock
          code={`// Generate embedding with your preferred provider
const embedding = await openai.embeddings.create({
  model: 'text-embedding-ada-002',
  input: 'Your document content here',
});

// Store in RayDB
await db.node(document).create({
  id: 'doc-1',
  title: 'My Document',
  content: 'Your document content here',
  embedding: embedding.data[0].embedding,
});`}
          language="typescript"
        />

        <h2 id="similarity-search">Similarity Search</h2>
        <CodeBlock
          code={`// Find similar documents
const queryEmbedding = await getEmbedding('search query');

const similar = await db
  .node(document)
  .vector('embedding')
  .similar(queryEmbedding, { limit: 10 })
  .all();

// Returns nodes with similarity scores
similar.forEach(({ node, score }) => {
  console.log(\`\${node.title}: \${score.toFixed(3)}\`);
});`}
          language="typescript"
        />

        <h2 id="hybrid-search">Hybrid Search</h2>
        <CodeBlock
          code={`// Combine vector search with graph traversal
const results = await db
  .node(document)
  .vector('embedding')
  .similar(queryEmbedding)
  .traverse(authored)  // Find authors of similar docs
  .all();`}
          language="typescript"
        />

        <h2 id="next-steps">Next Steps</h2>
        <ul>
          <li><a href="/docs/api/vector-api">Vector API Reference</a> – Full vector API</li>
          <li><a href="/docs/internals/performance">Performance</a> – Optimization tips</li>
        </ul>
      </DocPage>
    )
  }

  if (slug === 'guides/transactions') {
    return (
      <DocPage slug={slug}>
        <p>
          RayDB supports ACID transactions to ensure data consistency.
        </p>

        <h2 id="basic-transactions">Basic Transactions</h2>
        <CodeBlock
          code={`await db.transaction(async (tx) => {
  // All operations in this block are atomic
  const alice = await tx.node(user).create({
    id: 'alice',
    name: 'Alice',
  });
  
  const bob = await tx.node(user).create({
    id: 'bob', 
    name: 'Bob',
  });
  
  await tx.edge(follows).create({
    from: alice,
    to: bob,
  });
  
  // If any operation fails, all changes are rolled back
});`}
          language="typescript"
        />

        <h2 id="isolation">Isolation Levels</h2>
        <CodeBlock
          code={`// Read committed (default)
await db.transaction(async (tx) => {
  // ...
}, { isolation: 'read-committed' });

// Serializable for strict consistency
await db.transaction(async (tx) => {
  // ...
}, { isolation: 'serializable' });`}
          language="typescript"
        />

        <h2 id="next-steps">Next Steps</h2>
        <ul>
          <li><a href="/docs/api/high-level">API Reference</a> – Full transaction API</li>
          <li><a href="/docs/internals/architecture">Architecture</a> – How transactions work</li>
        </ul>
      </DocPage>
    )
  }

  // Default fallback
  return (
    <DocPage slug={slug}>
      <p>This guide is coming soon.</p>
    </DocPage>
  )
}
