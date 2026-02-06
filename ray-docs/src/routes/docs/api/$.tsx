import { createFileRoute, useLocation } from '@tanstack/solid-router'
import { Show } from 'solid-js'
import DocPage from '~/components/doc-page'
import CodeBlock from '~/components/code-block'
import { findDocBySlug } from '~/lib/docs'

export const Route = createFileRoute('/docs/api/$')({
  component: ApiSplatPage,
})

function ApiSplatPage() {
  const location = useLocation()
  const slug = () => {
    const path = location().pathname
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
          The API reference <code class="px-2 py-1 bg-slate-100 dark:bg-slate-800 rounded">{props.slug}</code> doesn't exist yet.
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

  if (slug === 'api/high-level') {
    return (
      <DocPage slug={slug}>
        <p>
          The high-level API provides a Drizzle-style fluent interface for 
          working with your graph database.
        </p>

        <h2 id="kite-function">kite()</h2>
        <p>Initialize the database connection.</p>
        <CodeBlock
          code={`import { kite } from '@kitedb/core';

const db = await kite(path, options);`}
          language="typescript"
        />

        <h2 id="node-methods">Node Methods</h2>
        <CodeBlock
          code={`// Create nodes
db.insert(user).values({ key: "alice", name: "Alice" }).returning()
db.insert(user).valuesMany([{ key: "a" }, { key: "b" }]).execute()

// Upsert by key
db.upsert(user).values({ key: "alice", email: "a@x.com" }).execute()

// Read
db.get(user, "alice")
db.getRef(user, "alice")

// Update by key
db.update(user, "alice").setAll({ name: "Alice V2" }).execute()

// Delete by key
db.delete(user, "alice")

// List / count
db.all(user)
db.countNodes()
db.countNodes(user)`}
          language="typescript"
        />

        <h2 id="edge-methods">Edge Methods</h2>
        <CodeBlock
          code={`// Create edge
db.link(src, follows, dst, { since: 2024 })
db.link(src).to(dst).via(follows).props({ since: 2024 }).execute()

// Delete / check
db.unlink(src, follows, dst)
db.hasEdge(src, follows, dst)

// Update edge props
db.updateEdge(src, follows, dst).setAll({ weight: 0.8 }).execute()

// List / count
db.allEdges()
db.allEdges(follows)
db.countEdges()
db.countEdges(follows)`}
          language="typescript"
        />

        <h2 id="next-steps">Next Steps</h2>
        <ul>
          <li><a href="/docs/api/low-level">Low-Level API</a> – Direct database primitives</li>
          <li><a href="/docs/api/vector-api">Vector API</a> – Similarity search</li>
        </ul>
      </DocPage>
    )
  }

  if (slug === 'api/low-level') {
    return (
      <DocPage slug={slug}>
        <p>
          The low-level API uses the <code>Database</code> class for direct
          graph operations, transaction control, and batched writes.
        </p>

        <h2 id="storage-access">Open and Write</h2>
        <CodeBlock
          code={`import { Database, PropType } from '@kitedb/core';

const db = Database.open('./data.kitedb', { createIfMissing: true });

db.begin();
try {
  const nodeId = db.createNode('user:alice');
  db.setNodePropByName(nodeId, 'name', {
    propType: PropType.String,
    stringValue: 'Alice',
  });

  db.commit();
} catch (err) {
  db.rollback();
  throw err;
}`}
          language="typescript"
        />

        <h2 id="batch-operations">Batch Operations</h2>
        <CodeBlock
          code={`// High-throughput bulk ingest
db.beginBulk();
const nodeIds = db.createNodesBatch(keys); // Array<string | null>
db.addEdgesBatch(edges);                   // Array<{ src, etype, dst }>
db.addEdgesWithPropsBatch(edgesWithProps);
db.commit();

// Optional maintenance checkpoint after ingest
db.checkpoint();`}
          language="typescript"
        />

        <h2 id="iterators">Streaming and Pagination</h2>
        <CodeBlock
          code={`// Stream nodes in fixed-size chunks
for (const batch of db.streamNodes({ batchSize: 1000 })) {
  for (const nodeId of batch) {
    // process nodeId
  }
}

// Cursor pagination
let cursor: string | undefined = undefined;
do {
  const page = db.getNodesPage({ limit: 100, cursor });
  for (const node of page.items) {
    // process node
  }
  cursor = page.nextCursor;
} while (cursor);`}
          language="typescript"
        />
      </DocPage>
    )
  }

  if (slug === 'api/vector-api') {
    return (
      <DocPage slug={slug}>
        <p>
          Complete reference for KiteDB's vector search capabilities.
        </p>

        <h2 id="vector-property">Defining Vector Properties</h2>
        <CodeBlock
          code={`import { vector } from '@kitedb/core';

// Define with dimensions
embedding: vector('embedding', 1536)`}
          language="typescript"
        />

        <h2 id="similarity-methods">Similarity Search Methods</h2>
        <CodeBlock
          code={`import { createVectorIndex } from '@kitedb/core';

const index = createVectorIndex({ dimensions: 1536 });

// Add vectors
index.set(nodeId, embedding);

// Search
const hits = index.search(queryVector, {
  k: 10,          // Max results
  threshold: 0.8, // Min similarity score (cosine)
  nProbe: 10,     // IVF probe count (optional)
});`}
          language="typescript"
        />

        <h2 id="indexing">Vector Indexing</h2>
        <CodeBlock
          code={`const index = createVectorIndex({ dimensions: 1536 });

// Build/rebuild IVF index for faster search
index.buildIndex();`}
          language="typescript"
        />
      </DocPage>
    )
  }

  // Default fallback
  return (
    <DocPage slug={slug}>
      <p>This API reference is coming soon.</p>
    </DocPage>
  )
}
