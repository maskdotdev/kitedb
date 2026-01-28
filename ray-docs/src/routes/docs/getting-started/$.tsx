import { createFileRoute, useLocation } from '@tanstack/solid-router'
import { Show } from 'solid-js'
import DocPage from '~/components/DocPage'
import CodeBlock from '~/components/CodeBlock'
import { findDocBySlug } from '~/lib/docs'

export const Route = createFileRoute('/docs/getting-started/$')({
  component: GettingStartedSplatPage,
})

function GettingStartedSplatPage() {
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
          The page <code class="px-2 py-1 bg-slate-100 dark:bg-slate-800 rounded">{props.slug}</code> doesn't exist yet.
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

  if (slug === 'getting-started/quick-start') {
    return (
      <DocPage slug={slug}>
        <p>
          Let's build a simple social graph database with users and their connections. 
          By the end of this guide, you'll understand the core concepts of RayDB.
        </p>

        <h2 id="create-schema">1. Define Your Schema</h2>
        <p>
          RayDB uses a type-safe schema to define nodes and edges. Let's create a simple 
          social network with users and follow relationships.
        </p>
        <CodeBlock
          code={`import { ray, defineNode, defineEdge, prop } from '@ray-db/ray';

// Define a user node
const user = defineNode('user', {
  key: (id: string) => \`user:\${id}\`,
  props: {
    name: prop.string('name'),
    email: prop.string('email'),
    createdAt: prop.date('created_at'),
  },
});

// Define a follow relationship
const follows = defineEdge('follows', {
  from: user,
  to: user,
  props: {
    followedAt: prop.date('followed_at'),
  },
});`}
          language="typescript"
          filename="schema.ts"
        />

        <h2 id="initialize">2. Initialize the Database</h2>
        <CodeBlock
          code={`const db = await ray('./my-app.raydb', {
  nodes: [user],
  edges: [follows],
});

console.log('Database initialized!');`}
          language="typescript"
        />

        <h2 id="add-data">3. Add Some Data</h2>
        <CodeBlock
          code={`// Create users
const alice = await db.node(user).create({
  id: 'alice',
  name: 'Alice Chen',
  email: 'alice@example.com',
  createdAt: new Date(),
});

const bob = await db.node(user).create({
  id: 'bob',
  name: 'Bob Smith',
  email: 'bob@example.com',
  createdAt: new Date(),
});

// Create a follow relationship
await db.edge(follows).create({
  from: alice,
  to: bob,
  followedAt: new Date(),
});`}
          language="typescript"
        />

        <h2 id="query">4. Query the Graph</h2>
        <CodeBlock
          code={`// Find all users Alice follows
const following = await db
  .node(user)
  .traverse(follows)
  .where({ from: alice })
  .all();

console.log('Alice follows:', following.map(u => u.name));

// Find who follows Bob
const followers = await db
  .node(user)
  .traverse(follows)
  .where({ to: bob })
  .all();

console.log('Bob has followers:', followers.length);`}
          language="typescript"
        />

        <h2 id="cleanup">5. Close the Database</h2>
        <CodeBlock
          code={`// Always close when done
await db.close();`}
          language="typescript"
        />

        <h2 id="next-steps">Next Steps</h2>
        <p>
          Congratulations! You've built your first graph database with RayDB. 
          Continue learning with these guides:
        </p>
        <ul>
          <li><a href="/docs/guides/schema">Schema Definition</a> – Advanced schema patterns</li>
          <li><a href="/docs/guides/queries">Queries & CRUD</a> – All query operations</li>
          <li><a href="/docs/guides/vectors">Vector Search</a> – Semantic similarity</li>
        </ul>
      </DocPage>
    )
  }

  // Default fallback
  return (
    <DocPage slug={slug}>
      <p>This getting started guide is coming soon.</p>
    </DocPage>
  )
}
