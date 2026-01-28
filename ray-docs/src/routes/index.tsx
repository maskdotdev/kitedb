import { createFileRoute, Link } from '@tanstack/solid-router'
import { For } from 'solid-js'
import {
  Zap,
  Database,
  GitBranch,
  Shield,
  Search,
  Sparkles,
  ArrowRight,
  BookOpen,
  Rocket,
  Code,
  Terminal,
  Cpu,
  Network,
  Box,
} from 'lucide-solid'
import Logo from '~/components/Logo'
import ThemeToggle from '~/components/ThemeToggle'
import { StatCard, StatGrid } from '~/components/StatCard'
import { Card, CardGrid } from '~/components/Card'
import CodeBlock from '~/components/CodeBlock'
import Tabs from '~/components/Tabs'

export const Route = createFileRoute('/')({
  component: HomePage,
})

function HomePage() {
  const features = [
    {
      icon: <Database class="w-5 h-5" aria-hidden="true" />,
      title: 'Graph + Vector',
      description: 'Combine graph traversals with vector similarity search in a single, unified API.',
    },
    {
      icon: <Zap class="w-5 h-5" aria-hidden="true" />,
      title: 'Blazing Fast',
      description: '~125ns node lookups, ~1.1μs traversals. 118× faster than Memgraph.',
    },
    {
      icon: <Shield class="w-5 h-5" aria-hidden="true" />,
      title: 'Type-Safe',
      description: 'Full TypeScript inference. Define once, get types everywhere. No codegen.',
    },
    {
      icon: <GitBranch class="w-5 h-5" aria-hidden="true" />,
      title: 'MVCC Transactions',
      description: 'Snapshot isolation with non-blocking readers. Writers never block readers.',
    },
    {
      icon: <Search class="w-5 h-5" aria-hidden="true" />,
      title: 'HNSW Vector Index',
      description: 'Hierarchical Navigable Small World graphs for O(log n) nearest neighbor search.',
    },
    {
      icon: <Sparkles class="w-5 h-5" aria-hidden="true" />,
      title: 'Zero Dependencies',
      description: 'Single-file storage. Easy backup, sync, and deployment. SQLite-inspired.',
    },
  ]

  const architectureFeatures = [
    {
      icon: <Cpu class="w-5 h-5" aria-hidden="true" />,
      title: 'CSR Storage Format',
      description: 'Compressed Sparse Row format for cache-efficient graph traversal. Memory-mapped for zero-copy reads.',
    },
    {
      icon: <Shield class="w-5 h-5" aria-hidden="true" />,
      title: 'MVCC Transactions',
      description: 'Snapshot isolation with non-blocking readers. Writers never block readers.',
    },
    {
      icon: <Network class="w-5 h-5" aria-hidden="true" />,
      title: 'HNSW Vector Index',
      description: 'Hierarchical Navigable Small World graphs for approximate nearest neighbor search in O(log n).',
    },
    {
      icon: <Box class="w-5 h-5" aria-hidden="true" />,
      title: 'Single-File Storage',
      description: 'SQLite-inspired single-file format. Easy backup, sync, and deployment.',
    },
  ]

  const schemaCode = `import { ray, defineNode, defineEdge, prop } from '@ray-db/ray';

// Define nodes with typed properties
const Document = defineNode('document', {
  key: (id: string) => \`doc:\${id}\`,
  props: {
    title: prop.string('title'),
    content: prop.string('content'),
    embedding: prop.vector('embedding', 1536),
  },
});

const Topic = defineNode('topic', {
  key: (name: string) => \`topic:\${name}\`,
  props: { name: prop.string('name') },
});

// Define typed edges
const discusses = defineEdge('discusses', {
  relevance: prop.float('relevance'),
});

// Open database with schema
const db = await ray('./knowledge.raydb', {
  nodes: [Document, Topic],
  edges: [discusses],
});`

  const traversalCode = `// Find all topics discussed by Alice's documents
const topics = await db
  .from(alice)
  .out('wrote')           // Alice -> Document
  .out('discusses')       // Document -> Topic
  .unique()
  .toArray();

// Multi-hop with filtering
const results = await db
  .from(startNode)
  .out('knows', { where: { since: { gt: 2020n } } })
  .out('worksAt')
  .filter(company => company.props.employees > 100)
  .limit(10)
  .toArray();`

  const vectorCode = `// Find similar documents
const similar = await db.similar(Document, queryEmbedding, {
  k: 10,
  threshold: 0.8,
});

// Combine with graph context
const contextual = await Promise.all(
  similar.map(async (doc) => ({
    document: doc,
    topics: await db.from(doc).out('discusses').toArray(),
    related: await db.from(doc).out('relatedTo').limit(5).toArray(),
  }))
);`

  const crudCode = `// Insert with returning
const doc = await db.insert(Document)
  .values({
    key: 'doc-1',
    title: 'Getting Started',
    content: 'Welcome to RayDB...',
    embedding: await embed('Welcome to RayDB...'),
  })
  .returning();

// Create relationships
await db.link(doc, discusses, topic, { relevance: 0.95 });

// Update properties
await db.update(Document)
  .set({ title: 'Updated Title' })
  .where({ key: 'doc-1' });`

  return (
    <div class="min-h-screen bg-[#030712]">
      {/* Skip link */}
      <a
        href="#main-content"
        class="sr-only focus:not-sr-only focus:absolute focus:top-4 focus:left-4 focus:z-50 focus:px-4 focus:py-2 focus:bg-[#00d4ff] focus:text-black focus:rounded-lg focus:font-semibold"
      >
        Skip to main content
      </a>

      {/* Header */}
      <header class="sticky top-0 z-50 border-b border-[#1e3a5f]/50 bg-[#030712]/80 backdrop-blur-xl">
        <nav class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8" aria-label="Main navigation">
          <div class="flex items-center justify-between h-16">
            <Link to="/" class="flex items-center gap-2.5 group" aria-label="RayDB Home">
              <Logo size={32} />
              <span class="text-xl font-bold text-gradient">RayDB</span>
            </Link>
            
            <div class="hidden md:flex items-center gap-1">
              <Link 
                to="/docs" 
                class="px-4 py-2 text-sm font-medium text-slate-400 hover:text-white rounded-lg hover:bg-white/5 transition-colors duration-150"
              >
                Documentation
              </Link>
              <a 
                href="/docs/api/high-level"
                class="px-4 py-2 text-sm font-medium text-slate-400 hover:text-white rounded-lg hover:bg-white/5 transition-colors duration-150"
              >
                API Reference
              </a>
              <a 
                href="/docs/benchmarks"
                class="px-4 py-2 text-sm font-medium text-slate-400 hover:text-white rounded-lg hover:bg-white/5 transition-colors duration-150"
              >
                Benchmarks
              </a>
            </div>

            <div class="flex items-center gap-1">
              <ThemeToggle />
              <a
                href="https://github.com/maskdotdev/ray"
                target="_blank"
                rel="noopener noreferrer"
                class="p-2.5 rounded-lg text-slate-400 hover:bg-white/5 hover:text-white transition-colors duration-150"
                aria-label="View RayDB on GitHub"
              >
                <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24" aria-hidden="true">
                  <path fill-rule="evenodd" d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z" clip-rule="evenodd" />
                </svg>
              </a>
            </div>
          </div>
        </nav>
      </header>

      <main id="main-content">
        {/* Hero Section */}
        <section class="relative pt-24 pb-32 sm:pt-32 sm:pb-40 overflow-hidden" aria-labelledby="hero-heading">
          {/* Background glow orbs */}
          <div class="absolute inset-0 -z-10 overflow-hidden" aria-hidden="true">
            <div class="hero-glow w-[800px] h-[800px] -top-[400px] left-1/2 -translate-x-1/2 animate-glow-pulse" />
            <div class="hero-glow w-[600px] h-[600px] top-[100px] -left-[200px] animate-glow-pulse animate-delay-200" />
            <div class="hero-glow w-[500px] h-[500px] top-[200px] -right-[100px] animate-glow-pulse animate-delay-400" />
            
            {/* Grid pattern */}
            <div class="absolute inset-0 bg-[linear-gradient(rgba(0,212,255,0.03)_1px,transparent_1px),linear-gradient(90deg,rgba(0,212,255,0.03)_1px,transparent_1px)] bg-[size:64px_64px] [mask-image:radial-gradient(ellipse_at_center,black_30%,transparent_70%)]" />
          </div>

          <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div class="text-center">
              {/* Main heading */}
              <h1 
                id="hero-heading"
                class="text-5xl sm:text-6xl md:text-7xl lg:text-8xl font-black tracking-tight text-balance animate-slide-up"
              >
                <span class="block text-white">The Graph Database</span>
                <span class="block mt-2 text-gradient neon-glow">That Speaks TypeScript</span>
              </h1>
              
              {/* Tagline */}
              <p class="mt-8 max-w-2xl mx-auto text-lg sm:text-xl text-slate-400 text-pretty leading-relaxed animate-slide-up animate-delay-100">
                Combine lightning-fast graph traversals with vector similarity search in a single, zero-dependency package. Built for Bun and modern TypeScript.
              </p>

              {/* CTA buttons */}
              <div class="mt-12 flex flex-col sm:flex-row items-center justify-center gap-4 animate-slide-up animate-delay-200">
                <Link
                  to="/docs/getting-started/installation"
                  class="group inline-flex items-center gap-2 px-8 py-4 text-base font-semibold text-black bg-[#00d4ff] rounded-xl shadow-[0_0_30px_rgba(0,212,255,0.4)] hover:shadow-[0_0_50px_rgba(0,212,255,0.6)] hover:scale-[1.02] active:scale-[0.98] transition-all duration-200"
                >
                  Get Started
                  <ArrowRight size={18} class="group-hover:translate-x-0.5 transition-transform duration-150" aria-hidden="true" />
                </Link>
                <a
                  href="https://github.com/maskdotdev/ray"
                  target="_blank"
                  rel="noopener noreferrer"
                  class="inline-flex items-center gap-2 px-8 py-4 text-base font-semibold text-white bg-white/5 border border-white/10 rounded-xl hover:bg-white/10 hover:border-white/20 active:scale-[0.98] transition-all duration-200"
                >
                  <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24" aria-hidden="true">
                    <path fill-rule="evenodd" d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z" clip-rule="evenodd" />
                  </svg>
                  View on GitHub
                </a>
              </div>

              {/* Install command */}
              <div class="mt-12 flex justify-center animate-slide-up animate-delay-300">
                <div class="group relative inline-flex items-center gap-4 px-6 py-4 bg-[#0a1628] rounded-xl border border-[#1e3a5f] shadow-[0_0_30px_rgba(0,0,0,0.3)]">
                  <Terminal size={18} class="text-slate-500" aria-hidden="true" />
                  <code class="text-sm font-mono">
                    <span class="text-slate-500">$</span>
                    <span class="text-[#00d4ff] ml-2">bun add</span>
                    <span class="text-white ml-2">@ray-db/ray</span>
                  </code>
                  <button
                    type="button"
                    class="p-2 rounded-lg text-slate-500 hover:text-[#00d4ff] hover:bg-white/5 transition-colors duration-150"
                    aria-label="Copy install command"
                    onClick={() => navigator.clipboard.writeText('bun add @ray-db/ray')}
                  >
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                    </svg>
                  </button>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* Stats Section */}
        <section class="py-20 border-y border-[#1e3a5f]/50 bg-[#0a1628]/50" aria-labelledby="stats-heading">
          <h2 id="stats-heading" class="sr-only">Performance Statistics</h2>
          <div class="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8">
            <StatGrid>
              <StatCard value="~125ns" label="Node Lookup" />
              <StatCard value="~1.1μs" label="1-Hop Traversal" />
              <StatCard value="Zero" label="Dependencies" />
              <StatCard value="100%" label="TypeScript" />
            </StatGrid>
          </div>
        </section>

        {/* Features Grid */}
        <section class="py-28" aria-labelledby="features-heading">
          <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div class="text-center mb-16">
              <h2 id="features-heading" class="text-3xl sm:text-4xl font-bold text-white text-balance">
                Built for Modern AI Applications
              </h2>
              <p class="mt-4 text-lg text-slate-400 max-w-2xl mx-auto text-pretty">
                Everything you need to build knowledge graphs, RAG pipelines, and recommendation systems.
              </p>
            </div>

            <CardGrid columns={3}>
              <For each={features}>
                {(feature) => (
                  <Card
                    title={feature.title}
                    description={feature.description}
                    icon={feature.icon}
                  />
                )}
              </For>
            </CardGrid>
          </div>
        </section>

        {/* Code Examples - Schema */}
        <section class="py-28 bg-[#0a1628]/30" aria-labelledby="schema-heading">
          <div class="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8">
            <div class="text-center mb-16">
              <h2 id="schema-heading" class="text-3xl sm:text-4xl font-bold text-white text-balance">
                Type-Safe by Design
              </h2>
              <p class="mt-4 text-lg text-slate-400 text-pretty">
                Define your schema once, get full TypeScript inference everywhere. No codegen needed.
              </p>
            </div>

            <CodeBlock
              code={schemaCode}
              language="typescript"
              filename="schema.ts"
            />
          </div>
        </section>

        {/* Query API Examples */}
        <section class="py-28" aria-labelledby="api-heading">
          <div class="max-w-5xl mx-auto px-4 sm:px-6 lg:px-8">
            <div class="text-center mb-16">
              <h2 id="api-heading" class="text-3xl sm:text-4xl font-bold text-white text-balance">
                Intuitive Query API
              </h2>
              <p class="mt-4 text-lg text-slate-400 text-pretty">
                Fluent, chainable queries that feel natural to TypeScript developers.
              </p>
            </div>

            <Tabs
              items={[
                {
                  label: 'Graph Traversal',
                  code: traversalCode,
                  language: 'typescript',
                },
                {
                  label: 'Vector Search',
                  code: vectorCode,
                  language: 'typescript',
                },
                {
                  label: 'CRUD Operations',
                  code: crudCode,
                  language: 'typescript',
                },
              ]}
            />
          </div>
        </section>

        {/* Architecture Section */}
        <section class="py-28 bg-[#0a1628]/30" aria-labelledby="architecture-heading">
          <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div class="text-center mb-16">
              <h2 id="architecture-heading" class="text-3xl sm:text-4xl font-bold text-white text-balance">
                How It Works
              </h2>
              <p class="mt-4 text-lg text-slate-400 max-w-2xl mx-auto text-pretty">
                Purpose-built architecture for maximum performance with minimal complexity.
              </p>
            </div>

            <CardGrid columns={4}>
              <For each={architectureFeatures}>
                {(feature) => (
                  <Card
                    title={feature.title}
                    description={feature.description}
                    icon={feature.icon}
                  />
                )}
              </For>
            </CardGrid>
          </div>
        </section>

        {/* Use Cases */}
        <section class="py-28" aria-labelledby="usecases-heading">
          <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div class="text-center mb-16">
              <h2 id="usecases-heading" class="text-3xl sm:text-4xl font-bold text-white text-balance">
                Perfect For
              </h2>
            </div>

            <CardGrid columns={4}>
              <Card
                title="RAG Pipelines"
                description="Store document chunks with embeddings and traverse relationships for context-aware retrieval."
                icon={<BookOpen class="w-5 h-5" aria-hidden="true" />}
              />
              <Card
                title="Knowledge Graphs"
                description="Model complex relationships between entities with semantic similarity search."
                icon={<GitBranch class="w-5 h-5" aria-hidden="true" />}
              />
              <Card
                title="Recommendations"
                description="Combine user-item graphs with embedding similarity for hybrid recommendations."
                icon={<Sparkles class="w-5 h-5" aria-hidden="true" />}
              />
              <Card
                title="Local-First Apps"
                description="Embedded architecture with single-file storage. No external database needed."
                icon={<Database class="w-5 h-5" aria-hidden="true" />}
              />
            </CardGrid>
          </div>
        </section>

        {/* CTA Section */}
        <section class="py-28 bg-[#0a1628]/30" aria-labelledby="cta-heading">
          <div class="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
            <h2 id="cta-heading" class="text-3xl sm:text-4xl font-bold text-white text-balance">
              Ready to Get Started?
            </h2>
            <p class="mt-4 text-lg text-slate-400 text-pretty">
              Build your first graph database in 5 minutes with our Quick Start guide.
            </p>

            <div class="mt-12 grid sm:grid-cols-2 gap-6 max-w-2xl mx-auto">
              <Link
                to="/docs/getting-started/installation"
                class="group flex items-center gap-4 p-6 rounded-2xl bg-[#0a1628] border border-[#1e3a5f] hover:border-[#00d4ff]/50 hover:shadow-[0_0_30px_rgba(0,212,255,0.1)] transition-all duration-200"
              >
                <div class="flex-shrink-0 w-14 h-14 flex items-center justify-center rounded-xl bg-[#00d4ff]/10 text-[#00d4ff] group-hover:bg-[#00d4ff]/20 group-hover:scale-110 transition-all duration-200">
                  <Rocket class="w-7 h-7" aria-hidden="true" />
                </div>
                <div class="text-left min-w-0">
                  <div class="font-semibold text-white group-hover:text-[#00d4ff] transition-colors duration-150">
                    Installation Guide
                  </div>
                  <div class="text-sm text-slate-500">Set up in 2 minutes</div>
                </div>
              </Link>

              <a
                href="/docs/getting-started/quick-start"
                class="group flex items-center gap-4 p-6 rounded-2xl bg-[#0a1628] border border-[#1e3a5f] hover:border-[#00d4ff]/50 hover:shadow-[0_0_30px_rgba(0,212,255,0.1)] transition-all duration-200"
              >
                <div class="flex-shrink-0 w-14 h-14 flex items-center justify-center rounded-xl bg-[#00d4ff]/10 text-[#00d4ff] group-hover:bg-[#00d4ff]/20 group-hover:scale-110 transition-all duration-200">
                  <Code class="w-7 h-7" aria-hidden="true" />
                </div>
                <div class="text-left min-w-0">
                  <div class="font-semibold text-white group-hover:text-[#00d4ff] transition-colors duration-150">
                    Quick Start Tutorial
                  </div>
                  <div class="text-sm text-slate-500">Build your first graph</div>
                </div>
              </a>
            </div>
          </div>
        </section>
      </main>

      {/* Footer */}
      <footer class="border-t border-[#1e3a5f]/50 py-12 bg-[#030712]">
        <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div class="flex flex-col sm:flex-row items-center justify-between gap-6">
            <div class="flex items-center gap-2.5">
              <Logo size={24} />
              <span class="font-semibold text-gradient">RayDB</span>
            </div>
            
            <p class="text-sm text-slate-500">
              MIT License. Built with Bun and SolidJS.
            </p>

            <div class="flex items-center gap-4">
              <a
                href="https://github.com/maskdotdev/ray"
                target="_blank"
                rel="noopener noreferrer"
                class="text-slate-500 hover:text-[#00d4ff] transition-colors duration-150"
                aria-label="RayDB on GitHub"
              >
                <svg class="w-5 h-5" fill="currentColor" viewBox="0 0 24 24" aria-hidden="true">
                  <path fill-rule="evenodd" d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z" clip-rule="evenodd" />
                </svg>
              </a>
            </div>
          </div>
        </div>
      </footer>
    </div>
  )
}
