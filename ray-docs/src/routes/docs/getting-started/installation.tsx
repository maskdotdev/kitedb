import { createFileRoute } from '@tanstack/solid-router'
import DocPage from '~/components/doc-page'
import CodeBlock from '~/components/code-block'

export const Route = createFileRoute('/docs/getting-started/installation')({
  component: InstallationPage,
})

function InstallationPage() {
  return (
    <DocPage slug="getting-started/installation">
      <p>
        RayDB is available for JavaScript/TypeScript, Rust, and Python.
      </p>

      <h2 id="javascript">JavaScript / TypeScript</h2>
      <CodeBlock
        code={`bun add @ray-db/ray
npm install @ray-db/ray
pnpm add @ray-db/ray
yarn add @ray-db/ray`}
        language="bash"
        inline
      />

      <h2 id="rust">Rust</h2>
      <CodeBlock code="cargo add raydb" language="bash" inline />

      <h2 id="python">Python</h2>
      <CodeBlock
        code={`pip install raydb
uv add raydb`}
        language="bash"
        inline
      />

      <h2 id="requirements">Requirements</h2>
      <ul>
        <li><strong>JavaScript/TypeScript:</strong> Bun 1.0+, Node.js 18+, or Deno</li>
        <li><strong>Rust:</strong> Rust 1.70+</li>
        <li><strong>Python:</strong> Python 3.9+</li>
      </ul>

      <h2 id="verify">Verify Installation</h2>
      <p>Create a simple test file:</p>
      <CodeBlock
        code={`import { ray, defineNode, prop } from '@ray-db/ray';

const user = defineNode('user', {
  key: (id: string) => \`user:\${id}\`,
  props: {
    name: prop.string('name'),
  },
});

const db = await ray('./test.raydb', {
  nodes: [user],
  edges: [],
});

console.log('RayDB is working!');
await db.close();`}
        language="typescript"
        filename="test.ts"
      />

      <p>Run it:</p>
      <CodeBlock
        code={`bun run test.ts
npx tsx test.ts`}
        language="bash"
        inline
      />

      <h2 id="next-steps">Next Steps</h2>
      <p>
        Now that RayDB is installed, head to the <a href="/docs/getting-started/quick-start">Quick Start</a> guide to build your first graph database.
      </p>
    </DocPage>
  )
}
