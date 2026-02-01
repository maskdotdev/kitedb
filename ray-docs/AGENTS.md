# Agent Guidelines for ray-docs

## SolidJS Reactivity Rules

This project uses SolidJS (via TanStack Start). Follow these rules to avoid hydration errors:

### DO NOT use `innerHTML`

```tsx
// BAD - violates Solid reactivity, causes "template2 is not a function" error
<div innerHTML={`<span>Some HTML</span>`} />
<span innerHTML={item.text} />
```

### DO use JSX components

```tsx
// GOOD - proper Solid JSX
<div><span>Some HTML</span></div>
<span>{item.text}</span>

// GOOD - for dynamic content, create small components
function Code(props: { children: JSX.Element }) {
  return <code class="text-cyan-300">{props.children}</code>;
}

// Then use as JSX
<span>Check <Code>delta.keyIndex</Code> for changes</span>
```

### DO NOT destructure props

```tsx
// BAD - breaks reactivity
function Component({ name, value }) {
  return <div>{name}: {value}</div>;
}

// GOOD - access via props object
function Component(props) {
  return <div>{props.name}: {props.value}</div>;
}
```

### DO NOT use `.map()` directly in JSX for reactive lists

```tsx
// BAD - may cause issues with reactivity
{items.map(item => <div>{item.name}</div>)}

// GOOD - use Solid's <For> component
<For each={items}>
  {(item) => <div>{item.name}</div>}
</For>
```

### DO NOT access reactive values outside of tracking scopes

```tsx
// BAD - accessing signal outside JSX/effect
const value = props.items.length;
return <div>{value}</div>;

// GOOD - access within JSX (tracked)
return <div>{props.items.length}</div>;
```

## Component Patterns

### Prefer small, composable components over complex props

Instead of passing HTML strings or complex nested data:

```tsx
// BAD
<DataFlowStep items={[
  { text: "<code>foo</code> bar" },
]} />

// GOOD
<FlowStep>
  <FlowItem><Code>foo</Code> bar</FlowItem>
</FlowStep>
```

### Use `class` not `className`

SolidJS uses `class` for CSS classes:

```tsx
// BAD
<div className="text-red-500" />

// GOOD  
<div class="text-red-500" />
```

## SVG in SolidJS

SVG attributes work differently in Solid. Use class-based styling:

```tsx
// GOOD
<svg class="w-5 h-5 text-cyan-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
  <path stroke-linecap="round" stroke-linejoin="round" d="M19 14l-7 7m0 0l-7-7m7 7V3" />
</svg>
```

For complex SVGs with many elements, if you get type errors on `fill`/`stroke` attributes on `<circle>` or `<line>`, use CSS classes or restructure.
