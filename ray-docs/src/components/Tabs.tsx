import type { Component } from 'solid-js'
import { createSignal, For, Show } from 'solid-js'
import CodeBlock from './CodeBlock'

interface TabItem {
  label: string
  code: string
  language?: string
}

interface TabsProps {
  items: TabItem[]
  defaultIndex?: number
}

export const Tabs: Component<TabsProps> = (props) => {
  const [activeIndex, setActiveIndex] = createSignal(props.defaultIndex ?? 0)

  const handleKeyDown = (e: KeyboardEvent, index: number) => {
    if (e.key === 'ArrowRight') {
      e.preventDefault()
      const nextIndex = (index + 1) % props.items.length
      setActiveIndex(nextIndex)
      document.getElementById(`tab-${nextIndex}`)?.focus()
    } else if (e.key === 'ArrowLeft') {
      e.preventDefault()
      const prevIndex = (index - 1 + props.items.length) % props.items.length
      setActiveIndex(prevIndex)
      document.getElementById(`tab-${prevIndex}`)?.focus()
    } else if (e.key === 'Home') {
      e.preventDefault()
      setActiveIndex(0)
      document.getElementById('tab-0')?.focus()
    } else if (e.key === 'End') {
      e.preventDefault()
      setActiveIndex(props.items.length - 1)
      document.getElementById(`tab-${props.items.length - 1}`)?.focus()
    }
  }

  return (
    <div class="rounded-2xl overflow-hidden border border-[#1e3a5f] bg-[#0a1628] shadow-[0_0_40px_rgba(0,0,0,0.3)]">
      {/* Tab headers */}
      <div 
        class="flex bg-[#0d1829] border-b border-[#1e3a5f]" 
        role="tablist"
        aria-label="Code examples"
      >
        <For each={props.items}>
          {(item, index) => (
            <button
              type="button"
              role="tab"
              id={`tab-${index()}`}
              aria-selected={activeIndex() === index()}
              aria-controls={`tabpanel-${index()}`}
              tabIndex={activeIndex() === index() ? 0 : -1}
              class={`px-6 py-3.5 text-sm font-medium transition-colors duration-150 relative ${
                activeIndex() === index()
                  ? 'text-[#00d4ff] bg-[#0a1628]'
                  : 'text-slate-400 hover:text-white hover:bg-[#1e3a5f]/30'
              }`}
              onClick={() => setActiveIndex(index())}
              onKeyDown={(e) => handleKeyDown(e, index())}
            >
              {item.label}
              <Show when={activeIndex() === index()}>
                <div 
                  class="absolute bottom-0 left-0 right-0 h-0.5 bg-[#00d4ff] shadow-[0_0_10px_rgba(0,212,255,0.5)]" 
                  aria-hidden="true"
                />
              </Show>
            </button>
          )}
        </For>
      </div>

      {/* Tab content */}
      <div>
        <For each={props.items}>
          {(item, index) => (
            <div
              role="tabpanel"
              id={`tabpanel-${index()}`}
              aria-labelledby={`tab-${index()}`}
              class={activeIndex() === index() ? 'block' : 'hidden'}
              tabIndex={0}
            >
              <CodeBlock code={item.code} language={item.language} />
            </div>
          )}
        </For>
      </div>
    </div>
  )
}

export default Tabs
