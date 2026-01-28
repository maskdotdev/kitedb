import type { Component } from 'solid-js'
import { Show } from 'solid-js'
import { Moon, Sun } from 'lucide-solid'
import { useTheme } from '~/lib/theme'

export const ThemeToggle: Component = () => {
  const { resolvedTheme, setTheme } = useTheme()

  const toggle = () => {
    setTheme(resolvedTheme() === 'dark' ? 'light' : 'dark')
  }

  return (
    <button
      type="button"
      onClick={toggle}
      class="relative p-2.5 rounded-lg text-slate-400 hover:bg-white/5 hover:text-[#00d4ff] transition-colors duration-150"
      aria-label={`Switch to ${resolvedTheme() === 'dark' ? 'light' : 'dark'} mode`}
    >
      <Show when={resolvedTheme() === 'dark'} fallback={<Moon size={20} aria-hidden="true" />}>
        <Sun size={20} aria-hidden="true" />
      </Show>
    </button>
  )
}

export default ThemeToggle
