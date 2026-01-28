import { createSignal } from 'solid-js'
import { isServer } from 'solid-js/web'

export type Theme = 'light' | 'dark' | 'system'

const THEME_KEY = 'raydb-theme'

function getInitialTheme(): Theme {
  if (isServer) return 'system'
  
  const stored = localStorage.getItem(THEME_KEY)
  if (stored === 'light' || stored === 'dark' || stored === 'system') {
    return stored
  }
  return 'system'
}

function getSystemTheme(): 'light' | 'dark' {
  if (isServer) return 'dark'
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

const [theme, setThemeInternal] = createSignal<Theme>(getInitialTheme())

export function useTheme() {
  const resolvedTheme = () => {
    const t = theme()
    return t === 'system' ? getSystemTheme() : t
  }

  const setTheme = (newTheme: Theme) => {
    setThemeInternal(newTheme)
    if (!isServer) {
      localStorage.setItem(THEME_KEY, newTheme)
      applyTheme(newTheme === 'system' ? getSystemTheme() : newTheme)
    }
  }

  return { theme, resolvedTheme, setTheme }
}

export function applyTheme(resolved: 'light' | 'dark') {
  if (isServer) return
  
  const root = document.documentElement
  if (resolved === 'dark') {
    root.classList.add('dark')
  } else {
    root.classList.remove('dark')
  }
}

// Initialize theme on client
if (!isServer) {
  const initial = getInitialTheme()
  applyTheme(initial === 'system' ? getSystemTheme() : initial)
  
  // Listen for system theme changes
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
    if (theme() === 'system') {
      applyTheme(e.matches ? 'dark' : 'light')
    }
  })
}
