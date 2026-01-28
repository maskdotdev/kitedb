import type { Component, JSX } from 'solid-js'
import { Show } from 'solid-js'

interface CardProps {
  title: string
  description?: string
  icon?: JSX.Element
  href?: string
  class?: string
  children?: JSX.Element
}

export const Card: Component<CardProps> = (props) => {
  const cardContent = () => (
    <>
      <Show when={props.icon}>
        <div class="flex-shrink-0 w-12 h-12 flex items-center justify-center rounded-xl bg-[#00d4ff]/10 text-[#00d4ff] group-hover:bg-[#00d4ff]/20 group-hover:scale-110 group-hover:shadow-[0_0_20px_rgba(0,212,255,0.3)] transition-all duration-200">
          {props.icon}
        </div>
      </Show>
      <div class="flex-1 min-w-0">
        <h3 class="font-semibold text-white group-hover:text-[#00d4ff] transition-colors duration-150">
          {props.title}
        </h3>
        <Show when={props.description}>
          <p class="mt-2 text-sm text-slate-400 leading-relaxed truncate-2">
            {props.description}
          </p>
        </Show>
        {props.children}
      </div>
    </>
  )

  const cardClass = `group relative flex items-start gap-4 p-6 rounded-2xl bg-[#0a1628] border border-[#1e3a5f] hover:border-[#00d4ff]/40 hover:shadow-[0_0_40px_rgba(0,212,255,0.1)] transition-all duration-300 ${props.class ?? ''}`

  return (
    <Show
      when={props.href}
      fallback={<article class={cardClass}>{cardContent()}</article>}
    >
      <a href={props.href!} class={cardClass}>
        {cardContent()}
        {/* Top glow line on hover */}
        <div 
          class="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-[#00d4ff] to-transparent opacity-0 group-hover:opacity-100 transition-opacity duration-300" 
          aria-hidden="true"
        />
      </a>
    </Show>
  )
}

interface CardGridProps {
  columns?: 1 | 2 | 3 | 4
  children: JSX.Element
}

export const CardGrid: Component<CardGridProps> = (props) => {
  const gridCols = () => {
    switch (props.columns ?? 2) {
      case 1: return 'grid-cols-1'
      case 2: return 'grid-cols-1 md:grid-cols-2'
      case 3: return 'grid-cols-1 md:grid-cols-2 lg:grid-cols-3'
      case 4: return 'grid-cols-1 sm:grid-cols-2 lg:grid-cols-4'
      default: return 'grid-cols-1 md:grid-cols-2'
    }
  }

  return (
    <div class={`grid gap-6 ${gridCols()}`}>
      {props.children}
    </div>
  )
}

export default Card
