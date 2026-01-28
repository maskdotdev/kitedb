import type { Component, JSX } from 'solid-js'

interface StatCardProps {
  value: string
  label: string
}

export const StatCard: Component<StatCardProps> = (props) => {
  return (
    <article class="group text-center p-8 rounded-2xl bg-[#0a1628]/80 border border-[#1e3a5f] hover:border-[#00d4ff]/30 hover:shadow-[0_0_30px_rgba(0,212,255,0.1)] transition-all duration-300">
      <div class="text-4xl md:text-5xl font-black text-gradient tabular-nums leading-none">
        {props.value}
      </div>
      <div class="mt-3 text-sm font-medium text-slate-400">
        {props.label}
      </div>
    </article>
  )
}

interface StatGridProps {
  children: JSX.Element
}

export const StatGrid: Component<StatGridProps> = (props) => {
  return (
    <div class="grid grid-cols-2 md:grid-cols-4 gap-4 md:gap-6" role="list" aria-label="Performance statistics">
      {props.children}
    </div>
  )
}

export default StatCard
