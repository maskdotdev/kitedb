import type { Component } from 'solid-js'
import { Link } from '@tanstack/solid-router'
import Logo from './Logo'

export const NotFound: Component = () => {
  return (
    <div class="min-h-screen flex items-center justify-center bg-[#030712] p-8">
      <div class="max-w-md text-center">
        <Logo size={64} class="mx-auto mb-8" />
        <h1 class="text-6xl font-black text-gradient mb-4">404</h1>
        <p class="text-xl text-slate-400 mb-8">
          This page doesn't exist yet.
        </p>
        <Link
          to="/"
          class="inline-flex items-center gap-2 px-6 py-3 bg-[#00d4ff] text-black font-semibold rounded-lg hover:bg-[#00d4ff]/90 shadow-[0_0_20px_rgba(0,212,255,0.3)] hover:shadow-[0_0_30px_rgba(0,212,255,0.5)] transition-all"
        >
          Go Home
        </Link>
      </div>
    </div>
  )
}

export default NotFound
