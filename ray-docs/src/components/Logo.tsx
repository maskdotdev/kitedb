import type { Component } from 'solid-js'

interface LogoProps {
  class?: string
  size?: number
}

export const Logo: Component<LogoProps> = (props) => {
  const size = () => props.size ?? 32
  
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 128 128"
      fill="none"
      class={props.class}
      width={size()}
      height={size()}
      aria-hidden="true"
    >
      <defs>
        <linearGradient id="neonGradient" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" style="stop-color:#00d4ff" />
          <stop offset="50%" style="stop-color:#2aa7ff" />
          <stop offset="100%" style="stop-color:#0d8bf5" />
        </linearGradient>
        <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
          <feGaussianBlur stdDeviation="3" result="coloredBlur"/>
          <feMerge>
            <feMergeNode in="coloredBlur"/>
            <feMergeNode in="SourceGraphic"/>
          </feMerge>
        </filter>
      </defs>
      <circle cx="64" cy="64" r="60" fill="url(#neonGradient)" />
      <g filter="url(#glow)">
        <circle cx="64" cy="40" r="10" fill="white" />
        <circle cx="40" cy="75" r="10" fill="white" />
        <circle cx="88" cy="75" r="10" fill="white" />
        <line x1="64" y1="50" x2="40" y2="65" stroke="white" stroke-width="3" stroke-linecap="round" />
        <line x1="64" y1="50" x2="88" y2="65" stroke="white" stroke-width="3" stroke-linecap="round" />
        <line x1="50" y1="75" x2="78" y2="75" stroke="white" stroke-width="3" stroke-linecap="round" />
      </g>
      <circle cx="64" cy="62" r="6" fill="white" opacity="0.9" />
    </svg>
  )
}

export default Logo
