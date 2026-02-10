import { interpolate, spring, useCurrentFrame, useVideoConfig } from "remotion";

interface KiteLogoProps {
  scale?: number;
  showGlow?: boolean;
  animateIn?: boolean;
  delay?: number;
}

// Node positions
const CENTER = { x: 108, y: 108 };
const NODES = [
  { x: 100, y: 20, color: "#06B6D4" },   // top
  { x: 175, y: 90, color: "#06B6D4" },   // right
  { x: 115, y: 210, color: "#3B82F6" },  // bottom
  { x: 35, y: 105, color: "#06B6D4" },   // left
];

// Edge paths from center to each node
const EDGES = NODES.map((node) => ({
  from: CENTER,
  to: node,
}));

// Outer edges connecting nodes (clockwise)
const OUTER_EDGES = [
  { from: NODES[0], to: NODES[1] }, // top -> right
  { from: NODES[1], to: NODES[2] }, // right -> bottom
  { from: NODES[2], to: NODES[3] }, // bottom -> left
  { from: NODES[3], to: NODES[0] }, // left -> top
];

export const KiteLogo: React.FC<KiteLogoProps> = ({
  scale = 1,
  showGlow = true,
  animateIn = true,
  delay = 0,
}) => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  const localFrame = frame - delay;

  // Phase 1: Center node appears (frames 0-15)
  const centerProgress = animateIn
    ? spring({
        frame: localFrame,
        fps,
        config: { damping: 12, stiffness: 200 },
      })
    : 1;

  // Phase 2: Edges grow outward from center (frames 8-35)
  const edgeProgress = animateIn
    ? interpolate(localFrame, [8, 35], [0, 1], {
        extrapolateLeft: "clamp",
        extrapolateRight: "clamp",
      })
    : 1;

  // Phase 3: Outer nodes pop in sequentially (frames 20-50)
  const nodeDelays = [20, 26, 32, 38];
  const nodeProgresses = NODES.map((_, i) =>
    animateIn
      ? spring({
          frame: localFrame - nodeDelays[i],
          fps,
          config: { damping: 10, stiffness: 300 },
        })
      : 1
  );

  // Phase 4: Outer edges connect (frames 35-60)
  const outerEdgeDelays = [35, 40, 45, 50];
  const outerEdgeProgresses = OUTER_EDGES.map((_, i) =>
    animateIn
      ? interpolate(localFrame, [outerEdgeDelays[i], outerEdgeDelays[i] + 12], [0, 1], {
          extrapolateLeft: "clamp",
          extrapolateRight: "clamp",
        })
      : 1
  );

  // Traveling pulse effect on edges (continuous after initial animation)
  const pulsePhase = localFrame * 0.15;

  // Glow pulse animation
  const glowPulse = interpolate(
    Math.sin(localFrame * 0.08),
    [-1, 1],
    [0.15, 0.4]
  );

  // Center ring rotation
  const ringRotation = localFrame * 2;

  // Overall fade in
  const opacity = animateIn
    ? interpolate(localFrame, [0, 10], [0, 1], {
        extrapolateLeft: "clamp",
        extrapolateRight: "clamp",
      })
    : 1;

  // Calculate edge path with animated length
  const getEdgePath = (from: { x: number; y: number }, to: { x: number; y: number }, progress: number) => {
    const currentX = from.x + (to.x - from.x) * progress;
    const currentY = from.y + (to.y - from.y) * progress;
    return `M${from.x} ${from.y}L${currentX} ${currentY}`;
  };

  // Calculate pulse position along edge
  const getPulsePosition = (from: { x: number; y: number }, to: { x: number; y: number }, t: number) => {
    const wrapped = ((t % 1) + 1) % 1;
    return {
      x: from.x + (to.x - from.x) * wrapped,
      y: from.y + (to.y - from.y) * wrapped,
    };
  };

  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 200 240"
      fill="none"
      width={200 * scale}
      height={240 * scale}
      style={{ opacity }}
    >
      <title>KiteDB Logo</title>
      {/* Neon Background Glow */}
      {showGlow && (
        <circle
          cx={CENTER.x}
          cy={CENTER.y}
          r="70"
          fill="url(#neonGlow)"
          fillOpacity={glowPulse * centerProgress}
        />
      )}

      {/* The Kite Fill - fades in after structure complete */}
      <path
        d="M100 20L175 90L115 210L35 105L100 20Z"
        fill="url(#kiteFill)"
        fillOpacity={interpolate(
          Math.min(...outerEdgeProgresses),
          [0.5, 1],
          [0, 0.15],
          { extrapolateLeft: "clamp", extrapolateRight: "clamp" }
        )}
      />

      {/* Internal Edges - grow from center */}
      <g
        stroke="url(#edgeGradient)"
        strokeWidth="3"
        strokeLinecap="round"
        strokeLinejoin="round"
      >
        {EDGES.map((edge, i) => (
          <path
            key={`inner-${i}`}
            d={getEdgePath(edge.from, edge.to, edgeProgress)}
            style={{
              filter: edgeProgress > 0.5 ? "drop-shadow(0 0 4px #00F0FF)" : "none",
            }}
          />
        ))}
      </g>

      {/* Outer Edges - connect nodes sequentially */}
      <g
        stroke="url(#edgeGradient)"
        strokeWidth="3"
        strokeLinecap="round"
        strokeLinejoin="round"
      >
        {OUTER_EDGES.map((edge, i) => (
          <path
            key={`outer-${i}`}
            d={getEdgePath(edge.from, edge.to, outerEdgeProgresses[i])}
            style={{
              filter: outerEdgeProgresses[i] > 0.5 ? "drop-shadow(0 0 4px #00F0FF)" : "none",
            }}
          />
        ))}
      </g>

      {/* Traveling pulses on edges (only after edges are drawn) */}
      {edgeProgress >= 1 && EDGES.map((edge, i) => {
        const pulsePos = getPulsePosition(edge.from, edge.to, pulsePhase + i * 0.25);
        const pulseOpacity = interpolate(
          Math.sin(pulsePhase * 3 + i),
          [-1, 1],
          [0.3, 0.9]
        );
        return (
          <circle
            key={`pulse-${i}`}
            cx={pulsePos.x}
            cy={pulsePos.y}
            r="3"
            fill="#00F0FF"
            opacity={pulseOpacity}
            style={{ filter: "blur(1px)" }}
          />
        );
      })}

      {/* Outer Nodes - pop in sequentially */}
      {NODES.map((node, i) => {
        const np = nodeProgresses[i];
        const nodeScale = interpolate(np, [0, 1], [0, 1], { extrapolateRight: "clamp" });
        const nodeOpacity = interpolate(np, [0, 0.3], [0, 1], { extrapolateRight: "clamp" });
        
        return (
          <g key={`node-${i}`} style={{ opacity: nodeOpacity }}>
            {/* Node glow on appear */}
            {np > 0 && np < 1 && (
              <circle
                cx={node.x}
                cy={node.y}
                r={12 * np}
                fill={node.color}
                opacity={0.4 * (1 - np)}
              />
            )}
            <circle
              cx={node.x}
              cy={node.y}
              r={5 * nodeScale}
              fill={node.color}
              stroke="white"
              strokeWidth="1.5"
            />
          </g>
        );
      })}

      {/* Center Node - appears first with pulse ring */}
      <g>
        {/* Expanding ring on appear */}
        {centerProgress > 0 && centerProgress < 1 && (
          <circle
            cx={CENTER.x}
            cy={CENTER.y}
            r={20 * centerProgress}
            stroke="#00F0FF"
            strokeWidth="2"
            fill="none"
            opacity={1 - centerProgress}
          />
        )}
        
        {/* Main center node */}
        <circle
          cx={CENTER.x}
          cy={CENTER.y}
          r={7 * centerProgress}
          fill="white"
          style={{
            filter: centerProgress > 0.5 ? "drop-shadow(0 0 6px #00F0FF)" : "none",
          }}
        />
        
        {/* Rotating dashed ring */}
        <circle
          cx={CENTER.x}
          cy={CENTER.y}
          r={14 * centerProgress}
          stroke="#00F0FF"
          strokeWidth="1.5"
          strokeOpacity={0.6 * centerProgress}
          strokeDasharray="4 2"
          fill="none"
          transform={`rotate(${ringRotation} ${CENTER.x} ${CENTER.y})`}
        />
      </g>

      <defs>
        <linearGradient
          id="edgeGradient"
          x1="100"
          y1="20"
          x2="115"
          y2="210"
          gradientUnits="userSpaceOnUse"
        >
          <stop stopColor="#00F0FF" />
          <stop offset="1" stopColor="#2563EB" />
        </linearGradient>
        <linearGradient
          id="kiteFill"
          x1="100"
          y1="20"
          x2="115"
          y2="210"
          gradientUnits="userSpaceOnUse"
        >
          <stop stopColor="#22D3EE" />
          <stop offset="1" stopColor="#1E40AF" />
        </linearGradient>
        <radialGradient id="neonGlow">
          <stop offset="0%" stopColor="#00F0FF" />
          <stop offset="100%" stopColor="transparent" />
        </radialGradient>
      </defs>
    </svg>
  );
};
