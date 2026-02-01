import { createFileRoute, useLocation } from "@tanstack/solid-router";
import { type JSX, Show } from "solid-js";
import CodeBlock from "~/components/code-block";
import DocPage from "~/components/doc-page";
import { findDocBySlug } from "~/lib/docs";

// Inline code component for data flow
function Code(props: { children: JSX.Element; color?: string }) {
	const colorClass = () => {
		switch (props.color) {
			case "cyan":
				return "text-cyan-300/80";
			case "violet":
				return "text-violet-300/80";
			case "emerald":
				return "text-emerald-300/80";
			default:
				return "text-cyan-300/80";
		}
	};
	return (
		<code class={`${colorClass()} text-xs px-1 bg-slate-700/50 rounded`}>
			{props.children}
		</code>
	);
}

// Bold label for storage layer items
function Label(props: { children: JSX.Element; color?: string }) {
	const colorClass = () => {
		switch (props.color) {
			case "emerald":
				return "text-emerald-400";
			case "violet":
				return "text-violet-400";
			default:
				return "text-cyan-400";
		}
	};
	return <strong class={colorClass()}>{props.children}</strong>;
}

// Flow item row
function FlowItem(props: {
	isLast?: boolean;
	color: string;
	children: JSX.Element;
}) {
	const connectorColor = () => {
		switch (props.color) {
			case "cyan":
				return "text-cyan-500/60";
			case "violet":
				return "text-violet-500/60";
			case "emerald":
				return "text-emerald-500/60";
			default:
				return "text-slate-500/60";
		}
	};
	return (
		<div class="flex items-start gap-2 text-sm text-slate-300">
			<span class={`mt-0.5 ${connectorColor()}`}>
				{props.isLast ? "└" : "├"}
			</span>
			<span>{props.children}</span>
		</div>
	);
}

// Data flow step card
function FlowStep(props: {
	number: string;
	title: string;
	color: string;
	children: JSX.Element;
}) {
	const borderColor = () => {
		switch (props.color) {
			case "cyan":
				return "border-cyan-500/30";
			case "violet":
				return "border-violet-500/30";
			case "emerald":
				return "border-emerald-500/30";
			default:
				return "border-slate-500/30";
		}
	};
	const badgeColor = () => {
		switch (props.color) {
			case "cyan":
				return "bg-cyan-500/20 text-cyan-400";
			case "violet":
				return "bg-violet-500/20 text-violet-400";
			case "emerald":
				return "bg-emerald-500/20 text-emerald-400";
			default:
				return "bg-slate-500/20 text-slate-400";
		}
	};
	const titleColor = () => {
		switch (props.color) {
			case "cyan":
				return "text-cyan-400";
			case "violet":
				return "text-violet-400";
			case "emerald":
				return "text-emerald-400";
			default:
				return "text-slate-400";
		}
	};
	return (
		<div
			class={`rounded-xl border ${borderColor()} bg-gradient-to-br from-slate-900 to-slate-800 p-4 shadow-lg`}
		>
			<div class="flex items-center gap-3 mb-3">
				<span
					class={`flex items-center justify-center w-7 h-7 rounded-lg text-sm font-bold ${badgeColor()}`}
				>
					{props.number}
				</span>
				<h4 class={`font-semibold ${titleColor()}`}>{props.title}</h4>
			</div>
			<div class="space-y-2 pl-2">{props.children}</div>
		</div>
	);
}

// Arrow connector between steps
function FlowArrow() {
	return (
		<div class="flex justify-center">
			<svg
				class="w-5 h-5 text-slate-500"
				viewBox="0 0 24 24"
				fill="none"
				stroke="currentColor"
				stroke-width="2"
			>
				<path
					stroke-linecap="round"
					stroke-linejoin="round"
					d="M19 14l-7 7m0 0l-7-7m7 7V3"
				/>
			</svg>
		</div>
	);
}

// Insert data flow diagram
function InsertDataFlow() {
	return (
		<div class="my-6 space-y-3">
			<FlowStep number="1" title="Query Layer" color="cyan">
				<FlowItem color="cyan">
					Validates schema <Code color="cyan">user</Code> has required
					properties
				</FlowItem>
				<FlowItem color="cyan">
					Converts <Code color="cyan">age: 30</Code> → internal I64 type
				</FlowItem>
				<FlowItem color="cyan" isLast>
					Calls graph layer: <Code color="cyan">createNode(...)</Code>
				</FlowItem>
			</FlowStep>

			<FlowArrow />

			<FlowStep number="2" title="Graph Layer" color="violet">
				<FlowItem color="violet">
					Begins transaction (if not already in one)
				</FlowItem>
				<FlowItem color="violet">
					Allocates new <Code color="violet">NodeID</Code> (monotonic counter)
				</FlowItem>
				<FlowItem color="violet">Records in transaction state</FlowItem>
				<FlowItem color="violet" isLast>
					On commit → writes to WAL and Delta
				</FlowItem>
			</FlowStep>

			<FlowArrow />

			<FlowStep number="3" title="Storage Layer" color="emerald">
				<FlowItem color="emerald">
					<Label color="emerald">WAL</Label>: Appends CREATE_NODE record
					(durability)
				</FlowItem>
				<FlowItem color="emerald">
					<Label color="emerald">Delta</Label>: Adds node to{" "}
					<Code color="emerald">createdNodes</Code> map
				</FlowItem>
				<FlowItem color="emerald" isLast>
					<Label color="emerald">Later</Label>: Checkpoint merges into snapshot
				</FlowItem>
			</FlowStep>
		</div>
	);
}

// Read data flow diagram
function ReadDataFlow() {
	return (
		<div class="my-6 space-y-3">
			<FlowStep number="1" title="Key Index Lookup" color="cyan">
				<FlowItem color="cyan">
					Check <Code color="cyan">delta.keyIndex</Code> (recent changes)
				</FlowItem>
				<FlowItem color="cyan">
					If not found → check snapshot's hash-bucketed index
				</FlowItem>
				<FlowItem color="cyan" isLast>
					Returns <Code color="cyan">NodeID</Code>
				</FlowItem>
			</FlowStep>

			<FlowArrow />

			<FlowStep number="2" title="Property Fetch" color="violet">
				<FlowItem color="violet">
					Check <Code color="violet">delta.modifiedNodes</Code> for changes
				</FlowItem>
				<FlowItem color="violet">
					Fall back to snapshot for unchanged properties
				</FlowItem>
				<FlowItem color="violet" isLast>
					Merge and return combined result
				</FlowItem>
			</FlowStep>

			{/* Result badge */}
			<div class="flex justify-center pt-2">
				<div class="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-emerald-500/10 border border-emerald-500/30 text-emerald-400 text-sm">
					<svg
						class="w-4 h-4"
						viewBox="0 0 24 24"
						fill="none"
						stroke="currentColor"
						stroke-width="2"
					>
						<path
							stroke-linecap="round"
							stroke-linejoin="round"
							d="M5 13l4 4L19 7"
						/>
					</svg>
					<span>Returns latest committed data</span>
				</div>
			</div>
		</div>
	);
}

// Delta State structure visualization
function DeltaStateStructure() {
	return (
		<div class="my-6 rounded-xl border border-violet-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<div class="flex items-center gap-2 mb-4">
				<svg
					class="w-5 h-5 text-violet-400"
					viewBox="0 0 24 24"
					fill="none"
					stroke="currentColor"
					stroke-width="2"
				>
					<path
						stroke-linecap="round"
						stroke-linejoin="round"
						d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4"
					/>
				</svg>
				<h4 class="font-semibold text-violet-400">Delta State</h4>
			</div>
			<div class="space-y-2">
				<DeltaRow
					connector="├"
					name="createdNodes"
					type="Map<NodeID, NodeData>"
					desc="New nodes"
					color="emerald"
				/>
				<DeltaRow
					connector="├"
					name="deletedNodes"
					type="Set<NodeID>"
					desc="Tombstones"
					color="red"
				/>
				<DeltaRow
					connector="├"
					name="modifiedNodes"
					type="Map<NodeID, PropChanges>"
					desc="Property updates"
					color="amber"
				/>
				<DeltaRow
					connector="├"
					name="outAdd/outDel"
					type="Map<NodeID, EdgePatch[]>"
					desc="Edge changes"
					color="violet"
				/>
				<DeltaRow
					connector="├"
					name="inAdd/inDel"
					type="Map<NodeID, EdgePatch[]>"
					desc="Reverse index"
					color="violet"
				/>
				<DeltaRow
					connector="└"
					name="keyIndex"
					type="Map<string, NodeID>"
					desc="Key lookups"
					color="cyan"
				/>
			</div>
		</div>
	);
}

function DeltaRow(props: {
	connector: string;
	name: string;
	type: string;
	desc: string;
	color: string;
}) {
	const colorClass = () => {
		switch (props.color) {
			case "emerald":
				return "text-emerald-400";
			case "red":
				return "text-red-400";
			case "amber":
				return "text-amber-400";
			case "violet":
				return "text-violet-400";
			default:
				return "text-cyan-400";
		}
	};
	return (
		<div class="flex items-center gap-3 text-sm">
			<span class="text-slate-600">{props.connector}</span>
			<code class={`font-mono ${colorClass()}`}>{props.name}</code>
			<span class="text-slate-500 text-xs hidden sm:inline">{props.type}</span>
			<span class="text-slate-400 ml-auto text-xs">{props.desc}</span>
		</div>
	);
}

// Read decision flow visualization
function ReadFlowDiagram() {
	return (
		<div class="my-6 space-y-3">
			{/* Step 1 */}
			<div class="rounded-xl border border-slate-600/50 bg-slate-800/50 p-4">
				<div class="flex items-start gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-cyan-500/20 text-cyan-400 text-xs font-bold shrink-0">
						1
					</span>
					<div class="flex-1">
						<p class="text-slate-300 text-sm">
							Is{" "}
							<code class="text-red-400 text-xs px-1 bg-slate-700/50 rounded">
								nodeId
							</code>{" "}
							in{" "}
							<code class="text-violet-400 text-xs px-1 bg-slate-700/50 rounded">
								delta.deletedNodes
							</code>
							?
						</p>
						<div class="mt-2 flex items-center gap-2 text-xs">
							<span class="text-emerald-400">→ Yes:</span>
							<span class="text-slate-400">return null (deleted)</span>
						</div>
					</div>
				</div>
			</div>

			{/* Step 2 */}
			<div class="rounded-xl border border-slate-600/50 bg-slate-800/50 p-4">
				<div class="flex items-start gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-cyan-500/20 text-cyan-400 text-xs font-bold shrink-0">
						2
					</span>
					<div class="flex-1">
						<p class="text-slate-300 text-sm">
							Is{" "}
							<code class="text-red-400 text-xs px-1 bg-slate-700/50 rounded">
								nodeId
							</code>{" "}
							in{" "}
							<code class="text-violet-400 text-xs px-1 bg-slate-700/50 rounded">
								delta.createdNodes
							</code>
							?
						</p>
						<div class="mt-2 flex items-center gap-2 text-xs">
							<span class="text-emerald-400">→ Yes:</span>
							<span class="text-slate-400">return delta data (new node)</span>
						</div>
					</div>
				</div>
			</div>

			{/* Step 3 */}
			<div class="rounded-xl border border-slate-600/50 bg-slate-800/50 p-4">
				<div class="flex items-start gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-cyan-500/20 text-cyan-400 text-xs font-bold shrink-0">
						3
					</span>
					<div class="flex-1">
						<p class="text-slate-300 text-sm">
							Does{" "}
							<code class="text-cyan-400 text-xs px-1 bg-slate-700/50 rounded">
								snapshot
							</code>{" "}
							have this node?
						</p>
						<div class="mt-2 flex items-center gap-2 text-xs">
							<span class="text-red-400">→ No:</span>
							<span class="text-slate-400">return null (never existed)</span>
						</div>
					</div>
				</div>
			</div>

			{/* Step 4 - Result */}
			<div class="rounded-xl border border-emerald-500/30 bg-emerald-500/5 p-4">
				<div class="flex items-start gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-emerald-500/20 text-emerald-400 text-xs font-bold shrink-0">
						4
					</span>
					<div class="flex-1">
						<p class="text-slate-300 text-sm">
							Merge{" "}
							<code class="text-cyan-400 text-xs px-1 bg-slate-700/50 rounded">
								snapshot
							</code>{" "}
							+{" "}
							<code class="text-violet-400 text-xs px-1 bg-slate-700/50 rounded">
								delta.modifiedNodes
							</code>
						</p>
						<div class="mt-2 flex items-center gap-2 text-xs">
							<span class="text-emerald-400">→</span>
							<span class="text-emerald-400 font-medium">
								Return combined result
							</span>
						</div>
					</div>
				</div>
			</div>
		</div>
	);
}

// Write flow visualization
function WriteFlowDiagram() {
	return (
		<div class="my-6 rounded-xl border border-slate-600/50 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<h4 class="text-sm font-semibold text-slate-400 mb-4">
				Transaction Commit
			</h4>
			<div class="space-y-3">
				{/* WAL */}
				<div class="flex items-center gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-emerald-500/20 text-emerald-400 text-xs font-bold shrink-0">
						1
					</span>
					<div class="flex items-center gap-2 flex-1">
						<span class="font-semibold text-emerald-400 w-16">WAL</span>
						<span class="text-slate-500">→</span>
						<span class="text-slate-300 text-sm">
							Append records (ensures durability)
						</span>
					</div>
				</div>

				{/* Delta */}
				<div class="flex items-center gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-violet-500/20 text-violet-400 text-xs font-bold shrink-0">
						2
					</span>
					<div class="flex items-center gap-2 flex-1">
						<span class="font-semibold text-violet-400 w-16">Delta</span>
						<span class="text-slate-500">→</span>
						<span class="text-slate-300 text-sm">
							Update in-memory state (visible to reads)
						</span>
					</div>
				</div>

				{/* Cache */}
				<div class="flex items-center gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-amber-500/20 text-amber-400 text-xs font-bold shrink-0">
						3
					</span>
					<div class="flex items-center gap-2 flex-1">
						<span class="font-semibold text-amber-400 w-16">Cache</span>
						<span class="text-slate-500">→</span>
						<span class="text-slate-300 text-sm">
							Invalidate affected entries
						</span>
					</div>
				</div>
			</div>

			{/* Note */}
			<div class="mt-4 pt-4 border-t border-slate-700/50">
				<p class="text-xs text-slate-500 flex items-center gap-2">
					<svg
						class="w-4 h-4 text-cyan-500"
						viewBox="0 0 24 24"
						fill="none"
						stroke="currentColor"
						stroke-width="2"
					>
						<path
							stroke-linecap="round"
							stroke-linejoin="round"
							d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
						/>
					</svg>
					The snapshot is NOT touched during normal writes
				</p>
			</div>
		</div>
	);
}

// Checkpoint step component
function CheckpointStep(props: { num: number; text: string }) {
	const colorClass = () => {
		if (props.num <= 2) return "bg-cyan-500/20 text-cyan-400";
		if (props.num <= 4) return "bg-violet-500/20 text-violet-400";
		return "bg-emerald-500/20 text-emerald-400";
	};
	return (
		<div class="flex items-center gap-3 relative">
			<span
				class={`flex items-center justify-center w-6 h-6 rounded-full text-xs font-bold shrink-0 z-10 ${colorClass()}`}
			>
				{props.num}
			</span>
			<span class="text-slate-300 text-sm">{props.text}</span>
		</div>
	);
}

// Checkpoint process visualization
function CheckpointProcess() {
	return (
		<div class="my-6">
			<div class="rounded-xl border border-cyan-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
				<h4 class="text-sm font-semibold text-slate-400 mb-4">
					Checkpoint Process
				</h4>

				{/* Steps */}
				<div class="relative">
					{/* Vertical line */}
					<div class="absolute left-3 top-3 bottom-3 w-px bg-gradient-to-b from-cyan-500/50 via-violet-500/50 to-emerald-500/50" />

					<div class="space-y-3">
						<CheckpointStep num={1} text="Read current snapshot" />
						<CheckpointStep num={2} text="Apply all delta changes" />
						<CheckpointStep
							num={3}
							text="Write new snapshot (CSR, compressed)"
						/>
						<CheckpointStep
							num={4}
							text="Update header to point to new snapshot"
						/>
						<CheckpointStep num={5} text="Clear delta and WAL" />
					</div>
				</div>

				{/* Timing note */}
				<div class="mt-4 pt-4 border-t border-slate-700/50 flex flex-wrap gap-4 text-xs">
					<div class="flex items-center gap-2">
						<span class="w-2 h-2 rounded-full bg-cyan-400" />
						<span class="text-slate-400">Auto: when WAL reaches threshold</span>
					</div>
					<div class="flex items-center gap-2">
						<span class="w-2 h-2 rounded-full bg-violet-400" />
						<span class="text-slate-400">
							Manual: <code class="text-violet-400">db.checkpoint()</code>
						</span>
					</div>
				</div>
			</div>
		</div>
	);
}

// ============================================================================
// CSR PAGE COMPONENTS
// ============================================================================

// Adjacency Matrix problem visualization
function AdjacencyMatrixProblem() {
	return (
		<div class="my-6 rounded-xl border border-red-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<div class="flex items-center gap-2 mb-4">
				<svg
					class="w-5 h-5 text-red-400"
					viewBox="0 0 24 24"
					fill="none"
					stroke="currentColor"
					stroke-width="2"
				>
					<path
						stroke-linecap="round"
						stroke-linejoin="round"
						d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
					/>
				</svg>
				<h4 class="font-semibold text-red-400">Adjacency Matrix</h4>
			</div>

			{/* Mini matrix visualization */}
			<div class="flex flex-col sm:flex-row gap-4 items-start">
				<div class="font-mono text-xs">
					<div class="text-slate-500 mb-1">{"     A  B  C  D"}</div>
					<div class="text-slate-400">
						A <span class="text-slate-600">[</span> 0{" "}
						<span class="text-cyan-400">1</span>{" "}
						<span class="text-cyan-400">1</span> 0{" "}
						<span class="text-slate-600">]</span>
					</div>
					<div class="text-slate-400">
						B <span class="text-slate-600">[</span> 0 0 0{" "}
						<span class="text-cyan-400">1</span>{" "}
						<span class="text-slate-600">]</span>
					</div>
					<div class="text-slate-400">
						C <span class="text-slate-600">[</span>{" "}
						<span class="text-cyan-400">1</span> 0 0 0{" "}
						<span class="text-slate-600">]</span>
					</div>
					<div class="text-slate-400">
						D <span class="text-slate-600">[</span> 0 0 0 0{" "}
						<span class="text-slate-600">]</span>
					</div>
				</div>
				<div class="flex-1 space-y-2 text-sm">
					<div class="flex items-center gap-2">
						<span class="text-red-400 font-mono">100K × 100K</span>
						<span class="text-slate-500">=</span>
						<span class="text-red-400 font-bold">10 billion</span>
						<span class="text-slate-400">entries</span>
					</div>
					<div class="flex items-center gap-2">
						<span class="text-slate-400">Actual edges:</span>
						<span class="text-cyan-400 font-mono">1M</span>
						<span class="text-slate-500">(0.01% used)</span>
					</div>
					<div class="mt-3 px-3 py-2 rounded-lg bg-red-500/10 border border-red-500/20 text-red-400 text-xs">
						Wastes 99.99% of space
					</div>
				</div>
			</div>
		</div>
	);
}

// Linked List problem visualization
function LinkedListProblem() {
	return (
		<div class="my-6 rounded-xl border border-amber-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<div class="flex items-center gap-2 mb-4">
				<svg
					class="w-5 h-5 text-amber-400"
					viewBox="0 0 24 24"
					fill="none"
					stroke="currentColor"
					stroke-width="2"
				>
					<path
						stroke-linecap="round"
						stroke-linejoin="round"
						d="M13 10V3L4 14h7v7l9-11h-7z"
					/>
				</svg>
				<h4 class="font-semibold text-amber-400">Linked Adjacency Lists</h4>
			</div>

			{/* Linked list visualization */}
			<div class="space-y-2 font-mono text-sm mb-4">
				<div class="flex items-center gap-2">
					<span class="text-cyan-400 w-4">A</span>
					<span class="text-slate-500">→</span>
					<span class="px-2 py-0.5 rounded bg-slate-700 text-slate-300">B</span>
					<span class="text-slate-500">→</span>
					<span class="px-2 py-0.5 rounded bg-slate-700 text-slate-300">C</span>
					<span class="text-slate-500">→</span>
					<span class="text-slate-600">null</span>
				</div>
				<div class="flex items-center gap-2">
					<span class="text-cyan-400 w-4">B</span>
					<span class="text-slate-500">→</span>
					<span class="px-2 py-0.5 rounded bg-slate-700 text-slate-300">D</span>
					<span class="text-slate-500">→</span>
					<span class="text-slate-600">null</span>
				</div>
			</div>

			<div class="space-y-2 text-sm">
				<p class="text-slate-400">
					<span class="text-amber-400 font-semibold">Problem:</span> Pointer
					chasing. Each lookup goes to random memory.
				</p>
				<div class="flex flex-wrap gap-4 text-xs">
					<div class="flex items-center gap-2">
						<span class="text-red-400">Cache miss:</span>
						<span class="text-slate-300 font-mono">~100ns</span>
					</div>
					<div class="flex items-center gap-2">
						<span class="text-emerald-400">Cache hit:</span>
						<span class="text-slate-300 font-mono">~1ns</span>
					</div>
				</div>
				<div class="mt-3 px-3 py-2 rounded-lg bg-amber-500/10 border border-amber-500/20 text-amber-400 text-xs">
					1000 edges × 100ns = 100μs wasted waiting for RAM
				</div>
			</div>
		</div>
	);
}

// CSR Solution visualization
function CSRSolutionDiagram() {
	return (
		<div class="my-6 rounded-xl border border-emerald-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<div class="flex items-center gap-2 mb-4">
				<svg
					class="w-5 h-5 text-emerald-400"
					viewBox="0 0 24 24"
					fill="none"
					stroke="currentColor"
					stroke-width="2"
				>
					<path
						stroke-linecap="round"
						stroke-linejoin="round"
						d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"
					/>
				</svg>
				<h4 class="font-semibold text-emerald-400">CSR Solution</h4>
			</div>

			{/* Graph */}
			<div class="mb-6 p-3 rounded-lg bg-slate-800/50 border border-slate-700">
				<div class="text-xs text-slate-500 mb-2">Graph:</div>
				<div class="flex flex-wrap gap-x-6 gap-y-1 font-mono text-sm">
					<span>
						<span class="text-cyan-400">A</span>{" "}
						<span class="text-slate-500">→</span> B, C
					</span>
					<span>
						<span class="text-cyan-400">B</span>{" "}
						<span class="text-slate-500">→</span> D
					</span>
					<span>
						<span class="text-cyan-400">C</span>{" "}
						<span class="text-slate-500">→</span> A
					</span>
					<span>
						<span class="text-cyan-400">D</span>{" "}
						<span class="text-slate-500">→</span>{" "}
						<span class="text-slate-600">(none)</span>
					</span>
				</div>
			</div>

			{/* Step by step */}
			<div class="space-y-6">
				{/* Step 1 - Destinations */}
				<div class="flex gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-cyan-500/20 text-cyan-400 text-xs font-bold shrink-0 mt-0.5">
						1
					</span>
					<div>
						<div class="text-sm text-slate-400 mb-3">
							Concatenate all destinations:
						</div>
						<div class="flex items-center font-mono text-sm">
							<span class="text-violet-400 mr-3">destinations</span>
							<span class="text-slate-500">=</span>
							{/* Boxes with bracket labels below */}
							<div class="ml-3">
								<div class="flex">
									<div class="w-10 py-1 text-center rounded-l bg-cyan-500/20 text-cyan-400 border-r border-cyan-500/30">
										B
									</div>
									<div class="w-10 py-1 text-center bg-cyan-500/20 text-cyan-400 border-r border-cyan-500/30">
										C
									</div>
									<div class="w-10 py-1 text-center bg-violet-500/20 text-violet-400 border-r border-violet-500/30">
										D
									</div>
									<div class="w-10 py-1 text-center rounded-r bg-emerald-500/20 text-emerald-400">
										A
									</div>
								</div>
								{/* Bracket labels using borders for cleaner look */}
								<div class="flex mt-1">
									{/* A's edges (B, C) - spans 2 boxes */}
									<div class="w-20 flex flex-col items-center">
										<div class="w-full h-2 border-l border-r border-b border-slate-500 rounded-b-sm" />
										<span class="text-xs text-cyan-400 mt-0.5">A</span>
									</div>
									{/* B's edge (D) */}
									<div class="w-10 flex flex-col items-center">
										<div class="w-4 h-2 border-l border-r border-b border-slate-500 rounded-b-sm" />
										<span class="text-xs text-violet-400 mt-0.5">B</span>
									</div>
									{/* C's edge (A) */}
									<div class="w-10 flex flex-col items-center">
										<div class="w-4 h-2 border-l border-r border-b border-slate-500 rounded-b-sm" />
										<span class="text-xs text-emerald-400 mt-0.5">C</span>
									</div>
								</div>
							</div>
						</div>
					</div>
				</div>

				{/* Step 2 - Offsets */}
				<div class="flex gap-3">
					<span class="flex items-center justify-center w-6 h-6 rounded-full bg-cyan-500/20 text-cyan-400 text-xs font-bold shrink-0 mt-0.5">
						2
					</span>
					<div>
						<div class="text-sm text-slate-400 mb-3">
							Record where each node's edges start:
						</div>
						<div class="flex items-center font-mono text-sm">
							<span class="text-violet-400 mr-3">offsets</span>
							<span class="text-slate-500">=</span>
							{/* Boxes with arrows and labels below - each column aligned */}
							<div class="ml-3">
								<div class="flex">
									<div class="w-10 py-1 text-center rounded-l bg-slate-700 text-slate-300 border-r border-slate-600">
										0
									</div>
									<div class="w-10 py-1 text-center bg-slate-700 text-slate-300 border-r border-slate-600">
										2
									</div>
									<div class="w-10 py-1 text-center bg-slate-700 text-slate-300 border-r border-slate-600">
										3
									</div>
									<div class="w-10 py-1 text-center bg-slate-700 text-slate-300 border-r border-slate-600">
										4
									</div>
									<div class="w-10 py-1 text-center rounded-r bg-slate-700 text-slate-300">
										4
									</div>
								</div>
								<div class="flex text-xs text-slate-500 mt-0.5">
									<div class="w-10 text-center">↑</div>
									<div class="w-10 text-center">↑</div>
									<div class="w-10 text-center">↑</div>
									<div class="w-10 text-center">↑</div>
									<div class="w-10 text-center">↑</div>
								</div>
								<div class="flex text-xs text-slate-500">
									<div class="w-10 text-center">A</div>
									<div class="w-10 text-center">B</div>
									<div class="w-10 text-center">C</div>
									<div class="w-10 text-center">D</div>
									<div class="w-10 text-center text-slate-400">end</div>
								</div>
							</div>
						</div>
					</div>
				</div>
			</div>
		</div>
	);
}

// CSR Traversal example
function CSRTraversalExample() {
	return (
		<div class="my-6 space-y-3">
			{/* Example 1 */}
			<div class="rounded-xl border border-cyan-500/30 bg-slate-800/50 p-4">
				<div class="text-sm text-slate-400 mb-2">
					"Who does <span class="text-cyan-400 font-semibold">A</span> connect
					to?"
				</div>
				<div class="space-y-1 font-mono text-sm">
					<div class="text-slate-400">
						start = offsets[<span class="text-cyan-400">0</span>] ={" "}
						<span class="text-emerald-400">0</span>
					</div>
					<div class="text-slate-400">
						end = offsets[<span class="text-cyan-400">1</span>] ={" "}
						<span class="text-emerald-400">2</span>
					</div>
					<div class="text-slate-400">
						destinations[<span class="text-emerald-400">0</span>:
						<span class="text-emerald-400">2</span>] ={" "}
						<span class="text-cyan-400">[B, C]</span>{" "}
						<span class="text-emerald-400">✓</span>
					</div>
				</div>
			</div>

			{/* Example 2 */}
			<div class="rounded-xl border border-slate-600/50 bg-slate-800/50 p-4">
				<div class="text-sm text-slate-400 mb-2">
					"Who does <span class="text-cyan-400 font-semibold">D</span> connect
					to?"
				</div>
				<div class="space-y-1 font-mono text-sm">
					<div class="text-slate-400">
						start = offsets[<span class="text-cyan-400">3</span>] ={" "}
						<span class="text-emerald-400">4</span>
					</div>
					<div class="text-slate-400">
						end = offsets[<span class="text-cyan-400">4</span>] ={" "}
						<span class="text-emerald-400">4</span>
					</div>
					<div class="text-slate-400">
						destinations[<span class="text-emerald-400">4</span>:
						<span class="text-emerald-400">4</span>] ={" "}
						<span class="text-slate-500">[]</span>{" "}
						<span class="text-slate-500">(no edges)</span>{" "}
						<span class="text-emerald-400">✓</span>
					</div>
				</div>
			</div>

			{/* Algorithm */}
			<div class="rounded-xl border border-violet-500/30 bg-slate-800/50 p-4">
				<div class="text-xs text-violet-400 font-semibold mb-2">Algorithm:</div>
				<div class="font-mono text-sm text-slate-300">
					<div>start = offsets[node]</div>
					<div>end = offsets[node + 1]</div>
					<div class="text-emerald-400">return destinations[start:end]</div>
				</div>
			</div>
		</div>
	);
}

// Memory layout comparison
function MemoryLayoutComparison() {
	return (
		<div class="my-6 grid sm:grid-cols-2 gap-4">
			{/* Linked List - Bad */}
			<div class="rounded-xl border border-red-500/30 bg-slate-800/50 p-4">
				<div class="flex items-center gap-2 mb-4">
					<span class="text-red-400 text-xs font-semibold">LINKED LIST</span>
					<span class="text-slate-600">—</span>
					<span class="text-slate-500 text-xs">scattered</span>
				</div>
				{/* Boxes with addresses aligned below each */}
				<div class="flex gap-6 mb-1">
					<div class="w-10 px-3 py-2 rounded bg-slate-700 text-slate-300 font-mono text-sm text-center">
						B
					</div>
					<div class="w-10 px-3 py-2 rounded bg-slate-700 text-slate-300 font-mono text-sm text-center">
						C
					</div>
					<div class="w-10 px-3 py-2 rounded bg-slate-700 text-slate-300 font-mono text-sm text-center">
						D
					</div>
				</div>
				<div class="flex gap-6 mb-3">
					<div class="w-10 text-center text-xs font-mono text-red-400/70">
						0x1000
					</div>
					<div class="w-10 text-center text-xs font-mono text-red-400/70">
						0x5F00
					</div>
					<div class="w-10 text-center text-xs font-mono text-red-400/70">
						0x2A00
					</div>
				</div>
				<div class="text-xs text-red-400">
					↑ Random locations = cache misses
				</div>
			</div>

			{/* CSR - Good */}
			<div class="rounded-xl border border-emerald-500/30 bg-slate-800/50 p-4">
				<div class="flex items-center gap-2 mb-4">
					<span class="text-emerald-400 text-xs font-semibold">CSR</span>
					<span class="text-slate-600">—</span>
					<span class="text-slate-500 text-xs">contiguous</span>
				</div>
				{/* Connected boxes with addresses aligned below each */}
				<div class="flex mb-1">
					<div class="w-10 py-2 rounded-l bg-cyan-500/20 text-cyan-400 font-mono text-sm text-center border-r border-cyan-500/30">
						B
					</div>
					<div class="w-10 py-2 bg-cyan-500/20 text-cyan-400 font-mono text-sm text-center border-r border-cyan-500/30">
						C
					</div>
					<div class="w-10 py-2 bg-cyan-500/20 text-cyan-400 font-mono text-sm text-center border-r border-cyan-500/30">
						D
					</div>
					<div class="w-10 py-2 rounded-r bg-cyan-500/20 text-cyan-400 font-mono text-sm text-center">
						A
					</div>
				</div>
				<div class="flex mb-3">
					<div class="w-10 text-center text-xs font-mono text-emerald-400/70">
						0x1000
					</div>
					<div class="w-10 text-center text-xs font-mono text-emerald-400/70">
						+4
					</div>
					<div class="w-10 text-center text-xs font-mono text-emerald-400/70">
						+8
					</div>
					<div class="w-10 text-center text-xs font-mono text-emerald-400/70">
						+C
					</div>
				</div>
				<div class="text-xs text-emerald-400">
					↑ Sequential = CPU prefetcher works
				</div>
			</div>
		</div>
	);
}

// Bidirectional edges visualization
function BidirectionalEdges() {
	return (
		<div class="my-6 rounded-xl border border-violet-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<div class="grid sm:grid-cols-2 gap-4">
				{/* Out-edges */}
				<div class="p-3 rounded-lg bg-slate-800/50 border border-cyan-500/20">
					<div class="text-xs text-cyan-400 font-semibold mb-2">
						Out-edges (A → B)
					</div>
					<div class="space-y-1 font-mono text-xs">
						<div class="text-slate-400">
							out_offsets = <span class="text-slate-300">[0, 2, 3, 4, 4]</span>
						</div>
						<div class="text-slate-400">
							out_dst = <span class="text-cyan-400">[B, C, D, A]</span>
						</div>
					</div>
					<div class="mt-2 text-xs text-slate-500">
						"Who does Alice follow?"
					</div>
				</div>

				{/* In-edges */}
				<div class="p-3 rounded-lg bg-slate-800/50 border border-violet-500/20">
					<div class="text-xs text-violet-400 font-semibold mb-2">
						In-edges (A ← C)
					</div>
					<div class="space-y-1 font-mono text-xs">
						<div class="text-slate-400">
							in_offsets = <span class="text-slate-300">[0, 1, 2, 3, 4]</span>
						</div>
						<div class="text-slate-400">
							in_src = <span class="text-violet-400">[C, A, A, B]</span>
						</div>
					</div>
					<div class="mt-2 text-xs text-slate-500">"Who follows Alice?"</div>
				</div>
			</div>

			<div class="mt-4 px-3 py-2 rounded-lg bg-slate-700/30 text-xs text-slate-400">
				<span class="text-amber-400">Trade-off:</span> 2× storage, but O(1)
				traversal in both directions
			</div>
		</div>
	);
}

// Edge types visualization
function EdgeTypesSorting() {
	return (
		<div class="my-6 rounded-xl border border-cyan-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-5 shadow-lg">
			<div class="mb-4">
				<div class="font-mono text-sm space-y-1">
					<div class="text-slate-400">
						out_dst = <span class="text-slate-600">[</span>
						<span class="text-cyan-400">B</span>,{" "}
						<span class="text-cyan-400">C</span>,{" "}
						<span class="text-cyan-400">D</span>,{" "}
						<span class="text-cyan-400">A</span>
						<span class="text-slate-600">]</span>
					</div>
					<div class="text-slate-400">
						out_etype = <span class="text-slate-600">[</span>
						<span class="text-emerald-400">0</span>,{" "}
						<span class="text-violet-400">1</span>,{" "}
						<span class="text-emerald-400">0</span>,{" "}
						<span class="text-emerald-400">0</span>
						<span class="text-slate-600">]</span>
					</div>
				</div>
				<div class="flex gap-4 mt-2 text-xs">
					<div class="flex items-center gap-1">
						<span class="w-2 h-2 rounded-full bg-emerald-400" />
						<span class="text-slate-400">0 = KNOWS</span>
					</div>
					<div class="flex items-center gap-1">
						<span class="w-2 h-2 rounded-full bg-violet-400" />
						<span class="text-slate-400">1 = LIKES</span>
					</div>
				</div>
			</div>

			<div class="text-xs text-slate-500 mb-3">
				Sorted by (etype, dst) within each node:
			</div>

			<div class="space-y-2">
				<div class="flex items-center gap-2 text-sm">
					<span class="w-4 h-4 rounded bg-emerald-500/20 text-emerald-400 flex items-center justify-center text-xs">
						1
					</span>
					<span class="text-slate-300">
						Binary search to find specific edge type
					</span>
				</div>
				<div class="flex items-center gap-2 text-sm">
					<span class="w-4 h-4 rounded bg-emerald-500/20 text-emerald-400 flex items-center justify-center text-xs">
						2
					</span>
					<span class="text-slate-300">
						Early termination when past desired type
					</span>
				</div>
				<div class="flex items-center gap-2 text-sm">
					<span class="w-4 h-4 rounded bg-emerald-500/20 text-emerald-400 flex items-center justify-center text-xs">
						3
					</span>
					<span class="text-slate-300">
						"Get A's KNOWS edges" doesn't scan all
					</span>
				</div>
			</div>
		</div>
	);
}

// ============================================================================
// SNAPSHOT + DELTA COMPONENTS
// ============================================================================

// Snapshot + Delta model diagram
function SnapshotDeltaModel() {
	return (
		<div class="my-8">
			{/* Outer container with title */}
			<div class="rounded-2xl border border-slate-600/50 bg-gradient-to-br from-slate-900 to-slate-800 p-6 shadow-xl">
				<h4 class="text-center text-lg font-semibold text-slate-300 mb-6">
					Database State
				</h4>

				{/* Three boxes in a row */}
				<div class="flex flex-col sm:flex-row items-stretch gap-4">
					{/* Snapshot */}
					<div class="flex-1 rounded-xl border border-cyan-500/40 bg-slate-800/50 p-4 relative">
						<div class="absolute -top-px left-4 right-4 h-px bg-gradient-to-r from-transparent via-cyan-400/50 to-transparent" />
						<div class="flex items-center gap-2 mb-3">
							<svg
								class="w-5 h-5 text-cyan-400"
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								stroke-width="2"
							>
								<path
									stroke-linecap="round"
									stroke-linejoin="round"
									d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"
								/>
							</svg>
							<h5 class="font-bold text-cyan-400">Snapshot</h5>
						</div>
						<p class="text-xs text-slate-400 mb-2">(disk)</p>
						<ul class="space-y-1 text-sm text-slate-300">
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-cyan-400/60" />
								Immutable
							</li>
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-cyan-400/60" />
								CSR format
							</li>
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-cyan-400/60" />
								Zero-copy
							</li>
						</ul>
					</div>

					{/* Plus sign */}
					<div class="hidden sm:flex items-center justify-center text-2xl text-slate-500 font-light">
						+
					</div>
					<div class="sm:hidden flex justify-center text-2xl text-slate-500 font-light">
						+
					</div>

					{/* Delta */}
					<div class="flex-1 rounded-xl border border-violet-500/40 bg-slate-800/50 p-4 relative">
						<div class="absolute -top-px left-4 right-4 h-px bg-gradient-to-r from-transparent via-violet-400/50 to-transparent" />
						<div class="flex items-center gap-2 mb-3">
							<svg
								class="w-5 h-5 text-violet-400"
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								stroke-width="2"
							>
								<path
									stroke-linecap="round"
									stroke-linejoin="round"
									d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"
								/>
							</svg>
							<h5 class="font-bold text-violet-400">Delta</h5>
						</div>
						<p class="text-xs text-slate-400 mb-2">(memory)</p>
						<ul class="space-y-1 text-sm text-slate-300">
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-violet-400/60" />
								Pending changes
							</li>
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-violet-400/60" />
								Fast writes
							</li>
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-violet-400/60" />
								Merged on read
							</li>
						</ul>
					</div>

					{/* Arrow */}
					<div class="hidden sm:flex items-center justify-center text-slate-500">
						<svg
							class="w-6 h-6"
							viewBox="0 0 24 24"
							fill="none"
							stroke="currentColor"
							stroke-width="2"
						>
							<path
								stroke-linecap="round"
								stroke-linejoin="round"
								d="M14 5l7 7m0 0l-7 7m7-7H3"
							/>
						</svg>
					</div>
					<div class="sm:hidden flex justify-center text-slate-500">
						<svg
							class="w-6 h-6"
							viewBox="0 0 24 24"
							fill="none"
							stroke="currentColor"
							stroke-width="2"
						>
							<path
								stroke-linecap="round"
								stroke-linejoin="round"
								d="M19 14l-7 7m0 0l-7-7m7 7V3"
							/>
						</svg>
					</div>

					{/* WAL */}
					<div class="flex-1 rounded-xl border border-emerald-500/40 bg-slate-800/50 p-4 relative">
						<div class="absolute -top-px left-4 right-4 h-px bg-gradient-to-r from-transparent via-emerald-400/50 to-transparent" />
						<div class="flex items-center gap-2 mb-3">
							<svg
								class="w-5 h-5 text-emerald-400"
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								stroke-width="2"
							>
								<path
									stroke-linecap="round"
									stroke-linejoin="round"
									d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"
								/>
							</svg>
							<h5 class="font-bold text-emerald-400">WAL</h5>
						</div>
						<p class="text-xs text-slate-400 mb-2">(durability)</p>
						<ul class="space-y-1 text-sm text-slate-300">
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-emerald-400/60" />
								Recovery log
							</li>
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-emerald-400/60" />
								Crash safety
							</li>
							<li class="flex items-center gap-2">
								<span class="w-1 h-1 rounded-full bg-emerald-400/60" />
								Write-ahead
							</li>
						</ul>
					</div>
				</div>
			</div>
		</div>
	);
}

// Architecture diagram component with thematic styling
function ArchitectureDiagram() {
	return (
		<div class="my-8 space-y-3">
			{/* Query Layer */}
			<div class="relative rounded-2xl border border-cyan-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-6 shadow-lg shadow-cyan-500/10">
				<div class="absolute -top-px left-8 right-8 h-px bg-gradient-to-r from-transparent via-cyan-400 to-transparent" />
				<h3 class="text-xl font-bold text-cyan-400 mb-3">Query Layer</h3>
				<div class="flex items-start gap-4">
					<div class="flex-1">
						<div class="flex items-center gap-2 text-slate-300 mb-2">
							<svg
								class="w-5 h-5 text-cyan-400 fill-none stroke-current"
								viewBox="0 0 24 24"
								stroke-width="2"
							>
								<path
									stroke-linecap="round"
									stroke-linejoin="round"
									d="M13 10V3L4 14h7v7l9-11h-7z"
								/>
							</svg>
							<span>Fluent API, type inference, schema validation</span>
						</div>
						<code class="text-sm text-cyan-300/80 font-mono">
							db.insert(user).values({"{...}"})
						</code>
					</div>
					<div class="hidden sm:flex items-center gap-1 text-cyan-400/60">
						<svg
							class="w-5 h-5 fill-none stroke-current"
							viewBox="0 0 24 24"
							stroke-width="2"
						>
							<path
								stroke-linecap="round"
								stroke-linejoin="round"
								d="M19 14l-7 7m0 0l-7-7m7 7V3"
							/>
						</svg>
					</div>
				</div>
			</div>

			{/* Arrow connector */}
			<div class="flex justify-center">
				<svg
					class="w-6 h-6 text-cyan-400/50"
					viewBox="0 0 24 24"
					fill="none"
					stroke="currentColor"
					stroke-width="2"
				>
					<path
						stroke-linecap="round"
						stroke-linejoin="round"
						d="M19 14l-7 7m0 0l-7-7m7 7V3"
					/>
				</svg>
			</div>

			{/* Graph Layer */}
			<div class="relative rounded-2xl border border-cyan-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-6 shadow-lg shadow-cyan-500/10">
				<h3 class="text-xl font-bold text-cyan-400 mb-3">Graph Layer</h3>
				<div class="flex items-start gap-4">
					<div class="flex-1">
						<div class="flex items-center gap-2 text-slate-300 mb-2">
							<svg
								class="w-5 h-5 text-cyan-400 fill-none stroke-current"
								viewBox="0 0 24 24"
								stroke-width="2"
							>
								<path
									stroke-linecap="round"
									stroke-linejoin="round"
									d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1"
								/>
							</svg>
							<span>Nodes, edges, traversal, transactions</span>
						</div>
						<code class="text-sm text-cyan-300/80 font-mono">
							createNode(), addEdge(), getNeighborsOut()
						</code>
					</div>
					{/* Mini graph visualization - using CSS circles */}
					<div class="hidden sm:flex items-center gap-1">
						<div class="flex flex-col items-center gap-1">
							<div class="w-3 h-3 rounded-full bg-cyan-400/80" />
							<div class="w-px h-3 bg-cyan-400/40" />
							<div class="w-3 h-3 rounded-full bg-cyan-400/80" />
						</div>
						<div class="flex flex-col gap-1">
							<div class="w-6 h-px bg-cyan-400/40" />
							<div class="w-6 h-px bg-cyan-400/40" />
						</div>
						<div class="flex flex-col items-center gap-1">
							<div class="w-3 h-3 rounded-full bg-cyan-400/80" />
							<div class="w-px h-3 bg-cyan-400/40" />
							<div class="w-3 h-3 rounded-full bg-cyan-400/80" />
						</div>
					</div>
				</div>
			</div>

			{/* Arrow connector */}
			<div class="flex justify-center">
				<svg
					class="w-6 h-6 text-cyan-400/50"
					viewBox="0 0 24 24"
					fill="none"
					stroke="currentColor"
					stroke-width="2"
				>
					<path
						stroke-linecap="round"
						stroke-linejoin="round"
						d="M19 14l-7 7m0 0l-7-7m7 7V3"
					/>
				</svg>
			</div>

			{/* Storage Layer */}
			<div class="relative rounded-2xl border border-cyan-500/30 bg-gradient-to-br from-slate-900 to-slate-800 p-6 shadow-lg shadow-cyan-500/10">
				<div class="absolute -bottom-px left-8 right-8 h-px bg-gradient-to-r from-transparent via-cyan-400 to-transparent" />
				<h3 class="text-xl font-bold text-cyan-400 mb-3">Storage Layer</h3>
				<div class="flex items-start gap-4">
					<div class="flex-1">
						<div class="flex items-center gap-2 text-slate-300 mb-2">
							<svg
								class="w-5 h-5 text-cyan-400 fill-none stroke-current"
								viewBox="0 0 24 24"
								stroke-width="2"
							>
								<path
									stroke-linecap="round"
									stroke-linejoin="round"
									d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"
								/>
							</svg>
							<span>Snapshot (CSR), Delta, WAL, Key Index</span>
						</div>
						<div class="flex items-center gap-2 text-slate-300">
							<svg
								class="w-5 h-5 text-cyan-400 fill-none stroke-current"
								viewBox="0 0 24 24"
								stroke-width="2"
							>
								<path
									stroke-linecap="round"
									stroke-linejoin="round"
									d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"
								/>
							</svg>
							<span>Memory-mapped files, crash recovery</span>
						</div>
					</div>
					{/* Storage icons */}
					<div class="hidden sm:flex gap-3 text-cyan-400/60">
						<svg
							class="w-8 h-8 fill-none stroke-current"
							viewBox="0 0 24 24"
							stroke-width="1.5"
						>
							<path
								stroke-linecap="round"
								stroke-linejoin="round"
								d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"
							/>
						</svg>
						<svg
							class="w-8 h-8 fill-none stroke-current"
							viewBox="0 0 24 24"
							stroke-width="1.5"
						>
							<path
								stroke-linecap="round"
								stroke-linejoin="round"
								d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
							/>
						</svg>
					</div>
				</div>
			</div>
		</div>
	);
}

export const Route = createFileRoute("/docs/internals/$")({
	component: InternalsSplatPage,
});

function InternalsSplatPage() {
	const location = useLocation();
	const slug = () => {
		const path = location().pathname;
		const match = path.match(/^\/docs\/(.+)$/);
		return match ? match[1] : "";
	};
	const doc = () => findDocBySlug(slug());

	return (
		<Show when={doc()} fallback={<DocNotFound slug={slug()} />}>
			<DocPageContent slug={slug()} />
		</Show>
	);
}

function DocNotFound(props: { slug: string }) {
	return (
		<div class="max-w-4xl mx-auto px-6 py-12">
			<div class="text-center">
				<h1 class="text-4xl font-extrabold text-slate-900 dark:text-white mb-4">
					Page Not Found
				</h1>
				<p class="text-lg text-slate-600 dark:text-slate-400 mb-8">
					The internals page{" "}
					<code class="px-2 py-1 bg-slate-100 dark:bg-slate-800 rounded">
						{props.slug}
					</code>{" "}
					doesn't exist yet.
				</p>
				<a
					href="/docs"
					class="inline-flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-cyan-500 to-violet-500 text-white font-semibold rounded-xl hover:shadow-lg hover:shadow-cyan-500/25 transition-all duration-200"
				>
					Back to Documentation
				</a>
			</div>
		</div>
	);
}

function DocPageContent(props: { slug: string }) {
	const slug = props.slug;

	// ============================================================================
	// ARCHITECTURE
	// ============================================================================
	if (slug === "internals/architecture") {
		return (
			<DocPage slug={slug}>
				<p>
					KiteDB is built as a layered system. Each layer has a specific job,
					and they work together to provide fast, reliable graph storage.
				</p>

				<h2 id="the-layers">The Three Layers</h2>

				<ArchitectureDiagram />

				<h3>Query Layer</h3>
				<p>
					This is what you interact with. It provides the Drizzle-style API with
					full TypeScript type inference. When you write{" "}
					<code>db.insert(user).values(...)</code>, the query layer validates
					your schema, converts TypeScript types to storage types, and calls
					into the graph layer.
				</p>

				<h3>Graph Layer</h3>
				<p>
					Manages the graph abstraction: nodes with properties, edges between
					nodes, and traversals. Handles transaction boundaries and coordinates
					reads between the snapshot and delta.
				</p>

				<h3>Storage Layer</h3>
				<p>
					The foundation. Stores data in a format optimized for graph
					operations. The key insight here is the{" "}
					<strong>Snapshot + Delta</strong> model, which separates immutable
					historical data from pending changes.
				</p>

				<h2 id="data-flow">What Happens When You Insert a Node</h2>

				<p>Let's trace through a simple insert:</p>

				<CodeBlock
					code={`await db.insert(user).values({ key: 'alice', name: 'Alice', age: 30 });`}
					language="typescript"
				/>

				<InsertDataFlow />

				<h2 id="read-path">What Happens When You Read</h2>

				<p>Reads merge data from two sources:</p>

				<CodeBlock
					code={`const alice = await db.get(user, 'alice');`}
					language="typescript"
				/>

				<ReadDataFlow />

				<h2 id="why-this-design">Why This Design</h2>

				<table>
					<thead>
						<tr>
							<th>Design Choice</th>
							<th>Benefit</th>
						</tr>
					</thead>
					<tbody>
						<tr>
							<td>Snapshot + Delta</td>
							<td>
								Reads don't block writes. Snapshot is immutable, delta is small.
							</td>
						</tr>
						<tr>
							<td>CSR format for edges</td>
							<td>Traversals read contiguous memory. CPU cache loves this.</td>
						</tr>
						<tr>
							<td>WAL for durability</td>
							<td>Committed data survives crashes. Recovery is fast.</td>
						</tr>
						<tr>
							<td>Single file</td>
							<td>Portable, atomic operations, simpler deployment.</td>
						</tr>
						<tr>
							<td>Memory-mapped I/O</td>
							<td>OS handles caching. Zero-copy reads.</td>
						</tr>
					</tbody>
				</table>

				<h2 id="next">Next Steps</h2>
				<ul>
					<li>
						<a href="/docs/internals/snapshot-delta">Snapshot + Delta</a> – The
						core storage model in detail
					</li>
					<li>
						<a href="/docs/internals/csr">CSR Format</a> – How edges are stored
					</li>
					<li>
						<a href="/docs/internals/single-file">Single-File Format</a> – The
						.kitedb file layout
					</li>
				</ul>
			</DocPage>
		);
	}

	// ============================================================================
	// SNAPSHOT + DELTA
	// ============================================================================
	if (slug === "internals/snapshot-delta") {
		return (
			<DocPage slug={slug}>
				<p>
					KiteDB separates storage into two parts: a <strong>snapshot</strong>{" "}
					(immutable, on disk) and a <strong>delta</strong> (mutable, in
					memory). This separation is the foundation of how KiteDB achieves fast
					reads and writes.
				</p>

				<h2 id="the-model">The Model</h2>

				<SnapshotDeltaModel />

				<h2 id="snapshot">Snapshot</h2>

				<p>
					The snapshot is a point-in-time image of the entire database. It's
					stored in <a href="/docs/internals/csr">CSR format</a> and
					memory-mapped directly from disk.
				</p>

				<p>
					<strong>Key properties:</strong>
				</p>
				<ul>
					<li>
						<strong>Immutable</strong> – Once written, never modified. Safe for
						concurrent reads.
					</li>
					<li>
						<strong>Zero-copy</strong> – Memory-mapped via <code>mmap()</code>.
						The OS handles caching.
					</li>
					<li>
						<strong>Compressed</strong> – zstd compression reduces disk usage by
						~60%.
					</li>
					<li>
						<strong>Complete</strong> – Contains all nodes, edges, properties,
						and indexes.
					</li>
				</ul>

				<h2 id="delta">Delta</h2>

				<p>
					The delta holds all changes since the last snapshot. It's a collection
					of in-memory data structures optimized for both reads and writes.
				</p>

				<DeltaStateStructure />

				<h2 id="reading">How Reads Work</h2>

				<p>Every read operation merges snapshot and delta:</p>

				<ReadFlowDiagram />

				<p>
					Edge traversals work similarly—scan snapshot edges, skip deleted ones,
					add new ones from delta.
				</p>

				<h2 id="writing">How Writes Work</h2>

				<p>Writes go to three places:</p>

				<WriteFlowDiagram />

				<h2 id="checkpoint">Checkpoint: Merging Delta into Snapshot</h2>

				<p>
					Periodically, KiteDB creates a new snapshot that incorporates all
					delta changes. This is called a <strong>checkpoint</strong>.
				</p>

				<CheckpointProcess />

				<p>
					During checkpoint, reads continue against the old snapshot + delta.
					The switch to the new snapshot is atomic.
				</p>

				<h2 id="why-it-works">Why This Works Well</h2>

				<table>
					<thead>
						<tr>
							<th>Property</th>
							<th>How It's Achieved</th>
						</tr>
					</thead>
					<tbody>
						<tr>
							<td>Fast reads</td>
							<td>Snapshot is mmap'd. OS caches hot pages. Delta is small.</td>
						</tr>
						<tr>
							<td>Fast writes</td>
							<td>WAL append + memory update. No disk seeks.</td>
						</tr>
						<tr>
							<td>Crash safety</td>
							<td>WAL survives crashes. Replay rebuilds delta.</td>
						</tr>
						<tr>
							<td>Concurrent reads</td>
							<td>Snapshot is immutable. MVCC handles delta visibility.</td>
						</tr>
					</tbody>
				</table>

				<h2 id="next">Next Steps</h2>
				<ul>
					<li>
						<a href="/docs/internals/csr">CSR Format</a> – How the snapshot
						stores edges
					</li>
					<li>
						<a href="/docs/internals/wal">WAL & Durability</a> – How the
						write-ahead log works
					</li>
					<li>
						<a href="/docs/internals/mvcc">MVCC & Transactions</a> – How
						concurrent access is handled
					</li>
				</ul>
			</DocPage>
		);
	}

	// ============================================================================
	// CSR FORMAT
	// ============================================================================
	if (slug === "internals/csr") {
		return (
			<DocPage slug={slug}>
				<p>
					KiteDB uses <strong>Compressed Sparse Row (CSR)</strong> format to
					store graph edges. CSR is a standard format for sparse matrices that
					provides fast traversal with minimal memory overhead.
				</p>

				<h2 id="the-problem">The Problem with Naive Edge Storage</h2>

				<p>Consider a graph with 100,000 nodes and 1 million edges.</p>

				<AdjacencyMatrixProblem />

				<LinkedListProblem />

				<h2 id="csr-solution">The CSR Solution</h2>

				<p>
					CSR stores all edges in two flat arrays: <strong>offsets</strong> and{" "}
					<strong>destinations</strong>. No pointers, no wasted space.
				</p>

				<CSRSolutionDiagram />

				<h2 id="traversal">How Traversal Works</h2>

				<p>Finding a node's neighbors is two array lookups:</p>

				<CSRTraversalExample />

				<h2 id="memory-layout">Why It's Fast: Memory Layout</h2>

				<MemoryLayoutComparison />

				<p class="text-sm text-slate-400">
					After the first access, B/C/D/A are already in CPU cache.
				</p>

				<h2 id="bidirectional">Bidirectional Edges</h2>

				<p>
					KiteDB stores edges in <strong>both directions</strong> for fast
					traversal either way:
				</p>

				<BidirectionalEdges />

				<h2 id="edge-types">Edge Types and Sorting</h2>

				<p>
					Real graphs have different edge types (follows, likes, knows). KiteDB
					stores edge types in a parallel array, sorted within each node:
				</p>

				<EdgeTypesSorting />

				<h2 id="existence-check">Edge Existence Check</h2>

				<p>To check if edge A→B exists with type KNOWS:</p>

				<CodeBlock
					code={`function hasEdge(src: NodeID, etype: EdgeType, dst: NodeID): boolean {
  const start = offsets[src];
  const end = offsets[src + 1];
  
  // Binary search for etype within [start, end)
  const typeStart = binarySearchStart(etypes, start, end, etype);
  const typeEnd = binarySearchEnd(etypes, start, end, etype);
  
  // Binary search for dst within type range
  return binarySearch(destinations, typeStart, typeEnd, dst);
}

// Complexity: O(log k) where k = number of edges from src`}
					language="typescript"
				/>

				<h2 id="numbers">Performance Numbers</h2>

				<table>
					<thead>
						<tr>
							<th>Operation</th>
							<th>CSR</th>
							<th>Linked List</th>
						</tr>
					</thead>
					<tbody>
						<tr>
							<td>Start traversal</td>
							<td>O(1) – two array lookups</td>
							<td>O(1) – follow pointer</td>
						</tr>
						<tr>
							<td>Iterate k neighbors</td>
							<td>O(k) – sequential read</td>
							<td>O(k) – but cache misses</td>
						</tr>
						<tr>
							<td>Edge existence</td>
							<td>O(log k) – binary search</td>
							<td>O(k) – linear scan</td>
						</tr>
						<tr>
							<td>Cache behavior</td>
							<td>Excellent – prefetcher works</td>
							<td>Poor – random access</td>
						</tr>
					</tbody>
				</table>

				<h2 id="next">Next Steps</h2>
				<ul>
					<li>
						<a href="/docs/internals/snapshot-delta">Snapshot + Delta</a> – How
						CSR fits into the storage model
					</li>
					<li>
						<a href="/docs/internals/key-index">Key Index</a> – How node lookups
						work
					</li>
					<li>
						<a href="/docs/internals/performance">Performance</a> – Optimization
						techniques
					</li>
				</ul>
			</DocPage>
		);
	}

	// ============================================================================
	// SINGLE-FILE FORMAT
	// ============================================================================
	if (slug === "internals/single-file") {
		return (
			<DocPage slug={slug}>
				<p>
					KiteDB stores everything in a single <code>.kitedb</code> file. This
					makes databases portable, simplifies deployment, and enables atomic
					operations.
				</p>

				<h2 id="file-layout">File Layout</h2>

				<CodeBlock
					code={`┌────────────────────────────────────────┐
│           Header (4 KB)                │  ← Database metadata
├────────────────────────────────────────┤
│                                        │
│         WAL Area (~64 MB)              │  ← Write-ahead log
│   ┌─────────────────────────────┐     │
│   │  Primary Region (75%)       │     │  ← Normal writes
│   ├─────────────────────────────┤     │
│   │  Secondary Region (25%)     │     │  ← Writes during checkpoint
│   └─────────────────────────────┘     │
│                                        │
├────────────────────────────────────────┤
│                                        │
│       Snapshot Area (grows)            │  ← CSR data, compressed
│                                        │
└────────────────────────────────────────┘`}
					language="text"
				/>

				<h2 id="header">The Header</h2>

				<p>
					The header is 4 KB and contains all metadata needed to open the
					database:
				</p>

				<CodeBlock
					code={`Header Contents:
├── Magic bytes         "KITE" + version
├── Page size           4096 (default)
├── Snapshot location   Start page, page count
├── WAL location        Start page, page count
├── WAL pointers        Head and tail positions
├── Counters            Max node ID, next transaction ID
├── Snapshot generation Incremented on each checkpoint
└── Checksums           CRC32C of header data

The header is always at offset 0, always 4 KB.`}
					language="text"
				/>

				<h2 id="atomicity">Atomic Updates</h2>

				<p>
					The header enables atomic state transitions. A checkpoint works like
					this:
				</p>

				<CodeBlock
					code={`1. Write new snapshot to free space at end of file
2. fsync() to ensure snapshot is durable
3. Update header with new snapshot location
4. fsync() header
5. Old snapshot space becomes free for next checkpoint

If crash occurs:
- Before step 4: Old snapshot is still valid
- After step 4: New snapshot is valid

No intermediate state is possible.`}
					language="text"
				/>

				<h2 id="wal-area">WAL Area</h2>

				<p>The WAL area is a circular buffer divided into two regions:</p>

				<CodeBlock
					code={`WAL Area (64 MB example):
┌─────────────────────────────────────────────────────┐
│                Primary (48 MB)                      │
│  Normal transaction writes go here                  │
├─────────────────────────────────────────────────────┤
│           Secondary (16 MB)                         │
│  Used during checkpoint when primary is being read  │
└─────────────────────────────────────────────────────┘

Why two regions?
- Checkpoint reads primary to build new snapshot
- Concurrent transactions write to secondary
- No blocking between reads and writes`}
					language="text"
				/>

				<h2 id="snapshot-area">Snapshot Area</h2>

				<p>The snapshot area holds the CSR-formatted graph data:</p>

				<CodeBlock
					code={`Snapshot Sections:
├── Node ID mappings     Physical ↔ Logical ID translation
├── Out-edge CSR         offsets[], destinations[], edge_types[]
├── In-edge CSR          offsets[], sources[], edge_types[]
├── Properties           Node and edge property values
├── String table         Deduplicated string storage
├── Key index            Hash-bucketed node key lookups
└── Schema               Labels, edge types, property keys

Each section is independently compressed (zstd).
Total compression ratio: typically 40-60% of raw size.`}
					language="text"
				/>

				<h2 id="growth">File Growth</h2>

				<p>The file grows in predictable ways:</p>

				<CodeBlock
					code={`Initial size:
  Header (4 KB) + WAL (64 MB) + Empty snapshot (~4 KB)
  ≈ 64 MB minimum

After data:
  Header + WAL + Snapshot (compressed graph data)
  
Example: 100K nodes, 1M edges
  Raw CSR: ~20 MB
  Compressed: ~8 MB
  Total file: ~72 MB

Growth pattern:
  - WAL area: Fixed size (configurable)
  - Snapshot area: Grows with data
  - Old snapshots: Reclaimed after checkpoint`}
					language="text"
				/>

				<h2 id="vs-directory">Single-File vs Multi-File</h2>

				<p>
					KiteDB previously supported a directory-based format. Single-file is
					now the default:
				</p>

				<table>
					<thead>
						<tr>
							<th>Aspect</th>
							<th>Single-File</th>
							<th>Directory (legacy)</th>
						</tr>
					</thead>
					<tbody>
						<tr>
							<td>Portability</td>
							<td>Copy one file</td>
							<td>Copy entire directory</td>
						</tr>
						<tr>
							<td>Atomic ops</td>
							<td>Header flip</td>
							<td>Manifest + renames</td>
						</tr>
						<tr>
							<td>Disk usage</td>
							<td>~40% smaller</td>
							<td>More overhead</td>
						</tr>
						<tr>
							<td>Complexity</td>
							<td>Simpler</td>
							<td>More moving parts</td>
						</tr>
					</tbody>
				</table>

				<h2 id="opening">Opening a Database</h2>

				<CodeBlock
					code={`function openDatabase(path):
  1. Read header (4 KB at offset 0)
  2. Validate magic bytes and checksums
  3. mmap() snapshot area (zero-copy)
  4. Parse snapshot sections
  5. Replay WAL to rebuild delta
  6. Ready for queries

If WAL replay finds incomplete transaction:
  → Discard it (transaction never committed)

Recovery is automatic and fast.`}
					language="text"
				/>

				<h2 id="next">Next Steps</h2>
				<ul>
					<li>
						<a href="/docs/internals/wal">WAL & Durability</a> – How the
						write-ahead log provides crash safety
					</li>
					<li>
						<a href="/docs/internals/snapshot-delta">Snapshot + Delta</a> – How
						reads merge these two sources
					</li>
				</ul>
			</DocPage>
		);
	}

	// ============================================================================
	// WAL & DURABILITY
	// ============================================================================
	if (slug === "internals/wal") {
		return (
			<DocPage slug={slug}>
				<p>
					The Write-Ahead Log (WAL) ensures that committed transactions survive
					crashes. Before any data is considered committed, it must be written
					to the WAL and flushed to disk.
				</p>

				<h2 id="principle">The WAL Principle</h2>

				<CodeBlock
					code={`Rule: Log before you do.

Transaction Commit:
1. Write all changes to WAL
2. fsync() WAL to disk        ← Data is now durable
3. Update in-memory delta     ← Data is now visible
4. Return success to caller

If crash occurs after step 2:
  → Replay WAL on restart, changes are recovered

If crash occurs before step 2:
  → Changes are lost, but that's OK (transaction didn't commit)`}
					language="text"
				/>

				<h2 id="record-format">WAL Record Format</h2>

				<p>Each operation is stored as a framed record:</p>

				<CodeBlock
					code={`┌──────────────────────────────────────────────┐
│  Length (4 bytes)      Size of this record   │
├──────────────────────────────────────────────┤
│  Type (1 byte)         CREATE_NODE, etc.     │
│  Flags (1 byte)        Reserved              │
│  Reserved (2 bytes)    Padding               │
│  TxID (8 bytes)        Transaction ID        │
│  Payload Length (4)    Size of payload       │
├──────────────────────────────────────────────┤
│  Payload (variable)    Operation-specific    │
├──────────────────────────────────────────────┤
│  CRC32C (4 bytes)      Checksum              │
│  Padding               Align to 8 bytes      │
└──────────────────────────────────────────────┘

Record Types:
- BEGIN          Start of transaction
- COMMIT         Transaction committed
- ROLLBACK       Transaction aborted
- CREATE_NODE    New node with key/properties
- DELETE_NODE    Node removal
- ADD_EDGE       Edge creation
- DELETE_EDGE    Edge removal
- SET_NODE_PROP  Property update
- DEL_NODE_PROP  Property removal`}
					language="text"
				/>

				<h2 id="circular-buffer">Circular Buffer</h2>

				<p>
					The WAL is a fixed-size circular buffer. When it fills up, old
					(already checkpointed) data is overwritten:
				</p>

				<CodeBlock
					code={`WAL Buffer (64 MB):
┌────────────────────────────────────────────────┐
│ ████████████░░░░░░░░░░░░░░░░░░░████████████   │
│ ↑          ↑                   ↑               │
│ old data   TAIL (read)         HEAD (write)    │
│ (reclaimed)                                    │
└────────────────────────────────────────────────┘

HEAD: Where new records are written
TAIL: Start of unprocessed records (for replay)

When HEAD catches up to TAIL:
  → Trigger checkpoint to free space`}
					language="text"
				/>

				<h2 id="dual-region">Dual-Region Design</h2>

				<p>The WAL is split into primary (75%) and secondary (25%) regions:</p>

				<CodeBlock
					code={`Why two regions?

During checkpoint:
1. Primary region is being READ to build new snapshot
2. New transactions need somewhere to WRITE

Solution:
┌─────────────────────────────────────────────────────┐
│           Primary Region (75%)                      │
│  Being read for checkpoint                          │
├─────────────────────────────────────────────────────┤
│      Secondary Region (25%)                         │
│  New writes go here during checkpoint               │
└─────────────────────────────────────────────────────┘

After checkpoint completes:
- Primary is cleared (data is in new snapshot)
- Secondary becomes the new primary
- Writes continue without interruption`}
					language="text"
				/>

				<h2 id="fsync">Durability Guarantees</h2>

				<p>KiteDB provides configurable durability:</p>

				<CodeBlock
					code={`Sync Modes:
┌──────────┬───────────────────┬──────────────────────┐
│ Mode     │ Behavior          │ Trade-off            │
├──────────┼───────────────────┼──────────────────────┤
│ full     │ fsync every       │ Safest, slower       │
│ (default)│ commit            │ writes               │
├──────────┼───────────────────┼──────────────────────┤
│ batch    │ fsync every N     │ Better throughput,   │
│          │ commits or T ms   │ small loss window    │
├──────────┼───────────────────┼──────────────────────┤
│ off      │ No fsync          │ Fastest, data loss   │
│          │ (OS decides)      │ on crash             │
└──────────┴───────────────────┴──────────────────────┘

For most applications, 'full' is the right choice.
Use 'batch' for high write throughput with acceptable risk.`}
					language="text"
				/>

				<h2 id="recovery">Crash Recovery</h2>

				<p>On database open, the WAL is replayed to rebuild the delta:</p>

				<CodeBlock
					code={`Recovery Process:
1. Read header to find WAL boundaries
2. Scan from TAIL to HEAD
3. For each record:
   - Validate CRC32C
   - If valid: apply to delta
   - If invalid: stop (incomplete write)
4. Handle incomplete transactions:
   - If BEGIN without COMMIT: discard
   - If COMMIT found: keep all changes

Recovery time: O(WAL size), typically < 1 second`}
					language="text"
				/>

				<h2 id="checkpoint-trigger">When Checkpoint Happens</h2>

				<CodeBlock
					code={`Automatic checkpoint triggers:
1. WAL reaches 75% capacity
2. Configured time interval (e.g., every 5 minutes)
3. On graceful shutdown

Manual checkpoint:
  await db.checkpoint();

During checkpoint:
- Reads continue (from old snapshot + delta)
- Writes continue (to secondary WAL region)
- No downtime`}
					language="text"
				/>

				<h2 id="next">Next Steps</h2>
				<ul>
					<li>
						<a href="/docs/internals/single-file">Single-File Format</a> – How
						WAL fits in the file layout
					</li>
					<li>
						<a href="/docs/internals/snapshot-delta">Snapshot + Delta</a> – What
						checkpoint produces
					</li>
					<li>
						<a href="/docs/internals/mvcc">MVCC & Transactions</a> – How
						transactions work
					</li>
				</ul>
			</DocPage>
		);
	}

	// ============================================================================
	// MVCC & TRANSACTIONS
	// ============================================================================
	if (slug === "internals/mvcc") {
		return (
			<DocPage slug={slug}>
				<p>
					KiteDB supports concurrent transactions using{" "}
					<strong>Multi-Version Concurrency Control (MVCC)</strong>. Multiple
					readers can access the database simultaneously without blocking each
					other or writers.
				</p>

				<h2 id="isolation">Snapshot Isolation</h2>

				<p>
					Each transaction sees a consistent snapshot of the database as it
					existed when the transaction started. Other transactions' uncommitted
					changes are invisible.
				</p>

				<CodeBlock
					code={`Timeline:
─────────────────────────────────────────────────────────►
   │                    │                    │
   T1 starts            T2 starts            T1 commits
   sees v1              sees v1              (writes v2)
                        │
                        T2 still sees v1
                        (T1's changes invisible until T2 restarts)

T1: await db.transaction(async () => {
      await db.update(user).set({ age: 31 }).where({ key: 'alice' });
    });  // commits v2

T2: await db.transaction(async () => {
      const alice = await db.get(user, 'alice');
      // alice.age is still 30 (v1), not 31
    });`}
					language="text"
				/>

				<h2 id="version-chains">Version Chains</h2>

				<p>
					When data is modified while readers exist, KiteDB keeps old versions
					in a chain:
				</p>

				<CodeBlock
					code={`Version Chain for Node "alice":

┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ v3: age=32      │ ←─ │ v2: age=31      │ ←─ │ v1: age=30      │
│ commitTs=150    │    │ commitTs=120    │    │ commitTs=80     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        ↑                      ↑                      ↑
   T3 sees this          T2 sees this          T1 sees this
   (startTs=145)         (startTs=115)         (startTs=75)

Each transaction follows the chain to find the version
that was committed before it started.`}
					language="text"
				/>

				<h2 id="visibility">Visibility Rules</h2>

				<CodeBlock
					code={`A version is visible to transaction T if:

1. version.commitTs <= T.startTs
   (Version was committed before T started)

   OR

2. version.txid == T.txid
   (T created this version itself — read-your-own-writes)

Walk the chain from newest to oldest.
Return first visible version.
If none visible, entity doesn't exist for this transaction.`}
					language="text"
				/>

				<h2 id="conflict-detection">Write Conflicts</h2>

				<p>
					KiteDB uses <strong>First-Committer-Wins</strong> to handle conflicts:
				</p>

				<CodeBlock
					code={`Scenario:
  T1 starts at ts=100
  T2 starts at ts=105
  Both modify node "alice"

T1 commits first (at ts=110):
  ✓ Succeeds — no conflict

T2 tries to commit (at ts=115):
  Check: Was "alice" modified after T2.startTs (105)?
  Yes, T1 modified it at ts=110
  ✗ T2 is rolled back with ConflictError

Resolution: T2 must retry with fresh read`}
					language="text"
				/>

				<CodeBlock
					code={`// Handling conflicts
try {
  await db.transaction(async () => {
    const alice = await db.get(user, 'alice');
    await db.update(user)
      .set({ age: alice.age + 1 })
      .where({ key: 'alice' });
  });
} catch (e) {
  if (e instanceof ConflictError) {
    // Another transaction modified alice
    // Retry with fresh data
  }
}`}
					language="typescript"
				/>

				<h2 id="lazy-versioning">Lazy Version Chains</h2>

				<p>
					Version chains are only created when necessary. If there are no
					concurrent readers, modifications happen in-place without versioning
					overhead.
				</p>

				<CodeBlock
					code={`Optimization: Lazy MVCC

When T1 modifies "alice":
  IF no other transactions are active:
    → Modify in-place (no version chain)
  ELSE:
    → Create version chain (preserve old value for readers)

Result: Serial workloads have zero MVCC overhead.
        Concurrent workloads get correct isolation.`}
					language="text"
				/>

				<h2 id="garbage-collection">Garbage Collection</h2>

				<p>Old versions are cleaned up when no transaction can see them:</p>

				<CodeBlock
					code={`GC Process:
1. Track oldest active transaction (minStartTs)
2. For each version chain:
   - Keep versions where commitTs >= minStartTs
   - Delete older versions (no one can see them)

Triggered:
- After transaction commits
- Periodically in background

Memory impact: Only active transactions hold memory.
               Long-running transactions delay GC.`}
					language="text"
				/>

				<h2 id="transaction-api">Transaction API</h2>

				<CodeBlock
					code={`// Explicit transaction
await db.transaction(async (ctx) => {
  const alice = await ctx.get(user, 'alice');
  await ctx.update(user).set({ age: alice.age + 1 }).where({ key: 'alice' });
  // Commits on successful return
  // Rolls back on exception
});

// Batch operations (single transaction)
await db.batch([
  db.insert(user).values({ key: 'bob', name: 'Bob' }),
  db.link(user, follows, user).from({ key: 'alice' }).to({ key: 'bob' }),
]);

// Without explicit transaction: each operation is auto-committed`}
					language="typescript"
				/>

				<h2 id="next">Next Steps</h2>
				<ul>
					<li>
						<a href="/docs/internals/wal">WAL & Durability</a> – How commits are
						made durable
					</li>
					<li>
						<a href="/docs/guides/transactions">Transactions Guide</a> –
						Practical usage patterns
					</li>
					<li>
						<a href="/docs/guides/concurrency">Concurrency Guide</a> –
						Multi-threaded access
					</li>
				</ul>
			</DocPage>
		);
	}

	// ============================================================================
	// KEY INDEX
	// ============================================================================
	if (slug === "internals/key-index") {
		return (
			<DocPage slug={slug}>
				<p>
					Every node in KiteDB can have a string key for lookup. The key index
					provides O(1) average-case lookups from key to NodeID.
				</p>

				<h2 id="the-problem">The Problem</h2>

				<CodeBlock
					code={`Without an index:
  "Find user:alice" → Scan all nodes → O(n)
  With 1M nodes at 1μs/check = 1 second

With hash index:
  "Find user:alice" → Hash lookup → O(1)
  With 1M nodes = ~100ns`}
					language="text"
				/>

				<h2 id="structure">Index Structure</h2>

				<p>The key index uses hash buckets with linear probing:</p>

				<CodeBlock
					code={`Key Index:
┌─────────────────────────────────────────────────────┐
│ Bucket Array (n buckets)                            │
│ ┌────┬────┬────┬────┬────┬────┐                    │
│ │ 0  │ 1  │ 2  │ 3  │ 4  │... │  ← Start offsets   │
│ └────┴────┴────┴────┴────┴────┘                    │
├─────────────────────────────────────────────────────┤
│ Entry Array (sorted by bucket, then hash)           │
│ ┌──────────────────────────────────────────────┐   │
│ │ hash64 │ stringId │ nodeId │                 │   │
│ │ hash64 │ stringId │ nodeId │  ← bucket 0     │   │
│ │ hash64 │ stringId │ nodeId │                 │   │
│ ├──────────────────────────────────────────────┤   │
│ │ hash64 │ stringId │ nodeId │  ← bucket 1     │   │
│ │ ...                                          │   │
│ └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘

hash64: xxHash64 of the key string
stringId: Index into string table (for collision resolution)
nodeId: The NodeID this key maps to`}
					language="text"
				/>

				<h2 id="lookup">Lookup Process</h2>

				<CodeBlock
					code={`function getNodeByKey(key):
  # Step 1: Check delta first (recent changes)
  if key in delta.keyIndexDeleted:
    return null  # Key was deleted
  if key in delta.keyIndex:
    return delta.keyIndex[key]  # Recently created/modified
  
  # Step 2: Search snapshot index
  hash = xxHash64(key)
  bucket = hash % numBuckets
  
  start = bucketArray[bucket]
  end = bucketArray[bucket + 1]
  
  # Binary search within bucket by hash
  for entry in entries[start:end]:
    if entry.hash64 == hash:
      # Potential match — verify string
      if stringTable[entry.stringId] == key:
        return entry.nodeId
  
  return null  # Key not found`}
					language="text"
				/>

				<h2 id="two-level">Two-Level Lookup</h2>

				<p>
					The key index is split between delta (memory) and snapshot (disk):
				</p>

				<CodeBlock
					code={`Delta (in-memory):
├── keyIndex: Map<string, NodeID>       Recently created keys
└── keyIndexDeleted: Set<string>        Recently deleted keys

Snapshot (on disk):
├── bucketArray: u32[]                  Bucket start positions
└── entries: { hash64, stringId, nodeId }[]

Lookup order:
1. delta.keyIndexDeleted  → If found, return null
2. delta.keyIndex         → If found, return NodeID
3. snapshot index         → Search hash buckets

This order ensures recent changes override old data.`}
					language="text"
				/>

				<h2 id="hashing">Why xxHash64</h2>

				<CodeBlock
					code={`Hash function requirements:
✓ Fast          — Called on every key lookup
✓ Good distribution — Minimize bucket collisions
✓ Deterministic — Same key always same hash

xxHash64:
- Speed: ~10 GB/s on modern CPUs
- Distribution: Excellent avalanche properties
- 64-bit output: Collision probability ≈ 1/2^64

For typical key lengths (10-100 bytes):
  xxHash64: ~50-100ns per hash
  SHA-256:  ~500-1000ns per hash
  
10x faster, and we don't need cryptographic security.`}
					language="text"
				/>

				<h2 id="collisions">Handling Collisions</h2>

				<CodeBlock
					code={`Hash collisions are rare but handled correctly:

Scenario: Two keys hash to same bucket
  "user:alice"  → hash: 0x1234...
  "user:alfred" → hash: 0x1234... (collision)

Resolution:
1. Both entries stored in same bucket
2. On lookup, hash matches both
3. stringId comparison breaks tie
4. Actual string comparison confirms match

Cost of collision: O(k) string comparisons where k = entries with same hash
With 64-bit hash and good distribution: k ≈ 1`}
					language="text"
				/>

				<h2 id="load-factor">Load Factor</h2>

				<CodeBlock
					code={`Load factor = entries / buckets

KiteDB uses ~50% load factor (2x buckets as entries):
- Low collision rate
- Reasonable memory usage
- Fast lookups

With 1M keys:
  Buckets: 2M × 4 bytes = 8 MB
  Entries: 1M × 24 bytes = 24 MB
  Total index: ~32 MB

Lookup: 1 bucket read + 1-2 entry reads = ~100ns`}
					language="text"
				/>

				<h2 id="next">Next Steps</h2>
				<ul>
					<li>
						<a href="/docs/internals/snapshot-delta">Snapshot + Delta</a> – How
						the two-level lookup fits in
					</li>
					<li>
						<a href="/docs/internals/performance">Performance</a> – Index
						optimization techniques
					</li>
				</ul>
			</DocPage>
		);
	}

	// ============================================================================
	// PERFORMANCE
	// ============================================================================
	if (slug === "internals/performance") {
		return (
			<DocPage slug={slug}>
				<p>
					KiteDB is designed for speed. This page explains why it's fast and how
					to get the best performance from it.
				</p>

				<h2 id="why-fast">Why KiteDB is Fast</h2>

				<h3>1. No Network Overhead</h3>
				<CodeBlock
					code={`Traditional database query:
  App → Network → DB Server → Disk → DB Server → Network → App
  Latency: 1-10ms per operation (network round-trip)

KiteDB (embedded):
  App → Memory/Disk → App
  Latency: 1-100μs per operation

Speedup: 10-1000x just from eliminating network`}
					language="text"
				/>

				<h3>2. Zero-Copy Memory Mapping</h3>
				<CodeBlock
					code={`Traditional read:
  Disk → Kernel buffer → User buffer → Parse → Use
  Copies: 2+ memory copies, allocation overhead

KiteDB read (mmap):
  Disk → Page cache → Direct access
  Copies: 0 (data stays in kernel page cache)

The OS handles caching. Hot data stays in RAM automatically.
Cold data is paged in on demand.`}
					language="text"
				/>

				<h3>3. Cache-Friendly Data Layout</h3>
				<CodeBlock
					code={`CSR format keeps edges contiguous:

Traversing 10 neighbors:
  Linked list: 10 random memory accesses × 100ns = 1000ns
  CSR: 1 sequential read × 10ns + 10 cache hits × 1ns = 20ns

Speedup: 50x for traversal operations`}
					language="text"
				/>

				<h3>4. Lazy MVCC</h3>
				<CodeBlock
					code={`Version chains only created when needed:

Serial workload (no concurrent readers):
  Modify → Update in-place
  Overhead: 0

Concurrent workload:
  Modify → Create version chain
  Overhead: Proportional to concurrency

Most workloads are mostly serial.
MVCC overhead is paid only when required.`}
					language="text"
				/>

				<h2 id="benchmarks">Benchmark Results</h2>

				<p>
					Measured against Memgraph (a fast graph database) at 100K nodes / 1M
					edges:
				</p>

				<table>
					<thead>
						<tr>
							<th>Operation</th>
							<th>KiteDB</th>
							<th>Memgraph</th>
							<th>Speedup</th>
						</tr>
					</thead>
					<tbody>
						<tr>
							<td>Key lookup</td>
							<td>160ns</td>
							<td>100μs</td>
							<td>624x</td>
						</tr>
						<tr>
							<td>1-hop traversal</td>
							<td>1.9μs</td>
							<td>100μs</td>
							<td>52x</td>
						</tr>
						<tr>
							<td>Edge existence</td>
							<td>610ns</td>
							<td>100μs</td>
							<td>164x</td>
						</tr>
						<tr>
							<td>2-hop traversal</td>
							<td>397μs</td>
							<td>100ms</td>
							<td>252x</td>
						</tr>
						<tr>
							<td>Batch insert</td>
							<td>6.7ms</td>
							<td>10ms</td>
							<td>1.5x</td>
						</tr>
					</tbody>
				</table>

				<p>
					The large speedups come from eliminating network overhead and using
					cache-friendly data structures.
				</p>

				<h2 id="best-practices">Best Practices</h2>

				<h3>Batch Writes</h3>
				<CodeBlock
					code={`// Slow: Individual inserts (1 WAL sync per operation)
for (const user of users) {
  await db.insert(userSchema).values(user);
}
// 1000 users × 1ms sync = 1000ms

// Fast: Batch insert (1 WAL sync for all)
await db.insert(userSchema).values(users);
// 1000 users × 1μs + 1ms sync = ~2ms

Speedup: 500x for bulk operations`}
					language="typescript"
				/>

				<h3>Limit Traversal Depth</h3>
				<CodeBlock
					code={`// Dangerous: Unbounded traversal
const all = await db.from(user)
  .traverse().out(follows)
  .nodes().toArray();
// Could return millions of nodes

// Safe: Bounded traversal
const friends = await db.from(user)
  .traverse().out(follows)
  .depth({ max: 2 })
  .limit(100)
  .nodes().toArray();
// Returns at most 100 nodes, max 2 hops`}
					language="typescript"
				/>

				<h3>Use Keys for Lookups</h3>
				<CodeBlock
					code={`// Fast: Key lookup (O(1) hash index)
const alice = await db.get(user, 'alice');

// Slower: Property scan (O(n) nodes)
const alice = await db.from(user)
  .where({ name: 'Alice' })
  .first();

Design keys to match your access patterns.`}
					language="typescript"
				/>

				<h3>Checkpoint Timing</h3>
				<CodeBlock
					code={`// Default: Automatic checkpoint when WAL fills
// Good for most workloads

// For write-heavy bursts: Manual checkpoint after
await importLargeDataset();
await db.checkpoint();  // Consolidate before queries

// For read-heavy: Larger WAL, less frequent checkpoints
const db = await kite('./mydb', {
  walSize: 256 * 1024 * 1024,  // 256MB WAL
});`}
					language="typescript"
				/>

				<h2 id="memory">Memory Usage</h2>

				<CodeBlock
					code={`Memory breakdown:

1. Snapshot (mmap'd)
   - Not counted against process memory
   - OS manages page cache
   - Hot pages stay in RAM, cold pages on disk

2. Delta
   - Created nodes: ~200 bytes per node
   - Modified nodes: ~100 bytes per change
   - Edges: ~20 bytes per edge change
   
3. Caches (configurable)
   - Property cache: LRU, default 10K entries
   - Traversal cache: LRU, invalidated on writes

4. MVCC version chains
   - Only when concurrent transactions exist
   - Cleaned up by GC

Typical 100K node graph:
  Snapshot on disk: ~10MB (compressed)
  Memory footprint: ~5MB (hot pages + delta + caches)`}
					language="text"
				/>

				<h2 id="profiling">Profiling Tips</h2>

				<CodeBlock
					code={`// Get database statistics
const stats = await db.stats();
console.log(stats);
// {
//   nodes: 100000,
//   edges: 500000,
//   snapshotSize: 10485760,
//   deltaSize: 524288,
//   walUsage: 0.45
// }

// If walUsage is consistently high:
// → Checkpoint more frequently or increase WAL size

// If deltaSize is large:
// → Checkpoint to consolidate into snapshot`}
					language="typescript"
				/>

				<h2 id="next">Next Steps</h2>
				<ul>
					<li>
						<a href="/docs/internals/csr">CSR Format</a> – Why traversals are
						fast
					</li>
					<li>
						<a href="/docs/internals/snapshot-delta">Snapshot + Delta</a> – How
						reads stay fast during writes
					</li>
					<li>
						<a href="/docs/benchmarks">Benchmarks</a> – Detailed performance
						measurements
					</li>
				</ul>
			</DocPage>
		);
	}

	// Default fallback
	return (
		<DocPage slug={slug}>
			<p>This internals documentation is coming soon.</p>
		</DocPage>
	);
}
