# RFC: Sidecar Indexing for Code Intelligence (Graph + Lexical + Vectors)

Status: draft  
Owner: Mask  
Date: 2026-02-05  

## Summary

Single-file KiteDB delivers strong baseline throughput, but parallel write scaling saturates quickly
due to commit/WAL ordering and fsync constraints (see `commit_lock` and `docs/BENCHMARKS.md`).

This RFC proposes a **single logical embedded database** with **multiple files**:

- **Graph store (authoritative, durable)**: nodes/edges + chunk metadata/text.
- **Lexical sidecar (derived, rebuildable)**: BM25 + trigram; **immediate freshness** via in-memory overlay.
- **Vector sidecar (derived, rebuildable)**: async remote embeddings + ANN/bruteforce; **eventual** persistence + compaction.

Goal: “save file → searchable in <50ms” via lexical/structural retrieval; vector results refine asynchronously
(target p95 < 2s) without blocking the hot path.

## Motivation

Workload: code intelligence indexing.

- Initial index: hundreds/thousands of chunks.
- Incremental updates: tens of chunks on edit.
- UX requirement: incremental changes searchable in <50ms (lexical/structural OK until vectors land).
- Engineering constraint: embedded/local; today’s single-file write path limits concurrent ingest of
  nodes+edges+vectors.

## Goals

- <50ms “freshness” for **lexical + structural** search after an edit batch is applied.
- Async semantic refinement via vectors (remote embeddings acceptable) with p95 “vector freshness” < 2s.
- Incremental updates: chunk add/update/delete; no in-place posting list deletions.
- Crash-safe, bounded recovery: graph always consistent; derived indexes can lag/rebuild.
- Bindings-friendly API (TS/JS + Python): simple modes; predictable semantics.

## Non-goals

- Cross-process, multi-writer correctness for the same DB directory (first version can assume “single process writer”).
- Full atomic “graph+lex+vector” durability in one commit (explicitly avoided for throughput).
- Language-specific parsers in core (tokenization is language-agnostic; bindings can enrich metadata).

## Terminology

- **Chunk**: smallest indexed unit (span of code/text) with stable identity.
- **chunk_key**: stable chunk identifier (e.g. `NodeId`, or `(path, span)` hashed).
- **doc_id**: monotonically increasing lexical document version (one chunk can map to many `doc_id` over time).
- **commit_ts**: graph commit timestamp/sequence (already exists in MVCC path; otherwise monotonic tx counter).
- **Freshness**:
  - Lex freshness: lexical queries reflect latest applied edit batch.
  - Vector freshness: semantic rerank reflects latest embeddings for affected chunks.

## Current State (Problem)

- Single-file commit serializes ordering and WAL flush (see `commit_lock` in `ray-rs/src/core/single_file/mod.rs`).
- Parallel writers do not scale linearly; vector writes especially contend and can hit `WalBufferFull`.
- Remote embeddings cannot reliably complete within 50ms; therefore “semantic freshness” cannot be guaranteed under 50ms.

## Proposal

### 1) File Layout (single logical DB)

DB “root” path: `X.kitedb/` directory (still embedded; one folder).

- `X.kitedb/graph.kite` : existing single-file core (authoritative).
- `X.kitedb/lex.kite`   : lexical sidecar (segments + tombstones + compaction).
- `X.kitedb/vec.kite`   : vector sidecar (append log + segments + compaction).

Rationale: multiple files allow independent locks, WAL buffers, and flush policies. Still “local embedded”.

### 2) Consistency Model

Hard guarantee:
- After `apply_changes()` returns, **lexical + structural** search must include the changes (same process).

Soft guarantee:
- Vector results may lag; they refine when embeddings arrive and are applied.

API exposes explicit modes:
- `SearchMode::Now` (default): best available now; lexical always fresh; vectors best-effort.
- `SearchMode::Durable`: only sidecar state known persisted (debug/verification).
- `await_indexed(commit_ts, max_wait_ms)` to wait for vector freshness up to a bound.

### 3) Write Pipeline

#### Hot path (<50ms): graph + lexical overlay

Input: file change batch → chunk diff (add/update/delete).

1. Graph transaction:
   - Upsert chunk nodes (metadata: `path`, `lang`, `span`, `hash`, `kind`, etc).
   - Update edges (imports/refs/contains/calls/etc).
   - Persist chunk text (authoritative) OR store content hash + optional snippet (policy choice).
2. Lexical update (synchronous in-process):
   - Tokenize changed chunks.
   - Update **in-memory overlay** (see below).
   - Enqueue sidecar persistence (async) OR write lex sidecar append without fsync.
3. Vector enqueue (async):
   - For each changed chunk: enqueue embedding job keyed by `(chunk_key, content_hash)`.
   - Mark chunk state `semantic_pending` in graph (optional; useful for UI/status).

Return: `commit_ts` for the graph batch.

#### Async path (<2s target): embeddings + vectors

1. Embedding workers (remote):
   - Debounce/coalesce per file (e.g. 50–150ms) to avoid embedding intermediate edits.
   - Batch chunks per request; bounded concurrency (2–8).
   - Cache by content hash (avoid repeat cost).
2. Vector apply:
   - Update in-memory vector delta overlay (instant availability once embedding ready).
   - Append to vector sidecar log/segments.
   - Background merge/compaction builds/maintains ANN structures.

### 4) Lexical Index Design

#### Tokenization (language-agnostic)

- Identifier tokens: `[A-Za-z0-9_]+` + Unicode letters where practical.
- Split `snake_case`, `camelCase`, digits:
  - `HTTPServer2` → `httpserver2`, `http`, `server`, `2` (configurable).
- Optional path tokens: basename, directory segments.
- Store per-chunk metadata for filtering/boosting: `lang`, `path`, `repo`, `kind`, `recency`, etc.

#### Storage: segment + tombstones

- New writes go to small **segments**:
  - Term dictionary (FST or hash) → postings list (doc_id, tf).
  - Document table: `doc_id -> {chunk_key, len, meta}`.
  - Trigram table: `tri -> [doc_id...]` (for fuzzy).
- Updates:
  - `chunk_key -> latest_doc_id` mapping.
  - New version: insert postings/doc row for new `doc_id`.
  - Tombstone old `doc_id` (bitset/roaring).
- Deletes:
  - Tombstone current `doc_id`; remove `chunk_key` mapping.
- Compaction:
  - Merge N segments → 1 larger segment; drop tombstoned docs; rebuild dictionaries.
  - Trigger: segment count, tombstone ratio, bytes, or idle time.

#### In-memory overlay (freshness)

To guarantee <50ms freshness while keeping sidecar persistence async:
- Maintain a tiny RAM “delta segment” for recent updates (bounded by size/time).
- Queries search: `base segments (disk)` + `delta segment (RAM)`.
- Compaction drains RAM delta into disk segments in background.

### 5) Vector Index Design (Sidecar)

Constraints:
- Remote embeddings are slow/unpredictable relative to 50ms.
- We want parallelism: vectors must not block graph/lex commit.

Approach:
- Vector sidecar is derived from `(chunk_key, embedding)` stream.
- Maintain:
  - In-memory delta overlay for newest embeddings (instant query inclusion).
  - Persistent append log/segments for durability and rebuild.
  - Background index build/merge (IVF/HNSW/etc; implementation choice).

Semantics:
- `search(mode=Now)` includes embeddings present in overlays/sidecar at call time.
- `await_indexed(commit_ts, max_wait_ms)` waits until all chunks with graph `commit_ts` are embedded+applied
  (or until timeout), then returns “fully semantic” results.

### 6) Query + Rerank Pipeline

Candidate generation (always fast):
- BM25 top-K from lex.
- Trigram top-K (fuzzy) unioned in.
- Structural boosts/filters from graph:
  - same file/module/package,
  - def/ref edges proximity,
  - recency / edit locality,
  - kind boosts (definitions > refs > comments).

Rerankers (optional, progressive):
- Vector similarity rerank (when available).
- Cross-encoder rerank (optional, likely remote; behind explicit wait/budget).
- Heuristic rerank (cheap).

API should allow:
- `budget_ms` and `mode` so callers can decide “instant” vs “quality”.
- returning `IndexStatus` (vector lag, pending counts) so UX can display “refining…”.

## API Sketch (core; bindings mirror)

- `apply_changes(changes, opts) -> commit_ts`
  - opts: `durability` (graph), `lex_persist` (sync/async), `enqueue_vectors` (default true)
- `search(query, opts) -> SearchResult`
  - opts: `mode=Now|Durable`, filters, `budget_ms`, `rerank=auto|off|vectors`
- `await_indexed(commit_ts, max_wait_ms) -> IndexStatus`
- `index_status() -> IndexStatus`
  - queue depth, oldest pending age, vector applied watermark, lex persisted watermark.

## Crash Recovery

Graph:
- authoritative; existing WAL/checkpoint guarantees apply.

Lex/vector sidecars:
- may lag or be partially written.
- on open:
  - validate sidecar headers + segment tables.
  - if corrupt: drop sidecar and rebuild from graph chunk text (and cached embeddings, if any).
  - if merely behind: continue; background catch-up from in-memory overlay starts once process runs.

Embedding cache:
- keyed by `(model_id, content_hash)`; stored outside graph durability concerns; can be reused across rebuilds.

## Performance Targets + Bench Plan

Targets:
- Hot path (graph + lex overlay update): p95 < 50ms for “tens of chunks”.
- Vector freshness: p95 < 2s for “tens of chunks” with remote embeddings under typical latency.

Benchmarks:
- Incremental edit batch benchmark:
  - apply 10/25/50 chunk updates → measure apply latency and immediate search correctness.
- Vector freshness benchmark:
  - synthetic remote latency distribution → measure `await_indexed` success under 2s.
- Compaction benchmarks:
  - tombstone ratios; merge cost; query regression thresholds.

## Rollout Plan

Phase 0 (MVP):
- Lex sidecar with in-memory delta; BM25 + trigram; query pipeline + structural boosts.
- Vector sidecar: append-only + delta overlay; brute-force search over delta (if needed) + existing store.
- `await_indexed()` + `index_status()`.

Phase 1:
- Vector ANN build/merge + compaction policies.
- Better coalescing + batching; per-file debounce; priority queues.

Phase 2:
- Optional “durable vectors” mode; stronger cross-file commit invariants if demanded by users.

## Open Questions

- chunk_key choice: `NodeId` vs stable `(path, span)`; cross-session stability requirements.
- Where chunk text lives: always in graph vs content-addressed blob store.
- Sidecar durability defaults: fsync policy for lex/vec (likely “async + rebuildable”).
- Multi-process access: do we lock the directory? read-only concurrent readers?
- Vector index algorithm choice (HNSW vs IVF vs hybrid); rebuild/merge characteristics.

