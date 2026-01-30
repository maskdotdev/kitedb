/**
 * Ray High-Level API
 *
 * Drizzle-style, type-safe API for the Ray graph database.
 * Provides schema definition, query building, and graph traversal with
 * full TypeScript type inference.
 *
 * @module
 *
 * @example
 * ```ts
 * import { ray, node, edge, prop } from './src/api';
 *
 * const user = node('user', {
 *   key: (id: string) => `user:${id}`,
 *   props: { name: prop.string('name') },
 * });
 *
 * const knows = edge('knows');
 *
 * const db = await ray('./my-db', {
 *   nodes: [user],
 *   edges: [knows],
 * });
 * ```
 */

/**
 * Query builders for CRUD operations
 * Fluent API for insert, update, delete with where conditions
 * @see {@link InsertBuilder}
 * @see {@link UpdateBuilder}
 * @see {@link DeleteBuilder}
 */
export type {
  DeleteBuilder,
  DeleteExecutor,
  InsertBuilder,
  InsertExecutor,
  LinkExecutor,
  NodeRef,
  UpdateBuilder,
  UpdateByRefBuilder,
  UpdateByRefExecutor,
  UpdateEdgeBuilder,
  UpdateEdgeExecutor,
  UpdateExecutor,
  WhereCondition,
} from "./builders.js";
/**
 * Main database context
 * @see {@link ray}
 * @see {@link Ray}
 */
export { Ray, type RayOptions, ray, type TransactionContext } from "./ray.js";
/**
 * Schema definition builders
 * Define node and edge types with properties and type inference
 * @see {@link node}
 * @see {@link edge}
 * @see {@link prop}
 */
export {
  // New API (0.2.0+)
  node,
  edge,
  // Legacy aliases (deprecated)
  defineEdge,
  defineNode,
  // Types
  type EdgeDef,
  type EdgePropsSchema,
  type InferEdge,
  type InferEdgeProps,
  type InferNode,
  type InferNodeInsert,
  type NodeDef,
  type OptionalPropDef,
  optional,
  type PropBuilder,
  type PropDef,
  type PropsSchema,
  prop,
  type RaySchema,
} from "./schema.js";

/**
 * Graph traversal with filtering and aggregation
 * Chain traversal steps (out, in, both) with lazy async iteration
 * @see {@link TraversalBuilder}
 */
export type {
  AsyncTraversalResult,
  EdgeResult,
  RawEdge,
  TraversalBuilder,
  TraversalDirection,
  TraverseOptions,
} from "./traversal.js";

/**
 * Path finding algorithms (Dijkstra, A*)
 * Find shortest paths between nodes with weighted edges
 * @see {@link PathFindingBuilder}
 */
export type {
  Heuristic,
  PathExecutor,
  PathFindingBuilder,
  PathResult,
  WeightSpec,
} from "./pathfinding.js";
