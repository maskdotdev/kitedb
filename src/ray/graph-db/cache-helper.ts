import type { GraphDB } from "../../types.ts";
import { CacheManager } from "../../cache/index.ts";

/**
 * Get cache manager from database
 */
export function getCache(db: GraphDB): CacheManager | null {
  return (db._cache as CacheManager | undefined) || null;
}

