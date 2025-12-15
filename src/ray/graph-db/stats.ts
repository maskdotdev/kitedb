import { COMPACT_WAL_SIZE } from "../../constants.ts";
import { getDeltaStats } from "../../core/delta.ts";
import { checkSnapshot as checkSnapshotFn } from "../../check/checker.ts";
import type { CheckResult, DbStats, GraphDB } from "../../types.ts";

/**
 * Get database statistics
 */
export function stats(db: GraphDB): DbStats {
  const snapshotNodes = db._snapshot ? db._snapshot.header.numNodes : 0n;
  const snapshotEdges = db._snapshot ? db._snapshot.header.numEdges : 0n;
  const snapshotMaxNodeId = db._snapshot ? db._snapshot.header.maxNodeId : 0n;

  const deltaStats = getDeltaStats(db._delta);

  const recommendCompact =
    BigInt(deltaStats.edgesAdded + deltaStats.edgesDeleted) >
      snapshotEdges / 10n ||
    BigInt(deltaStats.nodesCreated + deltaStats.nodesDeleted) >
      snapshotNodes / 10n ||
    db._walOffset > COMPACT_WAL_SIZE;

  return {
    snapshotGen: db._manifest.activeSnapshotGen,
    snapshotNodes,
    snapshotEdges,
    snapshotMaxNodeId,
    deltaNodesCreated: BigInt(deltaStats.nodesCreated),
    deltaNodesDeleted: BigInt(deltaStats.nodesDeleted),
    deltaEdgesAdded: BigInt(deltaStats.edgesAdded),
    deltaEdgesDeleted: BigInt(deltaStats.edgesDeleted),
    walSegment: db._manifest.activeWalSeg,
    walBytes: BigInt(db._walOffset),
    recommendCompact,
  };
}

/**
 * Check database integrity
 */
export function check(db: GraphDB): CheckResult {
  if (!db._snapshot) {
    return { valid: true, errors: [], warnings: ["No snapshot to check"] };
  }

  return checkSnapshotFn(db._snapshot);
}

