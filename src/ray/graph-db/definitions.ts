import type {
  GraphDB,
  LabelID,
  ETypeID,
  PropKeyID,
  TxHandle,
} from "../../types.ts";

/**
 * Define a new label
 */
export function defineLabel(handle: TxHandle, name: string): LabelID {
  const { _db: db, _tx: tx } = handle;
  
  // Check if already defined in delta (from WAL replay)
  for (const [existingId, existingName] of db._delta.newLabels) {
    if (existingName === name) {
      return existingId;
    }
  }
  
  // Check pending in current transaction
  for (const [existingId, existingName] of tx.pendingNewLabels) {
    if (existingName === name) {
      return existingId;
    }
  }
  
  // Create new
  const labelId = db._nextLabelId++;
  tx.pendingNewLabels.set(labelId, name);
  return labelId;
}

/**
 * Define a new edge type
 */
export function defineEtype(handle: TxHandle, name: string): ETypeID {
  const { _db: db, _tx: tx } = handle;
  
  // Check if already defined in delta (from WAL replay)
  for (const [existingId, existingName] of db._delta.newEtypes) {
    if (existingName === name) {
      return existingId;
    }
  }
  
  // Check pending in current transaction
  for (const [existingId, existingName] of tx.pendingNewEtypes) {
    if (existingName === name) {
      return existingId;
    }
  }
  
  // Create new
  const etypeId = db._nextEtypeId++;
  tx.pendingNewEtypes.set(etypeId, name);
  return etypeId;
}

/**
 * Define a new property key
 */
export function definePropkey(handle: TxHandle, name: string): PropKeyID {
  const { _db: db, _tx: tx } = handle;
  
  // Check if already defined in delta (from WAL replay)
  for (const [existingId, existingName] of db._delta.newPropkeys) {
    if (existingName === name) {
      return existingId;
    }
  }
  
  // Check pending in current transaction
  for (const [existingId, existingName] of tx.pendingNewPropkeys) {
    if (existingName === name) {
      return existingId;
    }
  }
  
  // Create new
  const propkeyId = db._nextPropkeyId++;
  tx.pendingNewPropkeys.set(propkeyId, name);
  return propkeyId;
}

