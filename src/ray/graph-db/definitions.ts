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
  const labelId = db._nextLabelId++;
  tx.pendingNewLabels.set(labelId, name);
  return labelId;
}

/**
 * Define a new edge type
 */
export function defineEtype(handle: TxHandle, name: string): ETypeID {
  const { _db: db, _tx: tx } = handle;
  const etypeId = db._nextEtypeId++;
  tx.pendingNewEtypes.set(etypeId, name);
  return etypeId;
}

/**
 * Define a new property key
 */
export function definePropkey(handle: TxHandle, name: string): PropKeyID {
  const { _db: db, _tx: tx } = handle;
  const propkeyId = db._nextPropkeyId++;
  tx.pendingNewPropkeys.set(propkeyId, name);
  return propkeyId;
}

