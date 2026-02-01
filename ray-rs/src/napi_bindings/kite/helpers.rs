//! Internal helper functions for Kite operations
//!
//! Contains utility functions for node/edge conversion, filtering,
//! transaction handling, and batch operations.

use napi::bindgen_prelude::*;
use napi::UnknownRef;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::api::kite::{BatchOp, BatchResult, Kite as RustKite, NodeRef};
use crate::api::traversal::TraversalDirection;
use crate::graph::edges::{
  add_edge as graph_add_edge, delete_edge as graph_delete_edge,
  get_edge_props as graph_get_edge_props, get_edge_props_db,
};
use crate::graph::iterators::{list_edges as graph_list_edges, ListEdgesOptions};
use crate::graph::key_index::get_node_key as graph_get_node_key;
use crate::graph::nodes::{
  del_node_prop as graph_del_node_prop, get_node_props_db, set_node_prop as graph_set_node_prop,
};
use crate::graph::tx::TxHandle;
use crate::types::{ETypeId, Edge, EdgePatch, NodeId, PropValue, TxState};

use super::key_spec::KeySpec;

// =============================================================================
// JS Value Output Conversion
// =============================================================================

/// Convert a PropValue to a JS Unknown value
pub(crate) fn prop_value_to_js(env: &Env, value: PropValue) -> Result<Unknown> {
  match value {
    PropValue::Null => Null.into_unknown(env),
    PropValue::Bool(v) => v.into_unknown(env),
    PropValue::I64(v) => v.into_unknown(env),
    PropValue::F64(v) => v.into_unknown(env),
    PropValue::String(v) => v.into_unknown(env),
    PropValue::VectorF32(v) => {
      let values: Vec<f64> = v.iter().map(|&value| value as f64).collect();
      values.into_unknown(env)
    }
  }
}

/// Convert a BatchResult to a JS Object
pub(crate) fn batch_result_to_js(env: &Env, result: BatchResult) -> Result<Object<'static>> {
  let mut obj = Object::new(env)?;
  match result {
    BatchResult::NodeCreated(node_ref) => {
      obj.set_named_property("type", "nodeCreated")?;
      let node_obj = node_to_js(
        env,
        node_ref.id,
        node_ref.key,
        &node_ref.node_type,
        HashMap::new(),
      )?;
      obj.set_named_property("node", node_obj)?;
    }
    BatchResult::NodeDeleted(deleted) => {
      obj.set_named_property("type", "nodeDeleted")?;
      obj.set_named_property("deleted", deleted)?;
    }
    BatchResult::EdgeCreated => {
      obj.set_named_property("type", "edgeCreated")?;
    }
    BatchResult::EdgeRemoved(deleted) => {
      obj.set_named_property("type", "edgeRemoved")?;
      obj.set_named_property("deleted", deleted)?;
    }
    BatchResult::PropSet => {
      obj.set_named_property("type", "propSet")?;
    }
    BatchResult::PropDeleted => {
      obj.set_named_property("type", "propDeleted")?;
    }
  }
  Ok(Object::from_raw(env.raw(), obj.raw()))
}

/// Create a JS node object with properties
pub(crate) fn node_to_js(
  env: &Env,
  node_id: NodeId,
  node_key: Option<String>,
  node_type: &str,
  props: HashMap<String, PropValue>,
) -> Result<Object<'static>> {
  let mut obj = Object::new(env)?;
  obj.set_named_property("id", node_id as i64)?;
  obj.set_named_property("key", node_key.as_deref().unwrap_or(""))?;
  obj.set_named_property("type", node_type)?;

  for (name, value) in props {
    let js_value = prop_value_to_js(env, value)?;
    obj.set_named_property(&name, js_value)?;
  }

  Ok(Object::from_raw(env.raw(), obj.raw()))
}

// =============================================================================
// Filter Data Structures
// =============================================================================

/// Data for filtering nodes
pub(crate) struct NodeFilterData {
  pub id: NodeId,
  pub key: String,
  pub node_type: String,
  pub props: HashMap<String, PropValue>,
}

/// Data for filtering edges
pub(crate) struct EdgeFilterData {
  pub src: NodeId,
  pub dst: NodeId,
  pub etype: ETypeId,
  pub props: HashMap<String, PropValue>,
}

/// Combined filter item for traversal
pub(crate) struct TraversalFilterItem {
  pub node_id: NodeId,
  pub edge: Option<Edge>,
  pub node: NodeFilterData,
  pub edge_info: Option<EdgeFilterData>,
}

/// Create node filter data from a node ID
pub(crate) fn node_filter_data(
  ray: &RustKite,
  node_id: NodeId,
  selected_props: Option<&HashSet<String>>,
) -> NodeFilterData {
  let node_ref = ray.get_by_id(node_id).ok().flatten();
  let (key, node_type) = match node_ref {
    Some(node_ref) => (node_ref.key.unwrap_or_default(), node_ref.node_type),
    None => ("".to_string(), "unknown".to_string()),
  };

  let props = get_node_props_selected(ray, node_id, selected_props);

  NodeFilterData {
    id: node_id,
    key,
    node_type,
    props,
  }
}

/// Create edge filter data from an edge
pub(crate) fn edge_filter_data(ray: &RustKite, edge: &Edge) -> EdgeFilterData {
  let mut props = HashMap::new();
  if let Some(props_by_id) = get_edge_props_db(ray.raw(), edge.src, edge.etype, edge.dst) {
    for (key_id, value) in props_by_id {
      if let Some(name) = ray.raw().get_propkey_name(key_id) {
        props.insert(name, value);
      }
    }
  }

  EdgeFilterData {
    src: edge.src,
    dst: edge.dst,
    etype: edge.etype,
    props,
  }
}

/// Create a JS object for node filtering
pub(crate) fn node_filter_arg(env: &Env, data: &NodeFilterData) -> Result<Object<'static>> {
  let mut obj = Object::new(env)?;
  obj.set_named_property("id", data.id as i64)?;
  obj.set_named_property("key", data.key.as_str())?;
  obj.set_named_property("type", data.node_type.as_str())?;
  for (name, value) in &data.props {
    let js_value = prop_value_to_js(env, value.clone())?;
    obj.set_named_property(name, js_value)?;
  }
  Ok(Object::from_raw(env.raw(), obj.raw()))
}

/// Create a JS object for edge filtering
pub(crate) fn edge_filter_arg(env: &Env, data: &EdgeFilterData) -> Result<Object<'static>> {
  let mut obj = Object::new(env)?;
  obj.set_named_property("src", data.src as i64)?;
  obj.set_named_property("dst", data.dst as i64)?;
  obj.set_named_property("etype", data.etype)?;
  for (name, value) in &data.props {
    let js_value = prop_value_to_js(env, value.clone())?;
    obj.set_named_property(name, js_value)?;
  }
  Ok(Object::from_raw(env.raw(), obj.raw()))
}

/// Call a JS filter function with an argument
#[allow(clippy::arc_with_non_send_sync)]
pub(crate) fn call_filter(
  env: &Env,
  func_ref: &Arc<UnknownRef<false>>,
  arg: Object,
) -> Result<bool> {
  let func_value = func_ref.get_value(env)?;
  let func: Function<Unknown, Unknown> = unsafe { func_value.cast()? };
  let result: Unknown = func.call(arg.into_unknown(env)?)?;
  result.coerce_to_bool()
}

// =============================================================================
// Property Selection Helpers
// =============================================================================

/// Check if a property should be included based on selection
pub(crate) fn should_include_prop(selected_props: Option<&HashSet<String>>, name: &str) -> bool {
  selected_props.is_none_or(|set| set.contains(name))
}

/// Get node properties with optional selection
pub(crate) fn get_node_props_selected(
  ray: &RustKite,
  node_id: NodeId,
  selected_props: Option<&HashSet<String>>,
) -> HashMap<String, PropValue> {
  let mut props = HashMap::new();
  if let Some(props_by_id) = get_node_props_db(ray.raw(), node_id) {
    for (key_id, value) in props_by_id {
      if let Some(name) = ray.raw().get_propkey_name(key_id) {
        if should_include_prop(selected_props, &name) {
          props.insert(name, value);
        }
      }
    }
  }
  props
}

/// Get all node properties
pub(crate) fn get_node_props(ray: &RustKite, node_id: NodeId) -> HashMap<String, PropValue> {
  get_node_props_selected(ray, node_id, None)
}

// =============================================================================
// Transaction Property Helpers
// =============================================================================

/// Get node properties within a transaction with optional selection
pub(crate) fn get_node_props_tx_selected(
  handle: &TxHandle,
  node_id: NodeId,
  selected_props: Option<&HashSet<String>>,
) -> HashMap<String, PropValue> {
  if handle.tx.pending_deleted_nodes.contains(&node_id) {
    return HashMap::new();
  }

  let mut props = HashMap::new();
  if let Some(props_by_id) = get_node_props_db(handle.db, node_id) {
    for (key_id, value) in props_by_id {
      if let Some(name) = handle.db.get_propkey_name(key_id) {
        if should_include_prop(selected_props, &name) {
          props.insert(name, value);
        }
      }
    }
  }

  if let Some(pending_props) = handle.tx.pending_node_props.get(&node_id) {
    for (key_id, value_opt) in pending_props {
      if let Some(name) = handle.db.get_propkey_name(*key_id) {
        if !should_include_prop(selected_props, &name) {
          continue;
        }
        match value_opt {
          Some(value) => {
            props.insert(name, value.clone());
          }
          None => {
            props.remove(&name);
          }
        }
      }
    }
  }

  props
}

/// Get node properties within a transaction
pub(crate) fn get_node_props_tx(
  _ray: &RustKite,
  handle: &TxHandle,
  node_id: NodeId,
) -> HashMap<String, PropValue> {
  get_node_props_tx_selected(handle, node_id, None)
}

/// Get node key within a transaction
pub(crate) fn get_node_key_tx(handle: &TxHandle, node_id: NodeId) -> Option<String> {
  if handle.tx.pending_deleted_nodes.contains(&node_id) {
    return None;
  }

  if let Some(delta) = handle.tx.pending_created_nodes.get(&node_id) {
    if let Some(key) = &delta.key {
      return Some(key.clone());
    }
  }

  for (key, id) in &handle.tx.pending_key_updates {
    if *id == node_id {
      return Some(key.clone());
    }
  }

  let delta = handle.db.delta.read();
  let key = graph_get_node_key(handle.db.snapshot.as_ref(), &delta, node_id);
  if let Some(ref key_str) = key {
    if handle.tx.pending_key_deletes.contains(key_str) {
      return None;
    }
  }
  key
}

/// Get edge properties within a transaction
pub(crate) fn get_edge_props_tx(
  handle: &TxHandle,
  src: NodeId,
  etype: ETypeId,
  dst: NodeId,
) -> HashMap<String, PropValue> {
  let mut props = HashMap::new();
  if let Some(props_by_id) = graph_get_edge_props(handle, src, etype, dst) {
    for (key_id, value) in props_by_id {
      if let Some(name) = handle.db.get_propkey_name(key_id) {
        props.insert(name, value);
      }
    }
  }
  props
}

// =============================================================================
// Node Type Inference
// =============================================================================

/// Infer node type from key prefix
pub(crate) fn node_type_from_key(
  node_specs: &HashMap<String, KeySpec>,
  key: &str,
) -> Option<String> {
  node_specs
    .iter()
    .find(|(_, spec)| key.starts_with(spec.prefix()))
    .map(|(name, _)| name.clone())
}

// =============================================================================
// Transaction Handle Helpers
// =============================================================================

/// Execute a read operation with a transaction handle
pub(crate) fn with_tx_handle<R>(
  ray: &Arc<RwLock<Option<RustKite>>>,
  tx_state: &Arc<Mutex<Option<TxState>>>,
  f: impl FnOnce(&RustKite, &mut TxHandle) -> Result<R>,
) -> Result<R> {
  let guard = ray.read();
  let ray = guard
    .as_ref()
    .ok_or_else(|| Error::from_reason("Kite is closed"))?;
  let mut tx_guard = tx_state.lock();
  let tx_state = tx_guard
    .take()
    .ok_or_else(|| Error::from_reason("No active transaction"))?;
  let mut handle = TxHandle::new(ray.raw(), tx_state);
  let result = f(ray, &mut handle);
  *tx_guard = Some(handle.tx);
  result
}

/// Execute a write operation with a transaction handle
pub(crate) fn with_tx_handle_mut<R>(
  ray: &Arc<RwLock<Option<RustKite>>>,
  tx_state: &Arc<Mutex<Option<TxState>>>,
  f: impl FnOnce(&RustKite, &mut TxHandle) -> Result<R>,
) -> Result<R> {
  let guard = ray.write();
  let ray = guard
    .as_ref()
    .ok_or_else(|| Error::from_reason("Kite is closed"))?;
  let mut tx_guard = tx_state.lock();
  let tx_state = tx_guard
    .take()
    .ok_or_else(|| Error::from_reason("No active transaction"))?;
  let mut handle = TxHandle::new(ray.raw(), tx_state);
  let result = f(ray, &mut handle);
  *tx_guard = Some(handle.tx);
  result
}

// =============================================================================
// Edge Listing with Transaction
// =============================================================================

/// List edges respecting transaction state
pub(crate) fn list_edges_with_tx(handle: &TxHandle, etype_filter: Option<ETypeId>) -> Vec<Edge> {
  let base_edges = graph_list_edges(
    handle.db,
    ListEdgesOptions {
      etype: etype_filter,
    },
  );

  let mut edges: HashSet<(NodeId, ETypeId, NodeId)> = base_edges
    .into_iter()
    .map(|edge| (edge.src, edge.etype, edge.dst))
    .collect();

  if !handle.tx.pending_deleted_nodes.is_empty() {
    edges.retain(|(src, _, dst)| {
      !handle.tx.pending_deleted_nodes.contains(src)
        && !handle.tx.pending_deleted_nodes.contains(dst)
    });
  }

  for (&src, del_set) in &handle.tx.pending_out_del {
    for patch in del_set {
      if etype_filter.is_some() && etype_filter != Some(patch.etype) {
        continue;
      }
      edges.remove(&(src, patch.etype, patch.other));
    }
  }

  for (&src, add_set) in &handle.tx.pending_out_add {
    for patch in add_set {
      if etype_filter.is_some() && etype_filter != Some(patch.etype) {
        continue;
      }
      edges.insert((src, patch.etype, patch.other));
    }
  }

  edges
    .into_iter()
    .map(|(src, etype, dst)| Edge { src, etype, dst })
    .collect()
}

// =============================================================================
// Batch Operations
// =============================================================================

/// Execute a batch of operations
pub(crate) fn execute_batch_ops(
  ray: &RustKite,
  handle: &mut TxHandle,
  ops: Vec<BatchOp>,
) -> Result<Vec<BatchResult>> {
  let mut results = Vec::with_capacity(ops.len());

  for op in ops {
    let result = match op {
      BatchOp::CreateNode {
        node_type,
        key_suffix,
        props,
      } => {
        let node_def = ray
          .node_def(&node_type)
          .ok_or_else(|| Error::from_reason(format!("Unknown node type: {node_type}")))?;

        let full_key = node_def.key(&key_suffix);
        let node_opts = crate::graph::nodes::NodeOpts {
          key: Some(full_key.clone()),
          labels: node_def.label_id.map(|id| vec![id]),
          props: None,
        };
        let node_id = crate::graph::nodes::create_node(handle, node_opts)
          .map_err(|e| Error::from_reason(e.to_string()))?;

        for (prop_name, value) in props {
          if let Some(&prop_key_id) = node_def.prop_key_ids.get(&prop_name) {
            graph_set_node_prop(handle, node_id, prop_key_id, value)
              .map_err(|e| Error::from_reason(e.to_string()))?;
          }
        }

        BatchResult::NodeCreated(NodeRef::new(node_id, Some(full_key), &node_type))
      }

      BatchOp::DeleteNode { node_id } => {
        let deleted = crate::graph::nodes::delete_node(handle, node_id)
          .map_err(|e| Error::from_reason(e.to_string()))?;
        BatchResult::NodeDeleted(deleted)
      }

      BatchOp::Link {
        src,
        edge_type,
        dst,
      } => {
        let edge_def = ray
          .edge_def(&edge_type)
          .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {edge_type}")))?;
        let etype_id = edge_def
          .etype_id
          .ok_or_else(|| Error::from_reason("Edge type not initialized"))?;
        graph_add_edge(handle, src, etype_id, dst)
          .map_err(|e| Error::from_reason(e.to_string()))?;
        BatchResult::EdgeCreated
      }

      BatchOp::Unlink {
        src,
        edge_type,
        dst,
      } => {
        let edge_def = ray
          .edge_def(&edge_type)
          .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {edge_type}")))?;
        let etype_id = edge_def
          .etype_id
          .ok_or_else(|| Error::from_reason("Edge type not initialized"))?;
        let deleted = graph_delete_edge(handle, src, etype_id, dst)
          .map_err(|e| Error::from_reason(e.to_string()))?;
        BatchResult::EdgeRemoved(deleted)
      }

      BatchOp::SetProp {
        node_id,
        prop_name,
        value,
      } => {
        let prop_key_id = handle.db.get_or_create_propkey(&prop_name);
        graph_set_node_prop(handle, node_id, prop_key_id, value)
          .map_err(|e| Error::from_reason(e.to_string()))?;
        BatchResult::PropSet
      }

      BatchOp::DelProp { node_id, prop_name } => {
        let prop_key_id = handle
          .db
          .get_propkey_id(&prop_name)
          .ok_or_else(|| Error::from_reason(format!("Unknown property: {prop_name}")))?;
        graph_del_node_prop(handle, node_id, prop_key_id)
          .map_err(|e| Error::from_reason(e.to_string()))?;
        BatchResult::PropDeleted
      }
    };

    results.push(result);
  }

  Ok(results)
}

// =============================================================================
// Neighbor Traversal
// =============================================================================

/// Get neighbors for a node in a given direction
pub(crate) fn get_neighbors(
  db: &crate::graph::db::GraphDB,
  node_id: NodeId,
  direction: TraversalDirection,
  etype: Option<ETypeId>,
) -> Vec<Edge> {
  let mut edges = Vec::new();
  let delta = db.delta.read();

  match direction {
    TraversalDirection::Out => {
      let deleted_set = delta.out_del.get(&node_id);

      if let Some(ref snapshot) = db.snapshot {
        if let Some(src_phys) = snapshot.get_phys_node(node_id) {
          for (dst_phys, edge_etype) in snapshot.iter_out_edges(src_phys) {
            if etype.is_some() && etype != Some(edge_etype) {
              continue;
            }

            if let Some(dst_id) = snapshot.get_node_id(dst_phys) {
              let is_deleted = deleted_set
                .map(|set| {
                  set.contains(&EdgePatch {
                    etype: edge_etype,
                    other: dst_id,
                  })
                })
                .unwrap_or(false);

              if !is_deleted {
                edges.push(Edge {
                  src: node_id,
                  etype: edge_etype,
                  dst: dst_id,
                });
              }
            }
          }
        }
      }

      if let Some(add_set) = delta.out_add.get(&node_id) {
        for patch in add_set {
          if (etype.is_none() || etype == Some(patch.etype))
            && !edges
              .iter()
              .any(|e| e.dst == patch.other && e.etype == patch.etype)
          {
            edges.push(Edge {
              src: node_id,
              etype: patch.etype,
              dst: patch.other,
            });
          }
        }
      }
    }
    TraversalDirection::In => {
      let deleted_set = delta.in_del.get(&node_id);

      if let Some(ref snapshot) = db.snapshot {
        if let Some(dst_phys) = snapshot.get_phys_node(node_id) {
          for (src_phys, edge_etype, _out_idx) in snapshot.iter_in_edges(dst_phys) {
            if etype.is_some() && etype != Some(edge_etype) {
              continue;
            }

            if let Some(src_id) = snapshot.get_node_id(src_phys) {
              let is_deleted = deleted_set
                .map(|set| {
                  set.contains(&EdgePatch {
                    etype: edge_etype,
                    other: src_id,
                  })
                })
                .unwrap_or(false);

              if !is_deleted {
                edges.push(Edge {
                  src: src_id,
                  etype: edge_etype,
                  dst: node_id,
                });
              }
            }
          }
        }
      }

      if let Some(add_set) = delta.in_add.get(&node_id) {
        for patch in add_set {
          if (etype.is_none() || etype == Some(patch.etype))
            && !edges
              .iter()
              .any(|e| e.src == patch.other && e.etype == patch.etype)
          {
            edges.push(Edge {
              src: patch.other,
              etype: patch.etype,
              dst: node_id,
            });
          }
        }
      }
    }
    TraversalDirection::Both => {
      edges.extend(get_neighbors(db, node_id, TraversalDirection::Out, etype));
      edges.extend(get_neighbors(db, node_id, TraversalDirection::In, etype));
    }
  }

  edges
}
