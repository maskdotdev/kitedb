//! NAPI bindings for the high-level Kite API
//!
//! This module provides a fluent, type-safe API for building and querying
//! graph databases from Node.js/Bun.

mod builders;
mod conversion;
mod helpers;
mod key_spec;
mod kite_traversal;
mod pathfinding;
mod types;

// Re-export public types
pub use builders::{
  KiteInsertBuilder, KiteInsertExecutorMany, KiteInsertExecutorSingle, KiteUpdateBuilder,
  KiteUpdateEdgeBuilder, KiteUpsertBuilder, KiteUpsertByIdBuilder, KiteUpsertEdgeBuilder,
  KiteUpsertExecutorMany, KiteUpsertExecutorSingle,
};
pub use kite_traversal::KiteTraversal;
pub use pathfinding::{JsPathEdge, JsPathResult, KitePath};
pub use types::{JsEdgeSpec, JsKeySpec, JsKiteOptions, JsNodeSpec, JsPropSpec};

// Internal imports
use conversion::js_props_to_map;
use helpers::{
  batch_result_to_js, execute_batch_ops, get_edge_props_tx, get_node_key_tx, get_node_props,
  get_node_props_selected, get_node_props_tx, get_node_props_tx_selected, list_edges_with_tx,
  node_to_js, node_type_from_key, with_tx_handle, with_tx_handle_mut,
};
use key_spec::{parse_key_spec, prop_spec_to_def, KeySpec};

use napi::bindgen_prelude::*;
use napi_derive::napi;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::api::kite::{BatchOp, EdgeDef, Kite as RustKite, KiteOptions, NodeDef};
use crate::api::traversal::TraversalBuilder;
use crate::graph::edges::{
  add_edge as graph_add_edge, del_edge_prop as graph_del_edge_prop,
  delete_edge as graph_delete_edge, edge_exists as graph_edge_exists, edge_exists_db,
  get_edge_prop as graph_get_edge_prop, set_edge_prop, upsert_edge_with_props,
};
use crate::graph::iterators::{
  list_edges as graph_list_edges, list_nodes as graph_list_nodes, ListEdgesOptions,
};
use crate::graph::key_index::get_node_key as graph_get_node_key;
use crate::graph::nodes::{
  delete_node as graph_delete_node, get_node_by_key as graph_get_node_by_key, get_node_by_key_db,
  get_node_prop as graph_get_node_prop, node_exists as graph_node_exists,
  set_node_prop as graph_set_node_prop,
};
use crate::graph::tx::{begin_read_tx, begin_tx, commit, rollback, TxHandle};
use crate::types::{NodeId, PropValue, TxState};

use super::database::{CheckResult, DbStats, MvccStats};
use super::database::{JsFullEdge, JsPropValue};

use conversion::{js_value_to_prop_value, key_suffix_from_js};

// =============================================================================
// Kite Handle
// =============================================================================

/// High-level Kite database handle for Node.js/Bun.
///
/// # Thread Safety and Concurrent Access
///
/// Kite uses an internal RwLock to support concurrent operations:
///
/// - **Read operations** (get, exists, neighbors, traversals) use a shared read lock,
///   allowing multiple concurrent reads without blocking each other.
/// - **Write operations** (insert, update, link, delete) use an exclusive write lock,
///   blocking all other operations until complete.
///
/// This means you can safely call multiple read methods concurrently:
///
/// ```javascript
/// // These execute concurrently - reads don't block each other
/// const [user1, user2, user3] = await Promise.all([
///   db.get("User", "alice"),
///   db.get("User", "bob"),
///   db.get("User", "charlie"),
/// ]);
/// ```
///
/// Write operations will wait for in-progress reads and block new operations:
///
/// ```javascript
/// // This will wait for any in-progress reads, then block new reads
/// await db.insert("User").key("david").set("name", "David").execute();
/// ```
#[napi]
pub struct Kite {
  inner: Arc<RwLock<Option<RustKite>>>,
  node_specs: Arc<HashMap<String, KeySpec>>,
  tx_state: Arc<Mutex<Option<TxState>>>,
}

impl Kite {
  /// Execute a read operation with a shared lock.
  /// Multiple read operations can execute concurrently.
  fn with_kite<R>(&self, f: impl FnOnce(&RustKite) -> Result<R>) -> Result<R> {
    let guard = self.inner.read();
    let ray = guard
      .as_ref()
      .ok_or_else(|| Error::from_reason("Kite is closed"))?;
    f(ray)
  }

  /// Execute a write operation with an exclusive lock.
  /// This blocks all other operations until complete.
  fn with_kite_mut<R>(&self, f: impl FnOnce(&mut RustKite) -> Result<R>) -> Result<R> {
    let mut guard = self.inner.write();
    let ray = guard
      .as_mut()
      .ok_or_else(|| Error::from_reason("Kite is closed"))?;
    f(ray)
  }

  fn key_spec(&self, node_type: &str) -> Result<&KeySpec> {
    self
      .node_specs
      .get(node_type)
      .ok_or_else(|| Error::from_reason(format!("Unknown node type: {node_type}")))
  }
}

#[napi]
impl Kite {
  /// Open a Kite database
  #[napi(factory)]
  pub fn open(path: String, options: JsKiteOptions) -> Result<Self> {
    let mut node_specs: HashMap<String, KeySpec> = HashMap::new();
    let mut ray_opts = KiteOptions::new();
    ray_opts.read_only = options.read_only.unwrap_or(false);
    ray_opts.create_if_missing = options.create_if_missing.unwrap_or(true);
    ray_opts.lock_file = options.lock_file.unwrap_or(true);

    for node in options.nodes {
      let key_spec = parse_key_spec(&node.name, node.key)?;
      let prefix = key_spec.prefix().to_string();

      let mut node_def = NodeDef::new(&node.name, &prefix);
      if let Some(props) = node.props.as_ref() {
        for (prop_name, prop_spec) in props {
          node_def = node_def.prop(prop_spec_to_def(prop_name, prop_spec)?);
        }
      }

      node_specs.insert(node.name.clone(), key_spec);
      ray_opts.nodes.push(node_def);
    }

    for edge in options.edges {
      let mut edge_def = EdgeDef::new(&edge.name);
      if let Some(props) = edge.props.as_ref() {
        for (prop_name, prop_spec) in props {
          edge_def = edge_def.prop(prop_spec_to_def(prop_name, prop_spec)?);
        }
      }
      ray_opts.edges.push(edge_def);
    }

    let ray = RustKite::open(path, ray_opts).map_err(|e| Error::from_reason(e.to_string()))?;

    Ok(Kite {
      inner: Arc::new(RwLock::new(Some(ray))),
      node_specs: Arc::new(node_specs),
      tx_state: Arc::new(Mutex::new(None)),
    })
  }

  /// Close the database
  #[napi]
  pub fn close(&self) -> Result<()> {
    let mut guard = self.inner.write();
    if let Some(ray) = guard.as_ref() {
      let mut tx_guard = self.tx_state.lock();
      if let Some(tx_state) = tx_guard.take() {
        let mut handle = TxHandle::new(ray.raw(), tx_state);
        rollback(&mut handle)
          .map_err(|e| Error::from_reason(format!("Failed to rollback: {e}")))?;
      }
    }

    if let Some(ray) = guard.take() {
      ray.close().map_err(|e| Error::from_reason(e.to_string()))?;
    }
    Ok(())
  }

  /// Get a node by key (returns node object with props)
  #[napi]
  pub fn get(
    &self,
    env: Env,
    node_type: String,
    key: Unknown,
    props: Option<Vec<String>>,
  ) -> Result<Option<Object>> {
    let spec = self.key_spec(&node_type)?.clone();
    let key_suffix = key_suffix_from_js(&env, &spec, key)?;
    let full_key = format!("{}{}", spec.prefix(), key_suffix);
    let selected_props = props.map(|props| props.into_iter().collect::<HashSet<String>>());
    if self.tx_state.lock().is_some() {
      return with_tx_handle(&self.inner, &self.tx_state, |_ray, handle| {
        let node_id = graph_get_node_by_key(handle, &full_key);
        match node_id {
          Some(node_id) => {
            let props = get_node_props_tx_selected(handle, node_id, selected_props.as_ref());
            let obj = node_to_js(&env, node_id, Some(full_key), &node_type, props)?;
            Ok(Some(obj))
          }
          None => Ok(None),
        }
      });
    }

    self.with_kite(move |ray| {
      let node_ref = ray
        .get(&node_type, &key_suffix)
        .map_err(|e| Error::from_reason(e.to_string()))?;

      match node_ref {
        Some(node_ref) => {
          let props = get_node_props_selected(ray, node_ref.id, selected_props.as_ref());
          let obj = node_to_js(&env, node_ref.id, node_ref.key, &node_ref.node_type, props)?;
          Ok(Some(obj))
        }
        None => Ok(None),
      }
    })
  }

  /// Get a node by ID (returns node object with props)
  #[napi]
  pub fn get_by_id(
    &self,
    env: Env,
    node_id: i64,
    props: Option<Vec<String>>,
  ) -> Result<Option<Object>> {
    let selected_props = props.map(|props| props.into_iter().collect::<HashSet<String>>());
    if self.tx_state.lock().is_some() {
      let node_specs = self.node_specs.clone();
      return with_tx_handle(&self.inner, &self.tx_state, |_ray, handle| {
        let node_id = node_id as NodeId;
        if !graph_node_exists(handle, node_id) {
          return Ok(None);
        }
        let key = get_node_key_tx(handle, node_id);
        let node_type = key
          .as_ref()
          .and_then(|k| node_type_from_key(&node_specs, k))
          .unwrap_or_else(|| "unknown".to_string());
        let props = get_node_props_tx_selected(handle, node_id, selected_props.as_ref());
        let obj = node_to_js(&env, node_id, key, &node_type, props)?;
        Ok(Some(obj))
      });
    }

    self.with_kite(move |ray| {
      let node_ref = ray
        .get_by_id(node_id as NodeId)
        .map_err(|e| Error::from_reason(e.to_string()))?;
      match node_ref {
        Some(node_ref) => {
          let props = get_node_props_selected(ray, node_ref.id, selected_props.as_ref());
          let obj = node_to_js(&env, node_ref.id, node_ref.key, &node_ref.node_type, props)?;
          Ok(Some(obj))
        }
        None => Ok(None),
      }
    })
  }

  /// Get a lightweight node reference by key (no properties)
  #[napi]
  pub fn get_ref(&self, env: Env, node_type: String, key: Unknown) -> Result<Option<Object>> {
    let spec = self.key_spec(&node_type)?.clone();
    let key_suffix = key_suffix_from_js(&env, &spec, key)?;
    let full_key = format!("{}{}", spec.prefix(), key_suffix);
    if self.tx_state.lock().is_some() {
      return with_tx_handle(&self.inner, &self.tx_state, |_ray, handle| {
        let node_id = graph_get_node_by_key(handle, &full_key);
        match node_id {
          Some(node_id) => {
            let obj = node_to_js(&env, node_id, Some(full_key), &node_type, HashMap::new())?;
            Ok(Some(obj))
          }
          None => Ok(None),
        }
      });
    }

    self.with_kite(move |ray| {
      let node_ref = ray
        .get_ref(&node_type, &key_suffix)
        .map_err(|e| Error::from_reason(e.to_string()))?;

      match node_ref {
        Some(node_ref) => {
          let obj = node_to_js(
            &env,
            node_ref.id,
            node_ref.key,
            &node_ref.node_type,
            HashMap::new(),
          )?;
          Ok(Some(obj))
        }
        None => Ok(None),
      }
    })
  }

  /// Get a node ID by key (no properties)
  #[napi]
  pub fn get_id(&self, env: Env, node_type: String, key: Unknown) -> Result<Option<i64>> {
    let spec = self.key_spec(&node_type)?.clone();
    let key_suffix = key_suffix_from_js(&env, &spec, key)?;
    let full_key = format!("{}{}", spec.prefix(), key_suffix);
    if self.tx_state.lock().is_some() {
      return with_tx_handle(&self.inner, &self.tx_state, |_ray, handle| {
        Ok(graph_get_node_by_key(handle, &full_key).map(|id| id as i64))
      });
    }

    self.with_kite(move |ray| Ok(get_node_by_key_db(ray.raw(), &full_key).map(|id| id as i64)))
  }

  /// Get multiple nodes by ID (returns node objects with props)
  #[napi]
  pub fn get_by_ids(
    &self,
    env: Env,
    node_ids: Vec<i64>,
    props: Option<Vec<String>>,
  ) -> Result<Vec<Object>> {
    if node_ids.is_empty() {
      return Ok(Vec::new());
    }

    let selected_props = props.map(|props| props.into_iter().collect::<HashSet<String>>());
    if self.tx_state.lock().is_some() {
      let node_specs = self.node_specs.clone();
      return with_tx_handle(&self.inner, &self.tx_state, |_ray, handle| {
        let mut out = Vec::with_capacity(node_ids.len());
        for node_id in &node_ids {
          let node_id = *node_id as NodeId;
          if !graph_node_exists(handle, node_id) {
            continue;
          }
          let key = get_node_key_tx(handle, node_id);
          let node_type = key
            .as_ref()
            .and_then(|k| node_type_from_key(&node_specs, k))
            .unwrap_or_else(|| "unknown".to_string());
          let props = get_node_props_tx_selected(handle, node_id, selected_props.as_ref());
          out.push(node_to_js(&env, node_id, key, &node_type, props)?);
        }
        Ok(out)
      });
    }

    self.with_kite(move |ray| {
      let mut out = Vec::with_capacity(node_ids.len());
      for node_id in node_ids {
        let node_ref = ray
          .get_by_id(node_id as NodeId)
          .map_err(|e| Error::from_reason(e.to_string()))?;
        if let Some(node_ref) = node_ref {
          let props = get_node_props_selected(ray, node_ref.id, selected_props.as_ref());
          out.push(node_to_js(
            &env,
            node_ref.id,
            node_ref.key,
            &node_ref.node_type,
            props,
          )?);
        }
      }
      Ok(out)
    })
  }

  /// Get a node property value
  #[napi]
  pub fn get_prop(&self, node_id: i64, prop_name: String) -> Result<Option<JsPropValue>> {
    let value = if self.tx_state.lock().is_some() {
      with_tx_handle(&self.inner, &self.tx_state, |ray, handle| {
        let prop_key_id = ray.raw().get_propkey_id(&prop_name);
        Ok(prop_key_id.and_then(|id| graph_get_node_prop(handle, node_id as NodeId, id)))
      })?
    } else {
      self.with_kite(|ray| Ok(ray.get_prop(node_id as NodeId, &prop_name)))?
    };
    Ok(value.map(JsPropValue::from))
  }

  /// Set a node property value
  #[napi]
  pub fn set_prop(&self, env: Env, node_id: i64, prop_name: String, value: Unknown) -> Result<()> {
    let prop_value = js_value_to_prop_value(&env, value)?;
    if self.tx_state.lock().is_some() {
      return with_tx_handle_mut(&self.inner, &self.tx_state, |ray, handle| {
        let prop_key_id = ray.raw().get_or_create_propkey(&prop_name);
        graph_set_node_prop(handle, node_id as NodeId, prop_key_id, prop_value)
          .map_err(|e| Error::from_reason(e.to_string()))
      });
    }

    self.with_kite_mut(|ray| {
      ray
        .set_prop(node_id as NodeId, &prop_name, prop_value)
        .map_err(|e| Error::from_reason(e.to_string()))
    })
  }

  /// Check if a node exists
  #[napi]
  pub fn exists(&self, node_id: i64) -> Result<bool> {
    if self.tx_state.lock().is_some() {
      return with_tx_handle(&self.inner, &self.tx_state, |_ray, handle| {
        Ok(graph_node_exists(handle, node_id as NodeId))
      });
    }
    self.with_kite(|ray| Ok(ray.exists(node_id as NodeId)))
  }

  /// Delete a node by ID
  #[napi]
  pub fn delete_by_id(&self, node_id: i64) -> Result<bool> {
    if self.tx_state.lock().is_some() {
      return with_tx_handle_mut(&self.inner, &self.tx_state, |_ray, handle| {
        graph_delete_node(handle, node_id as NodeId).map_err(|e| Error::from_reason(e.to_string()))
      });
    }
    self.with_kite_mut(|ray| {
      ray
        .delete_node(node_id as NodeId)
        .map_err(|e| Error::from_reason(e.to_string()))
    })
  }

  /// Delete a node by key
  #[napi]
  pub fn delete_by_key(&self, env: Env, node_type: String, key: Unknown) -> Result<bool> {
    let spec = self.key_spec(&node_type)?.clone();
    let key_suffix = key_suffix_from_js(&env, &spec, key)?;
    let full_key = format!("{}{}", spec.prefix(), key_suffix);
    if self.tx_state.lock().is_some() {
      return with_tx_handle_mut(&self.inner, &self.tx_state, |_ray, handle| {
        let node_id = graph_get_node_by_key(handle, &full_key);
        match node_id {
          Some(id) => graph_delete_node(handle, id).map_err(|e| Error::from_reason(e.to_string())),
          None => Ok(false),
        }
      });
    }

    self.with_kite_mut(|ray| {
      let full_key = ray
        .node_def(&node_type)
        .ok_or_else(|| Error::from_reason(format!("Unknown node type: {node_type}")))?
        .key(&key_suffix);
      let node_id = get_node_by_key_db(ray.raw(), &full_key);
      match node_id {
        Some(id) => {
          let res = ray
            .delete_node(id)
            .map_err(|e| Error::from_reason(e.to_string()))?;
          Ok(res)
        }
        None => Ok(false),
      }
    })
  }

  /// Create an insert builder
  #[napi]
  pub fn insert(&self, node_type: String) -> Result<KiteInsertBuilder> {
    let spec = self.key_spec(&node_type)?.clone();
    let prefix = spec.prefix().to_string();
    Ok(KiteInsertBuilder::new(
      self.inner.clone(),
      self.tx_state.clone(),
      node_type,
      prefix,
      spec,
    ))
  }

  /// Create an upsert builder
  #[napi]
  pub fn upsert(&self, node_type: String) -> Result<KiteUpsertBuilder> {
    let spec = self.key_spec(&node_type)?.clone();
    let prefix = spec.prefix().to_string();
    Ok(KiteUpsertBuilder::new(
      self.inner.clone(),
      self.tx_state.clone(),
      node_type,
      prefix,
      spec,
    ))
  }

  /// Create an update builder by node ID
  #[napi]
  pub fn update_by_id(&self, node_id: i64) -> Result<KiteUpdateBuilder> {
    Ok(KiteUpdateBuilder::new(
      self.inner.clone(),
      self.tx_state.clone(),
      node_id as NodeId,
    ))
  }

  /// Create an upsert builder by node ID
  #[napi]
  pub fn upsert_by_id(&self, node_type: String, node_id: i64) -> Result<KiteUpsertByIdBuilder> {
    Ok(KiteUpsertByIdBuilder::new(
      self.inner.clone(),
      self.tx_state.clone(),
      node_type,
      node_id as NodeId,
    ))
  }

  /// Create an update builder by key
  #[napi]
  pub fn update_by_key(
    &self,
    env: Env,
    node_type: String,
    key: Unknown,
  ) -> Result<KiteUpdateBuilder> {
    let spec = self.key_spec(&node_type)?.clone();
    let key_suffix = key_suffix_from_js(&env, &spec, key)?;
    let full_key = format!("{}{}", spec.prefix(), key_suffix);
    let node_id = if self.tx_state.lock().is_some() {
      with_tx_handle(&self.inner, &self.tx_state, |_ray, handle| {
        Ok(graph_get_node_by_key(handle, &full_key))
      })?
    } else {
      self.with_kite(|ray| {
        let full_key = ray
          .node_def(&node_type)
          .ok_or_else(|| Error::from_reason(format!("Unknown node type: {node_type}")))?
          .key(&key_suffix);
        Ok(get_node_by_key_db(ray.raw(), &full_key))
      })?
    };

    match node_id {
      Some(node_id) => Ok(KiteUpdateBuilder::new(
        self.inner.clone(),
        self.tx_state.clone(),
        node_id,
      )),
      None => Err(Error::from_reason("Key not found")),
    }
  }

  /// Link two nodes
  #[napi]
  pub fn link(
    &self,
    env: Env,
    src: i64,
    edge_type: String,
    dst: i64,
    props: Option<Object>,
  ) -> Result<()> {
    let props_map = js_props_to_map(&env, props)?;
    if self.tx_state.lock().is_some() {
      return with_tx_handle_mut(&self.inner, &self.tx_state, |ray, handle| {
        let edge_def = ray
          .edge_def(&edge_type)
          .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {edge_type}")))?;
        let etype_id = edge_def
          .etype_id
          .ok_or_else(|| Error::from_reason("Edge type not initialized"))?;

        if props_map.is_empty() {
          graph_add_edge(handle, src as NodeId, etype_id, dst as NodeId)
            .map_err(|e| Error::from_reason(e.to_string()))
        } else {
          let mut updates = Vec::with_capacity(props_map.len());
          for (prop_name, value) in &props_map {
            let prop_key_id = ray.raw().get_or_create_propkey(prop_name);
            let value_opt = match value {
              PropValue::Null => None,
              other => Some(other.clone()),
            };
            updates.push((prop_key_id, value_opt));
          }
          upsert_edge_with_props(handle, src as NodeId, etype_id, dst as NodeId, updates)
            .map(|_| ())
            .map_err(|e| Error::from_reason(e.to_string()))
        }
      });
    }

    self.with_kite_mut(|ray| {
      if props_map.is_empty() {
        ray
          .link(src as NodeId, &edge_type, dst as NodeId)
          .map_err(|e| Error::from_reason(e.to_string()))
      } else {
        ray
          .link_with_props(src as NodeId, &edge_type, dst as NodeId, props_map)
          .map_err(|e| Error::from_reason(e.to_string()))
      }
    })
  }

  /// Unlink two nodes
  #[napi]
  pub fn unlink(&self, src: i64, edge_type: String, dst: i64) -> Result<bool> {
    if self.tx_state.lock().is_some() {
      return with_tx_handle_mut(&self.inner, &self.tx_state, |ray, handle| {
        let edge_def = ray
          .edge_def(&edge_type)
          .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {edge_type}")))?;
        let etype_id = edge_def
          .etype_id
          .ok_or_else(|| Error::from_reason("Edge type not initialized"))?;
        graph_delete_edge(handle, src as NodeId, etype_id, dst as NodeId)
          .map_err(|e| Error::from_reason(e.to_string()))
      });
    }

    self.with_kite_mut(|ray| {
      ray
        .unlink(src as NodeId, &edge_type, dst as NodeId)
        .map_err(|e| Error::from_reason(e.to_string()))
    })
  }

  /// Check if an edge exists
  #[napi]
  pub fn has_edge(&self, src: i64, edge_type: String, dst: i64) -> Result<bool> {
    if self.tx_state.lock().is_some() {
      return with_tx_handle(&self.inner, &self.tx_state, |ray, handle| {
        let edge_def = ray
          .edge_def(&edge_type)
          .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {edge_type}")))?;
        let etype_id = edge_def
          .etype_id
          .ok_or_else(|| Error::from_reason("Edge type not initialized"))?;
        Ok(graph_edge_exists(
          handle,
          src as NodeId,
          etype_id,
          dst as NodeId,
        ))
      });
    }

    self.with_kite(move |ray| {
      let edge_def = ray
        .edge_def(&edge_type)
        .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {edge_type}")))?;
      let etype_id = edge_def
        .etype_id
        .ok_or_else(|| Error::from_reason("Edge type not initialized"))?;
      Ok(edge_exists_db(
        ray.raw(),
        src as NodeId,
        etype_id,
        dst as NodeId,
      ))
    })
  }

  /// Get an edge property value
  #[napi]
  pub fn get_edge_prop(
    &self,
    src: i64,
    edge_type: String,
    dst: i64,
    prop_name: String,
  ) -> Result<Option<JsPropValue>> {
    let value = if self.tx_state.lock().is_some() {
      with_tx_handle(&self.inner, &self.tx_state, |ray, handle| {
        let edge_def = ray
          .edge_def(&edge_type)
          .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {edge_type}")))?;
        let etype_id = edge_def
          .etype_id
          .ok_or_else(|| Error::from_reason("Edge type not initialized"))?;
        let prop_key_id = ray.raw().get_propkey_id(&prop_name);
        Ok(
          prop_key_id
            .and_then(|id| graph_get_edge_prop(handle, src as NodeId, etype_id, dst as NodeId, id)),
        )
      })?
    } else {
      self.with_kite(|ray| {
        ray
          .get_edge_prop(src as NodeId, &edge_type, dst as NodeId, &prop_name)
          .map_err(|e| Error::from_reason(e.to_string()))
      })?
    };
    Ok(value.map(JsPropValue::from))
  }

  /// Get all edge properties
  #[napi]
  pub fn get_edge_props(
    &self,
    src: i64,
    edge_type: String,
    dst: i64,
  ) -> Result<HashMap<String, JsPropValue>> {
    let props = if self.tx_state.lock().is_some() {
      with_tx_handle(&self.inner, &self.tx_state, |ray, handle| {
        let edge_def = ray
          .edge_def(&edge_type)
          .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {edge_type}")))?;
        let etype_id = edge_def
          .etype_id
          .ok_or_else(|| Error::from_reason("Edge type not initialized"))?;
        Ok(get_edge_props_tx(
          handle,
          src as NodeId,
          etype_id,
          dst as NodeId,
        ))
      })?
    } else {
      let props_opt = self.with_kite(|ray| {
        ray
          .get_edge_props(src as NodeId, &edge_type, dst as NodeId)
          .map_err(|e| Error::from_reason(e.to_string()))
      })?;
      props_opt.unwrap_or_default()
    };

    Ok(
      props
        .into_iter()
        .map(|(key, value)| (key, JsPropValue::from(value)))
        .collect(),
    )
  }

  /// Set an edge property value
  #[napi]
  pub fn set_edge_prop(
    &self,
    env: Env,
    src: i64,
    edge_type: String,
    dst: i64,
    prop_name: String,
    value: Unknown,
  ) -> Result<()> {
    let prop_value = js_value_to_prop_value(&env, value)?;
    if self.tx_state.lock().is_some() {
      return with_tx_handle_mut(&self.inner, &self.tx_state, |ray, handle| {
        let edge_def = ray
          .edge_def(&edge_type)
          .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {edge_type}")))?;
        let etype_id = edge_def
          .etype_id
          .ok_or_else(|| Error::from_reason("Edge type not initialized"))?;
        let prop_key_id = ray.raw().get_or_create_propkey(&prop_name);
        set_edge_prop(
          handle,
          src as NodeId,
          etype_id,
          dst as NodeId,
          prop_key_id,
          prop_value,
        )
        .map_err(|e| Error::from_reason(e.to_string()))
      });
    }
    self.with_kite_mut(|ray| {
      ray
        .set_edge_prop(
          src as NodeId,
          &edge_type,
          dst as NodeId,
          &prop_name,
          prop_value,
        )
        .map_err(|e| Error::from_reason(e.to_string()))
    })
  }

  /// Delete an edge property
  #[napi]
  pub fn del_edge_prop(
    &self,
    src: i64,
    edge_type: String,
    dst: i64,
    prop_name: String,
  ) -> Result<()> {
    if self.tx_state.lock().is_some() {
      return with_tx_handle_mut(&self.inner, &self.tx_state, |ray, handle| {
        let edge_def = ray
          .edge_def(&edge_type)
          .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {edge_type}")))?;
        let etype_id = edge_def
          .etype_id
          .ok_or_else(|| Error::from_reason("Edge type not initialized"))?;
        if let Some(prop_key_id) = ray.raw().get_propkey_id(&prop_name) {
          graph_del_edge_prop(handle, src as NodeId, etype_id, dst as NodeId, prop_key_id)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        }
        Ok(())
      });
    }

    self.with_kite_mut(|ray| {
      ray
        .del_edge_prop(src as NodeId, &edge_type, dst as NodeId, &prop_name)
        .map_err(|e| Error::from_reason(e.to_string()))
    })
  }

  /// Update edge properties with a builder
  #[napi]
  pub fn update_edge(
    &self,
    src: i64,
    edge_type: String,
    dst: i64,
  ) -> Result<KiteUpdateEdgeBuilder> {
    let etype_id = self.with_kite(|ray| {
      let edge_def = ray
        .edge_def(&edge_type)
        .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {edge_type}")))?;
      edge_def
        .etype_id
        .ok_or_else(|| Error::from_reason("Edge type not initialized"))
    })?;

    Ok(KiteUpdateEdgeBuilder::new(
      self.inner.clone(),
      self.tx_state.clone(),
      src as NodeId,
      etype_id,
      dst as NodeId,
    ))
  }

  /// Upsert edge properties with a builder
  #[napi]
  pub fn upsert_edge(
    &self,
    src: i64,
    edge_type: String,
    dst: i64,
  ) -> Result<KiteUpsertEdgeBuilder> {
    let etype_id = self.with_kite(|ray| {
      let edge_def = ray
        .edge_def(&edge_type)
        .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {edge_type}")))?;
      edge_def
        .etype_id
        .ok_or_else(|| Error::from_reason("Edge type not initialized"))
    })?;

    Ok(KiteUpsertEdgeBuilder::new(
      self.inner.clone(),
      self.tx_state.clone(),
      src as NodeId,
      etype_id,
      dst as NodeId,
    ))
  }

  /// List all nodes of a type (returns array of node objects)
  #[napi]
  pub fn all(&self, env: Env, node_type: String) -> Result<Vec<Object>> {
    if self.tx_state.lock().is_some() {
      return with_tx_handle(&self.inner, &self.tx_state, |ray, handle| {
        let node_def = ray
          .node_def(&node_type)
          .ok_or_else(|| Error::from_reason(format!("Unknown node type: {node_type}")))?;
        let prefix = node_def.key_prefix.clone();
        let mut out = Vec::new();
        let mut seen = HashSet::new();

        for node_id in graph_list_nodes(handle.db) {
          if handle.tx.pending_deleted_nodes.contains(&node_id) {
            continue;
          }
          let key = get_node_key_tx(handle, node_id);
          let key = match key {
            Some(key) => key,
            None => continue,
          };
          if !key.starts_with(&prefix) {
            continue;
          }
          let props = get_node_props_tx(ray, handle, node_id);
          out.push(node_to_js(&env, node_id, Some(key), &node_type, props)?);
          seen.insert(node_id);
        }

        for (&node_id, delta) in &handle.tx.pending_created_nodes {
          if seen.contains(&node_id) {
            continue;
          }
          if handle.tx.pending_deleted_nodes.contains(&node_id) {
            continue;
          }
          let key = match &delta.key {
            Some(key) => key,
            None => continue,
          };
          if !key.starts_with(&prefix) {
            continue;
          }
          let props = get_node_props_tx(ray, handle, node_id);
          out.push(node_to_js(
            &env,
            node_id,
            Some(key.clone()),
            &node_type,
            props,
          )?);
        }

        Ok(out)
      });
    }

    self.with_kite(|ray| {
      let node_def = ray
        .node_def(&node_type)
        .ok_or_else(|| Error::from_reason(format!("Unknown node type: {node_type}")))?;
      let prefix = node_def.key_prefix.clone();
      let mut out = Vec::new();
      for node_id in graph_list_nodes(ray.raw()) {
        let delta = ray.raw().delta.read();
        if let Some(key) = graph_get_node_key(ray.raw().snapshot.as_ref(), &delta, node_id) {
          if !key.starts_with(&prefix) {
            continue;
          }
          let props = get_node_props(ray, node_id);
          out.push(node_to_js(&env, node_id, Some(key), &node_type, props)?);
        }
      }
      Ok(out)
    })
  }

  /// Count nodes (optionally by type)
  #[napi]
  pub fn count_nodes(&self, node_type: Option<String>) -> Result<i64> {
    if self.tx_state.lock().is_some() {
      let node_type_clone = node_type.clone();
      return with_tx_handle(
        &self.inner,
        &self.tx_state,
        |ray, handle| match node_type_clone {
          Some(node_type) => {
            let node_def = ray
              .node_def(&node_type)
              .ok_or_else(|| Error::from_reason(format!("Unknown node type: {node_type}")))?;
            let prefix = node_def.key_prefix.clone();
            let mut count = 0i64;
            let mut seen = HashSet::new();

            for node_id in graph_list_nodes(handle.db) {
              if handle.tx.pending_deleted_nodes.contains(&node_id) {
                continue;
              }
              let key = match get_node_key_tx(handle, node_id) {
                Some(key) => key,
                None => continue,
              };
              if !key.starts_with(&prefix) {
                continue;
              }
              count += 1;
              seen.insert(node_id);
            }

            for (&node_id, delta) in &handle.tx.pending_created_nodes {
              if seen.contains(&node_id) {
                continue;
              }
              if handle.tx.pending_deleted_nodes.contains(&node_id) {
                continue;
              }
              let key = match &delta.key {
                Some(key) => key,
                None => continue,
              };
              if key.starts_with(&prefix) {
                count += 1;
              }
            }

            Ok(count)
          }
          None => Ok(crate::graph::nodes::count_nodes(handle) as i64),
        },
      );
    }
    self.with_kite(|ray| match node_type {
      Some(node_type) => ray
        .count_nodes_by_type(&node_type)
        .map(|v| v as i64)
        .map_err(|e| Error::from_reason(e.to_string())),
      None => Ok(ray.count_nodes() as i64),
    })
  }

  /// Count edges (optionally by type)
  #[napi]
  pub fn count_edges(&self, edge_type: Option<String>) -> Result<i64> {
    if self.tx_state.lock().is_some() {
      let edge_type_clone = edge_type.clone();
      return with_tx_handle(&self.inner, &self.tx_state, |ray, handle| {
        let etype_filter = if let Some(edge_type) = edge_type_clone {
          let edge_def = ray
            .edge_def(&edge_type)
            .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {edge_type}")))?;
          Some(
            edge_def
              .etype_id
              .ok_or_else(|| Error::from_reason("Edge type not initialized"))?,
          )
        } else {
          None
        };
        Ok(list_edges_with_tx(handle, etype_filter).len() as i64)
      });
    }

    self.with_kite(|ray| match edge_type {
      Some(edge_type) => ray
        .count_edges_by_type(&edge_type)
        .map(|v| v as i64)
        .map_err(|e| Error::from_reason(e.to_string())),
      None => Ok(ray.count_edges() as i64),
    })
  }

  /// List all edges (optionally by type)
  #[napi]
  pub fn all_edges(&self, edge_type: Option<String>) -> Result<Vec<JsFullEdge>> {
    if self.tx_state.lock().is_some() {
      let edge_type_clone = edge_type.clone();
      return with_tx_handle(&self.inner, &self.tx_state, |ray, handle| {
        let etype_filter = if let Some(ref edge_type) = edge_type_clone {
          let edge_def = ray
            .edge_def(edge_type)
            .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {edge_type}")))?;
          Some(
            edge_def
              .etype_id
              .ok_or_else(|| Error::from_reason("Edge type not initialized"))?,
          )
        } else {
          None
        };

        let edges = list_edges_with_tx(handle, etype_filter);
        Ok(
          edges
            .into_iter()
            .map(|edge| JsFullEdge {
              src: edge.src as i64,
              etype: edge.etype,
              dst: edge.dst as i64,
            })
            .collect(),
        )
      });
    }

    self.with_kite(|ray| {
      let options = if let Some(ref edge_type) = edge_type {
        let edge_def = ray
          .edge_def(edge_type)
          .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {edge_type}")))?;
        let etype_id = edge_def
          .etype_id
          .ok_or_else(|| Error::from_reason("Edge type not initialized"))?;
        ListEdgesOptions {
          etype: Some(etype_id),
        }
      } else {
        ListEdgesOptions::default()
      };

      let edges = graph_list_edges(ray.raw(), options);
      Ok(
        edges
          .into_iter()
          .map(|edge| JsFullEdge {
            src: edge.src as i64,
            etype: edge.etype,
            dst: edge.dst as i64,
          })
          .collect(),
      )
    })
  }

  /// Check if a path exists between two nodes
  #[napi]
  pub fn has_path(&self, source: i64, target: i64, edge_type: Option<String>) -> Result<bool> {
    self.with_kite_mut(|ray| {
      ray
        .has_path(source as NodeId, target as NodeId, edge_type.as_deref())
        .map_err(|e| Error::from_reason(e.to_string()))
    })
  }

  /// Get all nodes reachable within a maximum depth
  #[napi]
  pub fn reachable_from(
    &self,
    source: i64,
    max_depth: i64,
    edge_type: Option<String>,
  ) -> Result<Vec<i64>> {
    self.with_kite(|ray| {
      let nodes = ray
        .reachable_from(source as NodeId, max_depth as usize, edge_type.as_deref())
        .map_err(|e| Error::from_reason(e.to_string()))?;
      Ok(nodes.into_iter().map(|id| id as i64).collect())
    })
  }

  /// Get all node type names
  #[napi]
  pub fn node_types(&self) -> Result<Vec<String>> {
    self.with_kite(|ray| {
      Ok(
        ray
          .node_types()
          .into_iter()
          .map(|s| s.to_string())
          .collect(),
      )
    })
  }

  /// Get all edge type names
  #[napi]
  pub fn edge_types(&self) -> Result<Vec<String>> {
    self.with_kite(|ray| {
      Ok(
        ray
          .edge_types()
          .into_iter()
          .map(|s| s.to_string())
          .collect(),
      )
    })
  }

  /// Get database statistics
  #[napi]
  pub fn stats(&self) -> Result<DbStats> {
    self.with_kite(|ray| {
      let s = ray.stats();
      Ok(DbStats {
        snapshot_gen: s.snapshot_gen as i64,
        snapshot_nodes: s.snapshot_nodes as i64,
        snapshot_edges: s.snapshot_edges as i64,
        snapshot_max_node_id: s.snapshot_max_node_id as i64,
        delta_nodes_created: s.delta_nodes_created as i64,
        delta_nodes_deleted: s.delta_nodes_deleted as i64,
        delta_edges_added: s.delta_edges_added as i64,
        delta_edges_deleted: s.delta_edges_deleted as i64,
        wal_segment: s.wal_segment as i64,
        wal_bytes: s.wal_bytes as i64,
        recommend_compact: s.recommend_compact,
        mvcc_stats: s.mvcc_stats.map(|stats| MvccStats {
          active_transactions: stats.active_transactions as i64,
          min_active_ts: stats.min_active_ts as i64,
          versions_pruned: stats.versions_pruned as i64,
          gc_runs: stats.gc_runs as i64,
          last_gc_time: stats.last_gc_time as i64,
          committed_writes_size: stats.committed_writes_size as i64,
          committed_writes_pruned: stats.committed_writes_pruned as i64,
        }),
      })
    })
  }

  /// Get a human-readable description of the database
  #[napi]
  pub fn describe(&self) -> Result<String> {
    self.with_kite(|ray| Ok(ray.describe()))
  }

  /// Check database integrity
  #[napi]
  pub fn check(&self) -> Result<CheckResult> {
    self.with_kite(|ray| {
      let result = ray.check().map_err(|e| Error::from_reason(e.to_string()))?;
      Ok(CheckResult::from(result))
    })
  }

  /// Begin a transaction
  #[napi]
  pub fn begin(&self, read_only: Option<bool>) -> Result<i64> {
    let read_only = read_only.unwrap_or(false);
    let mut tx_guard = self.tx_state.lock();
    if tx_guard.is_some() {
      return Err(Error::from_reason("Transaction already active"));
    }

    let guard = self.inner.read();
    let ray = guard
      .as_ref()
      .ok_or_else(|| Error::from_reason("Kite is closed"))?;

    let handle = if read_only {
      begin_read_tx(ray.raw())
        .map_err(|e| Error::from_reason(format!("Failed to begin transaction: {e}")))?
    } else {
      begin_tx(ray.raw())
        .map_err(|e| Error::from_reason(format!("Failed to begin transaction: {e}")))?
    };

    let txid = handle.tx.txid as i64;
    *tx_guard = Some(handle.tx);
    Ok(txid)
  }

  /// Commit the current transaction
  #[napi]
  pub fn commit(&self) -> Result<()> {
    let mut tx_guard = self.tx_state.lock();
    let tx_state = tx_guard
      .take()
      .ok_or_else(|| Error::from_reason("No active transaction"))?;

    let guard = self.inner.write();
    let ray = guard
      .as_ref()
      .ok_or_else(|| Error::from_reason("Kite is closed"))?;
    let mut handle = TxHandle::new(ray.raw(), tx_state);
    commit(&mut handle).map_err(|e| Error::from_reason(format!("Failed to commit: {e}")))?;
    Ok(())
  }

  /// Rollback the current transaction
  #[napi]
  pub fn rollback(&self) -> Result<()> {
    let mut tx_guard = self.tx_state.lock();
    let tx_state = tx_guard
      .take()
      .ok_or_else(|| Error::from_reason("No active transaction"))?;

    let guard = self.inner.write();
    let ray = guard
      .as_ref()
      .ok_or_else(|| Error::from_reason("Kite is closed"))?;
    let mut handle = TxHandle::new(ray.raw(), tx_state);
    rollback(&mut handle).map_err(|e| Error::from_reason(format!("Failed to rollback: {e}")))?;
    Ok(())
  }

  /// Check if there's an active transaction
  #[napi]
  pub fn has_transaction(&self) -> Result<bool> {
    Ok(self.tx_state.lock().is_some())
  }

  /// Execute a batch of operations atomically
  #[napi]
  pub fn batch(&self, env: Env, ops: Vec<Object>) -> Result<Vec<Object>> {
    let mut rust_ops = Vec::with_capacity(ops.len());

    for op in ops {
      let op_name: Option<String> = op.get_named_property("op").ok();
      let op_name = match op_name {
        Some(name) => name,
        None => op.get_named_property("type")?,
      };

      match op_name.as_str() {
        "createNode" => {
          let node_type: String = op.get_named_property("nodeType")?;
          let key: Unknown = op.get_named_property("key")?;
          let props: Option<Object> = op.get_named_property("props")?;
          let spec = self.key_spec(&node_type)?.clone();
          let key_suffix = key_suffix_from_js(&env, &spec, key)?;
          let props_map = js_props_to_map(&env, props)?;
          rust_ops.push(BatchOp::CreateNode {
            node_type,
            key_suffix,
            props: props_map,
          });
        }
        "deleteNode" => {
          let node_id: i64 = op.get_named_property("nodeId")?;
          rust_ops.push(BatchOp::DeleteNode {
            node_id: node_id as NodeId,
          });
        }
        "link" => {
          let src: i64 = op.get_named_property("src")?;
          let dst: i64 = op.get_named_property("dst")?;
          let edge_type: String = op.get_named_property("edgeType")?;
          rust_ops.push(BatchOp::Link {
            src: src as NodeId,
            edge_type,
            dst: dst as NodeId,
          });
        }
        "unlink" => {
          let src: i64 = op.get_named_property("src")?;
          let dst: i64 = op.get_named_property("dst")?;
          let edge_type: String = op.get_named_property("edgeType")?;
          rust_ops.push(BatchOp::Unlink {
            src: src as NodeId,
            edge_type,
            dst: dst as NodeId,
          });
        }
        "setProp" => {
          let node_id: i64 = op.get_named_property("nodeId")?;
          let prop_name: String = op.get_named_property("propName")?;
          let value: Unknown = op.get_named_property("value")?;
          let prop_value = js_value_to_prop_value(&env, value)?;
          rust_ops.push(BatchOp::SetProp {
            node_id: node_id as NodeId,
            prop_name,
            value: prop_value,
          });
        }
        "delProp" => {
          let node_id: i64 = op.get_named_property("nodeId")?;
          let prop_name: String = op.get_named_property("propName")?;
          rust_ops.push(BatchOp::DelProp {
            node_id: node_id as NodeId,
            prop_name,
          });
        }
        other => {
          return Err(Error::from_reason(format!("Unknown batch op: {other}")));
        }
      }
    }

    let results = if self.tx_state.lock().is_some() {
      with_tx_handle_mut(&self.inner, &self.tx_state, |ray, handle| {
        execute_batch_ops(ray, handle, rust_ops)
      })?
    } else {
      self.with_kite_mut(|ray| {
        ray
          .batch(rust_ops)
          .map_err(|e| Error::from_reason(e.to_string()))
      })?
    };

    let mut out = Vec::with_capacity(results.len());
    for result in results {
      out.push(batch_result_to_js(&env, result)?);
    }
    Ok(out)
  }

  /// Begin a traversal from a node ID
  #[napi]
  pub fn from(&self, node_id: i64) -> Result<KiteTraversal> {
    Ok(KiteTraversal {
      ray: self.inner.clone(),
      builder: TraversalBuilder::new(vec![node_id as NodeId]),
      where_edge: None,
      where_node: None,
    })
  }

  /// Begin a traversal from multiple nodes
  #[napi]
  pub fn from_nodes(&self, node_ids: Vec<i64>) -> Result<KiteTraversal> {
    Ok(KiteTraversal {
      ray: self.inner.clone(),
      builder: TraversalBuilder::new(node_ids.into_iter().map(|id| id as NodeId).collect()),
      where_edge: None,
      where_node: None,
    })
  }

  /// Begin a path finding query
  #[napi]
  pub fn path(&self, source: i64, target: i64) -> Result<KitePath> {
    Ok(KitePath::new(
      self.inner.clone(),
      source as NodeId,
      vec![target as NodeId],
    ))
  }

  /// Begin a path finding query to multiple targets
  #[napi]
  pub fn path_to_any(&self, source: i64, targets: Vec<i64>) -> Result<KitePath> {
    Ok(KitePath::new(
      self.inner.clone(),
      source as NodeId,
      targets.into_iter().map(|id| id as NodeId).collect(),
    ))
  }
}

/// Kite entrypoint - sync version
#[napi]
pub fn kite_sync(path: String, options: JsKiteOptions) -> Result<Kite> {
  Kite::open(path, options)
}

// =============================================================================
// Async Kite Open Task
// =============================================================================

/// Task for opening Kite database asynchronously
pub struct OpenKiteTask {
  path: String,
  options: JsKiteOptions,
  // Store result here to avoid public type in trait
  result: Option<(RustKite, HashMap<String, KeySpec>)>,
}

impl napi::Task for OpenKiteTask {
  type Output = ();
  type JsValue = Kite;

  fn compute(&mut self) -> Result<Self::Output> {
    let mut node_specs: HashMap<String, KeySpec> = HashMap::new();
    let mut ray_opts = KiteOptions::new();
    ray_opts.read_only = self.options.read_only.unwrap_or(false);
    ray_opts.create_if_missing = self.options.create_if_missing.unwrap_or(true);
    ray_opts.lock_file = self.options.lock_file.unwrap_or(true);

    for node in &self.options.nodes {
      let key_spec = parse_key_spec(&node.name, node.key.clone())?;
      let prefix = key_spec.prefix().to_string();

      let mut node_def = NodeDef::new(&node.name, &prefix);
      if let Some(props) = node.props.as_ref() {
        for (prop_name, prop_spec) in props {
          node_def = node_def.prop(prop_spec_to_def(prop_name, prop_spec)?);
        }
      }

      node_specs.insert(node.name.clone(), key_spec);
      ray_opts.nodes.push(node_def);
    }

    for edge in &self.options.edges {
      let mut edge_def = EdgeDef::new(&edge.name);
      if let Some(props) = edge.props.as_ref() {
        for (prop_name, prop_spec) in props {
          edge_def = edge_def.prop(prop_spec_to_def(prop_name, prop_spec)?);
        }
      }
      ray_opts.edges.push(edge_def);
    }

    let ray =
      RustKite::open(&self.path, ray_opts).map_err(|e| Error::from_reason(e.to_string()))?;
    self.result = Some((ray, node_specs));
    Ok(())
  }

  fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
    let (ray, node_specs) = self
      .result
      .take()
      .ok_or_else(|| Error::from_reason("Task result not available"))?;
    Ok(Kite {
      inner: Arc::new(RwLock::new(Some(ray))),
      node_specs: Arc::new(node_specs),
      tx_state: Arc::new(Mutex::new(None)),
    })
  }
}

/// Kite entrypoint - async version (recommended)
/// Opens the database on a background thread to avoid blocking the event loop
#[napi]
pub fn kite(path: String, options: JsKiteOptions) -> AsyncTask<OpenKiteTask> {
  AsyncTask::new(OpenKiteTask {
    path,
    options,
    result: None,
  })
}
