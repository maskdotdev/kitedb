//! Builder pattern implementations for Kite operations
//!
//! Contains insert, upsert, and update builders for nodes and edges.

#![allow(clippy::type_complexity)]

use napi::bindgen_prelude::*;
use napi_derive::napi;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;

use crate::api::kite::Kite as RustKite;
use crate::graph::edges::{
  del_edge_prop as graph_del_edge_prop, set_edge_prop, upsert_edge_with_props,
};
use crate::graph::nodes::{
  del_node_prop as graph_del_node_prop, set_node_prop as graph_set_node_prop,
  upsert_node_by_id_with_props, NodeOpts,
};
use crate::graph::tx::{begin_tx, commit, rollback};
use crate::types::{ETypeId, NodeId, PropValue, TxState};

use super::conversion::{js_props_to_map, js_value_to_prop_value, key_suffix_from_js};
use super::helpers::{get_node_props, get_node_props_tx, node_to_js, with_tx_handle_mut};
use super::key_spec::KeySpec;

// =============================================================================
// Insert Builder
// =============================================================================

/// Builder for inserting new nodes
#[napi]
pub struct KiteInsertBuilder {
  pub(crate) ray: Arc<RwLock<Option<RustKite>>>,
  pub(crate) tx_state: Arc<Mutex<Option<TxState>>>,
  pub(crate) node_type: String,
  pub(crate) key_prefix: String,
  pub(crate) key_spec: KeySpec,
}

impl KiteInsertBuilder {
  pub(crate) fn new(
    ray: Arc<RwLock<Option<RustKite>>>,
    tx_state: Arc<Mutex<Option<TxState>>>,
    node_type: String,
    key_prefix: String,
    key_spec: KeySpec,
  ) -> Self {
    Self {
      ray,
      tx_state,
      node_type,
      key_prefix,
      key_spec,
    }
  }
}

#[napi]
impl KiteInsertBuilder {
  /// Specify values for a single insert
  #[napi]
  pub fn values(
    &self,
    env: Env,
    key: Unknown,
    props: Option<Object>,
  ) -> Result<KiteInsertExecutorSingle> {
    let key_suffix = key_suffix_from_js(&env, &self.key_spec, key)?;
    let full_key = format!("{}{}", self.key_prefix, key_suffix);
    let props_map = js_props_to_map(&env, props)?;
    Ok(KiteInsertExecutorSingle {
      ray: self.ray.clone(),
      tx_state: self.tx_state.clone(),
      node_type: self.node_type.clone(),
      full_key,
      props: props_map,
    })
  }

  /// Specify values for multiple inserts
  #[napi]
  pub fn values_many(&self, env: Env, entries: Vec<Unknown>) -> Result<KiteInsertExecutorMany> {
    let mut items = Vec::with_capacity(entries.len());
    for entry in entries {
      let obj = entry.coerce_to_object()?;
      let key: Unknown = obj.get_named_property("key")?;
      let props: Option<Object> = obj.get_named_property("props")?;
      let key_suffix = key_suffix_from_js(&env, &self.key_spec, key)?;
      let full_key = format!("{}{}", self.key_prefix, key_suffix);
      let props_map = js_props_to_map(&env, props)?;
      items.push((full_key, props_map));
    }
    Ok(KiteInsertExecutorMany {
      ray: self.ray.clone(),
      tx_state: self.tx_state.clone(),
      node_type: self.node_type.clone(),
      entries: items,
    })
  }
}

/// Executor for a single insert operation
#[napi]
pub struct KiteInsertExecutorSingle {
  ray: Arc<RwLock<Option<RustKite>>>,
  tx_state: Arc<Mutex<Option<TxState>>>,
  node_type: String,
  full_key: String,
  props: HashMap<String, PropValue>,
}

#[napi]
impl KiteInsertExecutorSingle {
  /// Execute the insert without returning
  #[napi]
  pub fn execute(&self) -> Result<()> {
    insert_single(
      &self.ray,
      &self.tx_state,
      &self.node_type,
      &self.full_key,
      &self.props,
    )
    .map(|_| ())
  }

  /// Execute the insert and return the node
  #[napi]
  pub fn returning(&self, env: Env) -> Result<Object> {
    let node_ref = insert_single(
      &self.ray,
      &self.tx_state,
      &self.node_type,
      &self.full_key,
      &self.props,
    )?;
    let props = node_ref.1.unwrap_or_default();
    node_to_js(
      &env,
      node_ref.0,
      Some(self.full_key.clone()),
      &self.node_type,
      props,
    )
  }
}

/// Executor for multiple insert operations
#[napi]
pub struct KiteInsertExecutorMany {
  ray: Arc<RwLock<Option<RustKite>>>,
  tx_state: Arc<Mutex<Option<TxState>>>,
  node_type: String,
  entries: Vec<(String, HashMap<String, PropValue>)>,
}

#[napi]
impl KiteInsertExecutorMany {
  /// Execute the inserts without returning
  #[napi]
  pub fn execute(&self) -> Result<()> {
    let _ = insert_many(
      &self.ray,
      &self.tx_state,
      &self.node_type,
      &self.entries,
      false,
    )?;
    Ok(())
  }

  /// Execute the inserts and return nodes
  #[napi]
  pub fn returning(&self, env: Env) -> Result<Vec<Object>> {
    let results = insert_many(
      &self.ray,
      &self.tx_state,
      &self.node_type,
      &self.entries,
      true,
    )?;
    let mut out = Vec::with_capacity(results.len());
    for ((full_key, _), (node_id, props)) in self.entries.iter().zip(results.into_iter()) {
      let props = props.expect("props loaded");
      out.push(node_to_js(
        &env,
        node_id,
        Some(full_key.clone()),
        &self.node_type,
        props,
      )?);
    }
    Ok(out)
  }
}

fn insert_single(
  ray: &Arc<RwLock<Option<RustKite>>>,
  tx_state: &Arc<Mutex<Option<TxState>>>,
  node_type: &str,
  full_key: &str,
  props: &HashMap<String, PropValue>,
) -> Result<(NodeId, Option<HashMap<String, PropValue>>)> {
  if tx_state.lock().is_some() {
    return with_tx_handle_mut(ray, tx_state, |ray, handle| {
      let node_def = ray
        .node_def(node_type)
        .ok_or_else(|| Error::from_reason(format!("Unknown node type: {node_type}")))?
        .clone();

      let node_id = crate::graph::nodes::create_node(
        handle,
        crate::graph::nodes::NodeOpts::new().with_key(full_key),
      )
      .map_err(|e| Error::from_reason(format!("Failed to create node: {e}")))?;

      for (prop_name, value) in props {
        let prop_key_id = if let Some(&id) = node_def.prop_key_ids.get(prop_name) {
          id
        } else {
          handle.db.get_or_create_propkey(prop_name)
        };
        crate::graph::nodes::set_node_prop(handle, node_id, prop_key_id, value.clone())
          .map_err(|e| Error::from_reason(format!("Failed to set prop: {e}")))?;
      }

      Ok((node_id, Some(props.clone())))
    });
  }

  let mut guard = ray.write();
  let ray = guard
    .as_mut()
    .ok_or_else(|| Error::from_reason("Kite is closed"))?;
  let node_def = ray
    .node_def(node_type)
    .ok_or_else(|| Error::from_reason(format!("Unknown node type: {node_type}")))?
    .clone();

  let mut handle =
    begin_tx(ray.raw()).map_err(|e| Error::from_reason(format!("Failed to begin tx: {e}")))?;

  let node_id = match crate::graph::nodes::create_node(
    &mut handle,
    crate::graph::nodes::NodeOpts::new().with_key(full_key),
  ) {
    Ok(id) => id,
    Err(e) => {
      let _ = rollback(&mut handle);
      return Err(Error::from_reason(format!("Failed to create node: {e}")));
    }
  };

  for (prop_name, value) in props {
    let prop_key_id = if let Some(&id) = node_def.prop_key_ids.get(prop_name) {
      id
    } else {
      handle.db.get_or_create_propkey(prop_name)
    };
    if let Err(e) =
      crate::graph::nodes::set_node_prop(&mut handle, node_id, prop_key_id, value.clone())
    {
      let _ = rollback(&mut handle);
      return Err(Error::from_reason(format!("Failed to set prop: {e}")));
    }
  }

  if let Err(e) = commit(&mut handle) {
    return Err(Error::from_reason(format!("Failed to commit: {e}")));
  }

  Ok((node_id, Some(props.clone())))
}

fn insert_many(
  ray: &Arc<RwLock<Option<RustKite>>>,
  tx_state: &Arc<Mutex<Option<TxState>>>,
  node_type: &str,
  entries: &[(String, HashMap<String, PropValue>)],
  load_props: bool,
) -> Result<Vec<(NodeId, Option<HashMap<String, PropValue>>)>> {
  if entries.is_empty() {
    return Ok(Vec::new());
  }

  if tx_state.lock().is_some() {
    return with_tx_handle_mut(ray, tx_state, |ray, handle| {
      let node_def = ray
        .node_def(node_type)
        .ok_or_else(|| Error::from_reason(format!("Unknown node type: {node_type}")))?
        .clone();

      let mut results = Vec::with_capacity(entries.len());
      for (full_key, props) in entries {
        let node_id = crate::graph::nodes::create_node(
          handle,
          crate::graph::nodes::NodeOpts::new().with_key(full_key),
        )
        .map_err(|e| Error::from_reason(format!("Failed to create node: {e}")))?;

        for (prop_name, value) in props {
          let prop_key_id = if let Some(&id) = node_def.prop_key_ids.get(prop_name) {
            id
          } else {
            handle.db.get_or_create_propkey(prop_name)
          };
          crate::graph::nodes::set_node_prop(handle, node_id, prop_key_id, value.clone())
            .map_err(|e| Error::from_reason(format!("Failed to set prop: {e}")))?;
        }

        let props = if load_props {
          Some(props.clone())
        } else {
          None
        };
        results.push((node_id, props));
      }

      Ok(results)
    });
  }

  let mut guard = ray.write();
  let ray = guard
    .as_mut()
    .ok_or_else(|| Error::from_reason("Kite is closed"))?;
  let node_def = ray
    .node_def(node_type)
    .ok_or_else(|| Error::from_reason(format!("Unknown node type: {node_type}")))?
    .clone();

  let mut handle =
    begin_tx(ray.raw()).map_err(|e| Error::from_reason(format!("Failed to begin tx: {e}")))?;

  let mut results = Vec::with_capacity(entries.len());
  for (full_key, props) in entries {
    let node_id = match crate::graph::nodes::create_node(
      &mut handle,
      crate::graph::nodes::NodeOpts::new().with_key(full_key),
    ) {
      Ok(id) => id,
      Err(e) => {
        let _ = rollback(&mut handle);
        return Err(Error::from_reason(format!("Failed to create node: {e}")));
      }
    };

    for (prop_name, value) in props {
      let prop_key_id = if let Some(&id) = node_def.prop_key_ids.get(prop_name) {
        id
      } else {
        handle.db.get_or_create_propkey(prop_name)
      };
      if let Err(e) =
        crate::graph::nodes::set_node_prop(&mut handle, node_id, prop_key_id, value.clone())
      {
        let _ = rollback(&mut handle);
        return Err(Error::from_reason(format!("Failed to set prop: {e}")));
      }
    }

    let props = if load_props {
      Some(props.clone())
    } else {
      None
    };
    results.push((node_id, props));
  }

  if let Err(e) = commit(&mut handle) {
    return Err(Error::from_reason(format!("Failed to commit: {e}")));
  }

  Ok(results)
}

// =============================================================================
// Upsert Builder
// =============================================================================

/// Builder for upserting nodes (insert or update)
#[napi]
pub struct KiteUpsertBuilder {
  pub(crate) ray: Arc<RwLock<Option<RustKite>>>,
  pub(crate) tx_state: Arc<Mutex<Option<TxState>>>,
  pub(crate) node_type: String,
  pub(crate) key_prefix: String,
  pub(crate) key_spec: KeySpec,
}

impl KiteUpsertBuilder {
  pub(crate) fn new(
    ray: Arc<RwLock<Option<RustKite>>>,
    tx_state: Arc<Mutex<Option<TxState>>>,
    node_type: String,
    key_prefix: String,
    key_spec: KeySpec,
  ) -> Self {
    Self {
      ray,
      tx_state,
      node_type,
      key_prefix,
      key_spec,
    }
  }
}

#[napi]
impl KiteUpsertBuilder {
  /// Specify values for a single upsert
  #[napi]
  pub fn values(
    &self,
    env: Env,
    key: Unknown,
    props: Option<Object>,
  ) -> Result<KiteUpsertExecutorSingle> {
    let key_suffix = key_suffix_from_js(&env, &self.key_spec, key)?;
    let full_key = format!("{}{}", self.key_prefix, key_suffix);
    let props_map = js_props_to_map(&env, props)?;
    Ok(KiteUpsertExecutorSingle {
      ray: self.ray.clone(),
      tx_state: self.tx_state.clone(),
      node_type: self.node_type.clone(),
      full_key,
      props: props_map,
    })
  }

  /// Specify values for multiple upserts
  #[napi]
  pub fn values_many(&self, env: Env, entries: Vec<Unknown>) -> Result<KiteUpsertExecutorMany> {
    let mut items = Vec::with_capacity(entries.len());
    for entry in entries {
      let obj = entry.coerce_to_object()?;
      let key: Unknown = obj.get_named_property("key")?;
      let props: Option<Object> = obj.get_named_property("props")?;
      let key_suffix = key_suffix_from_js(&env, &self.key_spec, key)?;
      let full_key = format!("{}{}", self.key_prefix, key_suffix);
      let props_map = js_props_to_map(&env, props)?;
      items.push((full_key, props_map));
    }
    Ok(KiteUpsertExecutorMany {
      ray: self.ray.clone(),
      tx_state: self.tx_state.clone(),
      node_type: self.node_type.clone(),
      entries: items,
    })
  }
}

/// Executor for a single upsert operation
#[napi]
pub struct KiteUpsertExecutorSingle {
  ray: Arc<RwLock<Option<RustKite>>>,
  tx_state: Arc<Mutex<Option<TxState>>>,
  node_type: String,
  full_key: String,
  props: HashMap<String, PropValue>,
}

#[napi]
impl KiteUpsertExecutorSingle {
  /// Execute the upsert without returning
  #[napi]
  pub fn execute(&self) -> Result<()> {
    upsert_single(
      &self.ray,
      &self.tx_state,
      &self.node_type,
      &self.full_key,
      &self.props,
    )
    .map(|_| ())
  }

  /// Execute the upsert and return the node
  #[napi]
  pub fn returning(&self, env: Env) -> Result<Object> {
    let (node_id, props) = upsert_single(
      &self.ray,
      &self.tx_state,
      &self.node_type,
      &self.full_key,
      &self.props,
    )?;
    node_to_js(
      &env,
      node_id,
      Some(self.full_key.clone()),
      &self.node_type,
      props,
    )
  }
}

/// Executor for multiple upsert operations
#[napi]
pub struct KiteUpsertExecutorMany {
  ray: Arc<RwLock<Option<RustKite>>>,
  tx_state: Arc<Mutex<Option<TxState>>>,
  node_type: String,
  entries: Vec<(String, HashMap<String, PropValue>)>,
}

#[napi]
impl KiteUpsertExecutorMany {
  /// Execute the upserts without returning
  #[napi]
  pub fn execute(&self) -> Result<()> {
    let _ = upsert_many(
      &self.ray,
      &self.tx_state,
      &self.node_type,
      &self.entries,
      false,
    )?;
    Ok(())
  }

  /// Execute the upserts and return nodes
  #[napi]
  pub fn returning(&self, env: Env) -> Result<Vec<Object>> {
    let results = upsert_many(
      &self.ray,
      &self.tx_state,
      &self.node_type,
      &self.entries,
      true,
    )?;
    let mut out = Vec::with_capacity(results.len());
    for ((full_key, _), (node_id, props)) in self.entries.iter().zip(results.into_iter()) {
      let props = props.expect("props loaded");
      out.push(node_to_js(
        &env,
        node_id,
        Some(full_key.clone()),
        &self.node_type,
        props,
      )?);
    }
    Ok(out)
  }
}

fn upsert_single(
  ray: &Arc<RwLock<Option<RustKite>>>,
  tx_state: &Arc<Mutex<Option<TxState>>>,
  node_type: &str,
  full_key: &str,
  props: &HashMap<String, PropValue>,
) -> Result<(NodeId, HashMap<String, PropValue>)> {
  if tx_state.lock().is_some() {
    return with_tx_handle_mut(ray, tx_state, |ray, handle| {
      let node_def = ray
        .node_def(node_type)
        .ok_or_else(|| Error::from_reason(format!("Unknown node type: {node_type}")))?
        .clone();

      let mut updates = Vec::with_capacity(props.len());
      for (prop_name, value) in props {
        let prop_key_id = if let Some(&id) = node_def.prop_key_ids.get(prop_name) {
          id
        } else {
          handle.db.get_or_create_propkey(prop_name)
        };
        let value_opt = match value {
          PropValue::Null => None,
          other => Some(other.clone()),
        };
        updates.push((prop_key_id, value_opt));
      }

      let (node_id, _) = crate::graph::nodes::upsert_node_with_props(handle, full_key, updates)
        .map_err(|e| Error::from_reason(format!("Failed to upsert node: {e}")))?;

      let props = get_node_props_tx(ray, handle, node_id);
      Ok((node_id, props))
    });
  }

  let mut guard = ray.write();
  let ray = guard
    .as_mut()
    .ok_or_else(|| Error::from_reason("Kite is closed"))?;
  let node_def = ray
    .node_def(node_type)
    .ok_or_else(|| Error::from_reason(format!("Unknown node type: {node_type}")))?
    .clone();

  let mut handle =
    begin_tx(ray.raw()).map_err(|e| Error::from_reason(format!("Failed to begin tx: {e}")))?;

  let mut updates = Vec::with_capacity(props.len());
  for (prop_name, value) in props {
    let prop_key_id = if let Some(&id) = node_def.prop_key_ids.get(prop_name) {
      id
    } else {
      handle.db.get_or_create_propkey(prop_name)
    };
    let value_opt = match value {
      PropValue::Null => None,
      other => Some(other.clone()),
    };
    updates.push((prop_key_id, value_opt));
  }

  let (node_id, _) =
    match crate::graph::nodes::upsert_node_with_props(&mut handle, full_key, updates) {
      Ok(result) => result,
      Err(e) => {
        let _ = rollback(&mut handle);
        return Err(Error::from_reason(format!("Failed to upsert node: {e}")));
      }
    };

  if let Err(e) = commit(&mut handle) {
    return Err(Error::from_reason(format!("Failed to commit: {e}")));
  }

  let props = get_node_props(ray, node_id);
  Ok((node_id, props))
}

fn upsert_many(
  ray: &Arc<RwLock<Option<RustKite>>>,
  tx_state: &Arc<Mutex<Option<TxState>>>,
  node_type: &str,
  entries: &[(String, HashMap<String, PropValue>)],
  load_props: bool,
) -> Result<Vec<(NodeId, Option<HashMap<String, PropValue>>)>> {
  if entries.is_empty() {
    return Ok(Vec::new());
  }

  if tx_state.lock().is_some() {
    return with_tx_handle_mut(ray, tx_state, |ray, handle| {
      let node_def = ray
        .node_def(node_type)
        .ok_or_else(|| Error::from_reason(format!("Unknown node type: {node_type}")))?
        .clone();

      let mut node_ids = Vec::with_capacity(entries.len());
      for (full_key, props) in entries {
        let mut updates = Vec::with_capacity(props.len());
        for (prop_name, value) in props {
          let prop_key_id = if let Some(&id) = node_def.prop_key_ids.get(prop_name) {
            id
          } else {
            handle.db.get_or_create_propkey(prop_name)
          };
          let value_opt = match value {
            PropValue::Null => None,
            other => Some(other.clone()),
          };
          updates.push((prop_key_id, value_opt));
        }

        let (node_id, _) =
          crate::graph::nodes::upsert_node_with_props(handle, full_key, updates)
            .map_err(|e| Error::from_reason(format!("Failed to upsert node: {e}")))?;
        node_ids.push(node_id);
      }

      let mut results = Vec::with_capacity(node_ids.len());
      for node_id in node_ids {
        let props = if load_props {
          Some(get_node_props_tx(ray, handle, node_id))
        } else {
          None
        };
        results.push((node_id, props));
      }
      Ok(results)
    });
  }

  let mut guard = ray.write();
  let ray = guard
    .as_mut()
    .ok_or_else(|| Error::from_reason("Kite is closed"))?;
  let node_def = ray
    .node_def(node_type)
    .ok_or_else(|| Error::from_reason(format!("Unknown node type: {node_type}")))?
    .clone();

  let mut handle =
    begin_tx(ray.raw()).map_err(|e| Error::from_reason(format!("Failed to begin tx: {e}")))?;

  let mut node_ids = Vec::with_capacity(entries.len());
  for (full_key, props) in entries {
    let mut updates = Vec::with_capacity(props.len());
    for (prop_name, value) in props {
      let prop_key_id = if let Some(&id) = node_def.prop_key_ids.get(prop_name) {
        id
      } else {
        handle.db.get_or_create_propkey(prop_name)
      };
      let value_opt = match value {
        PropValue::Null => None,
        other => Some(other.clone()),
      };
      updates.push((prop_key_id, value_opt));
    }

    let (node_id, _) =
      match crate::graph::nodes::upsert_node_with_props(&mut handle, full_key, updates) {
        Ok(result) => result,
        Err(e) => {
          let _ = rollback(&mut handle);
          return Err(Error::from_reason(format!("Failed to upsert node: {e}")));
        }
      };
    node_ids.push(node_id);
  }

  if let Err(e) = commit(&mut handle) {
    let _ = rollback(&mut handle);
    return Err(Error::from_reason(format!("Failed to commit: {e}")));
  }

  let mut results = Vec::with_capacity(node_ids.len());
  for node_id in node_ids {
    let props = if load_props {
      Some(get_node_props(ray, node_id))
    } else {
      None
    };
    results.push((node_id, props));
  }

  Ok(results)
}

// =============================================================================
// Update Builder
// =============================================================================

/// Builder for updating node properties
#[napi]
pub struct KiteUpdateBuilder {
  pub(crate) ray: Arc<RwLock<Option<RustKite>>>,
  pub(crate) tx_state: Arc<Mutex<Option<TxState>>>,
  pub(crate) node_id: NodeId,
  pub(crate) updates: HashMap<String, Option<PropValue>>,
}

impl KiteUpdateBuilder {
  pub(crate) fn new(
    ray: Arc<RwLock<Option<RustKite>>>,
    tx_state: Arc<Mutex<Option<TxState>>>,
    node_id: NodeId,
  ) -> Self {
    Self {
      ray,
      tx_state,
      node_id,
      updates: HashMap::new(),
    }
  }
}

#[napi]
impl KiteUpdateBuilder {
  /// Set a node property
  #[napi]
  pub fn set(&mut self, env: Env, prop_name: String, value: Unknown) -> Result<()> {
    let prop_value = js_value_to_prop_value(&env, value)?;
    self.updates.insert(prop_name, Some(prop_value));
    Ok(())
  }

  /// Remove a node property
  #[napi]
  pub fn unset(&mut self, prop_name: String) -> Result<()> {
    self.updates.insert(prop_name, None);
    Ok(())
  }

  /// Set multiple properties at once
  #[napi]
  pub fn set_all(&mut self, env: Env, props: Object) -> Result<()> {
    let props_map = js_props_to_map(&env, Some(props))?;
    for (prop_name, value) in props_map {
      self.updates.insert(prop_name, Some(value));
    }
    Ok(())
  }

  /// Execute the update
  #[napi]
  pub fn execute(&self) -> Result<()> {
    if self.updates.is_empty() {
      return Ok(());
    }

    if self.tx_state.lock().is_some() {
      return with_tx_handle_mut(&self.ray, &self.tx_state, |ray, handle| {
        for (prop_name, value_opt) in &self.updates {
          let prop_key_id = ray.raw().get_or_create_propkey(prop_name);
          let result = match value_opt {
            Some(value) => graph_set_node_prop(handle, self.node_id, prop_key_id, value.clone()),
            None => graph_del_node_prop(handle, self.node_id, prop_key_id),
          };

          if let Err(e) = result {
            return Err(Error::from_reason(format!("Failed to update prop: {e}")));
          }
        }
        Ok(())
      });
    }

    let mut guard = self.ray.write();
    let ray = guard
      .as_mut()
      .ok_or_else(|| Error::from_reason("Kite is closed"))?;

    let mut handle =
      begin_tx(ray.raw()).map_err(|e| Error::from_reason(format!("Failed to begin tx: {e}")))?;

    for (prop_name, value_opt) in &self.updates {
      let prop_key_id = ray.raw().get_or_create_propkey(prop_name);
      let result = match value_opt {
        Some(value) => {
          crate::graph::nodes::set_node_prop(&mut handle, self.node_id, prop_key_id, value.clone())
        }
        None => crate::graph::nodes::del_node_prop(&mut handle, self.node_id, prop_key_id),
      };

      if let Err(e) = result {
        let _ = rollback(&mut handle);
        return Err(Error::from_reason(format!("Failed to update prop: {e}")));
      }
    }

    commit(&mut handle).map_err(|e| Error::from_reason(format!("Failed to commit: {e}")))?;
    Ok(())
  }
}

// =============================================================================
// Upsert By ID Builder
// =============================================================================

/// Builder for upserting a node by ID
#[napi]
pub struct KiteUpsertByIdBuilder {
  pub(crate) ray: Arc<RwLock<Option<RustKite>>>,
  pub(crate) tx_state: Arc<Mutex<Option<TxState>>>,
  pub(crate) node_type: String,
  pub(crate) node_id: NodeId,
  pub(crate) updates: HashMap<String, Option<PropValue>>,
}

impl KiteUpsertByIdBuilder {
  pub(crate) fn new(
    ray: Arc<RwLock<Option<RustKite>>>,
    tx_state: Arc<Mutex<Option<TxState>>>,
    node_type: String,
    node_id: NodeId,
  ) -> Self {
    Self {
      ray,
      tx_state,
      node_type,
      node_id,
      updates: HashMap::new(),
    }
  }
}

#[napi]
impl KiteUpsertByIdBuilder {
  /// Set a node property
  #[napi]
  pub fn set(&mut self, env: Env, prop_name: String, value: Unknown) -> Result<()> {
    let prop_value = js_value_to_prop_value(&env, value)?;
    self.updates.insert(prop_name, Some(prop_value));
    Ok(())
  }

  /// Remove a node property
  #[napi]
  pub fn unset(&mut self, prop_name: String) -> Result<()> {
    self.updates.insert(prop_name, None);
    Ok(())
  }

  /// Set multiple properties at once
  #[napi]
  pub fn set_all(&mut self, env: Env, props: Object) -> Result<()> {
    let props_map = js_props_to_map(&env, Some(props))?;
    for (prop_name, value) in props_map {
      self.updates.insert(prop_name, Some(value));
    }
    Ok(())
  }

  /// Execute the upsert
  #[napi]
  pub fn execute(&self) -> Result<()> {
    if self.tx_state.lock().is_some() {
      return with_tx_handle_mut(&self.ray, &self.tx_state, |ray, handle| {
        let node_def = ray
          .node_def(&self.node_type)
          .ok_or_else(|| Error::from_reason(format!("Unknown node type: {}", self.node_type)))?
          .clone();

        let mut updates = Vec::with_capacity(self.updates.len());
        for (prop_name, value_opt) in &self.updates {
          let prop_key_id = if let Some(&id) = node_def.prop_key_ids.get(prop_name) {
            id
          } else {
            ray.raw().get_or_create_propkey(prop_name)
          };
          updates.push((prop_key_id, value_opt.clone()));
        }

        let opts = NodeOpts {
          key: None,
          labels: node_def.label_id.map(|id| vec![id]),
          props: None,
        };

        upsert_node_by_id_with_props(handle, self.node_id, opts, updates)
          .map_err(|e| Error::from_reason(format!("Failed to upsert node: {e}")))?;

        Ok(())
      });
    }

    let mut guard = self.ray.write();
    let ray = guard
      .as_mut()
      .ok_or_else(|| Error::from_reason("Kite is closed"))?;

    let node_def = ray
      .node_def(&self.node_type)
      .ok_or_else(|| Error::from_reason(format!("Unknown node type: {}", self.node_type)))?
      .clone();

    let mut handle =
      begin_tx(ray.raw()).map_err(|e| Error::from_reason(format!("Failed to begin tx: {e}")))?;

    let mut updates = Vec::with_capacity(self.updates.len());
    for (prop_name, value_opt) in &self.updates {
      let prop_key_id = if let Some(&id) = node_def.prop_key_ids.get(prop_name) {
        id
      } else {
        ray.raw().get_or_create_propkey(prop_name)
      };
      updates.push((prop_key_id, value_opt.clone()));
    }

    let opts = NodeOpts {
      key: None,
      labels: node_def.label_id.map(|id| vec![id]),
      props: None,
    };

    if let Err(e) = upsert_node_by_id_with_props(&mut handle, self.node_id, opts, updates) {
      let _ = rollback(&mut handle);
      return Err(Error::from_reason(format!("Failed to upsert node: {e}")));
    }

    commit(&mut handle).map_err(|e| Error::from_reason(format!("Failed to commit: {e}")))?;
    Ok(())
  }
}

// =============================================================================
// Update Edge Builder
// =============================================================================

/// Builder for updating edge properties
#[napi]
pub struct KiteUpdateEdgeBuilder {
  pub(crate) ray: Arc<RwLock<Option<RustKite>>>,
  pub(crate) tx_state: Arc<Mutex<Option<TxState>>>,
  pub(crate) src: NodeId,
  pub(crate) etype_id: ETypeId,
  pub(crate) dst: NodeId,
  pub(crate) updates: HashMap<String, Option<PropValue>>,
}

impl KiteUpdateEdgeBuilder {
  pub(crate) fn new(
    ray: Arc<RwLock<Option<RustKite>>>,
    tx_state: Arc<Mutex<Option<TxState>>>,
    src: NodeId,
    etype_id: ETypeId,
    dst: NodeId,
  ) -> Self {
    Self {
      ray,
      tx_state,
      src,
      etype_id,
      dst,
      updates: HashMap::new(),
    }
  }
}

#[napi]
impl KiteUpdateEdgeBuilder {
  /// Set an edge property
  #[napi]
  pub fn set(&mut self, env: Env, prop_name: String, value: Unknown) -> Result<()> {
    let prop_value = js_value_to_prop_value(&env, value)?;
    self.updates.insert(prop_name, Some(prop_value));
    Ok(())
  }

  /// Remove an edge property
  #[napi]
  pub fn unset(&mut self, prop_name: String) -> Result<()> {
    self.updates.insert(prop_name, None);
    Ok(())
  }

  /// Set multiple edge properties at once
  #[napi]
  pub fn set_all(&mut self, env: Env, props: Object) -> Result<()> {
    let props_map = js_props_to_map(&env, Some(props))?;
    for (prop_name, value) in props_map {
      self.updates.insert(prop_name, Some(value));
    }
    Ok(())
  }

  /// Execute the edge update
  #[napi]
  pub fn execute(&self) -> Result<()> {
    if self.updates.is_empty() {
      return Ok(());
    }

    if self.tx_state.lock().is_some() {
      return with_tx_handle_mut(&self.ray, &self.tx_state, |ray, handle| {
        for (prop_name, value_opt) in &self.updates {
          let result = match value_opt {
            Some(value) => {
              let prop_key_id = ray.raw().get_or_create_propkey(prop_name);
              set_edge_prop(
                handle,
                self.src,
                self.etype_id,
                self.dst,
                prop_key_id,
                value.clone(),
              )
            }
            None => {
              if let Some(prop_key_id) = ray.raw().get_propkey_id(prop_name) {
                graph_del_edge_prop(handle, self.src, self.etype_id, self.dst, prop_key_id)
              } else {
                Ok(())
              }
            }
          };

          if let Err(e) = result {
            return Err(Error::from_reason(format!(
              "Failed to update edge prop: {e}"
            )));
          }
        }
        Ok(())
      });
    }

    let mut guard = self.ray.write();
    let ray = guard
      .as_mut()
      .ok_or_else(|| Error::from_reason("Kite is closed"))?;

    let mut handle =
      begin_tx(ray.raw()).map_err(|e| Error::from_reason(format!("Failed to begin tx: {e}")))?;

    for (prop_name, value_opt) in &self.updates {
      let result = match value_opt {
        Some(value) => {
          let prop_key_id = ray.raw().get_or_create_propkey(prop_name);
          set_edge_prop(
            &mut handle,
            self.src,
            self.etype_id,
            self.dst,
            prop_key_id,
            value.clone(),
          )
        }
        None => {
          if let Some(prop_key_id) = ray.raw().get_propkey_id(prop_name) {
            graph_del_edge_prop(&mut handle, self.src, self.etype_id, self.dst, prop_key_id)
          } else {
            Ok(())
          }
        }
      };

      if let Err(e) = result {
        let _ = rollback(&mut handle);
        return Err(Error::from_reason(format!(
          "Failed to update edge prop: {e}"
        )));
      }
    }

    commit(&mut handle).map_err(|e| Error::from_reason(format!("Failed to commit: {e}")))?;
    Ok(())
  }
}

// =============================================================================
// Upsert Edge Builder
// =============================================================================

/// Builder for upserting edges (create if not exists, update properties)
#[napi]
pub struct KiteUpsertEdgeBuilder {
  pub(crate) ray: Arc<RwLock<Option<RustKite>>>,
  pub(crate) tx_state: Arc<Mutex<Option<TxState>>>,
  pub(crate) src: NodeId,
  pub(crate) etype_id: ETypeId,
  pub(crate) dst: NodeId,
  pub(crate) updates: HashMap<String, Option<PropValue>>,
}

impl KiteUpsertEdgeBuilder {
  pub(crate) fn new(
    ray: Arc<RwLock<Option<RustKite>>>,
    tx_state: Arc<Mutex<Option<TxState>>>,
    src: NodeId,
    etype_id: ETypeId,
    dst: NodeId,
  ) -> Self {
    Self {
      ray,
      tx_state,
      src,
      etype_id,
      dst,
      updates: HashMap::new(),
    }
  }
}

#[napi]
impl KiteUpsertEdgeBuilder {
  /// Set an edge property
  #[napi]
  pub fn set(&mut self, env: Env, prop_name: String, value: Unknown) -> Result<()> {
    let prop_value = js_value_to_prop_value(&env, value)?;
    self.updates.insert(prop_name, Some(prop_value));
    Ok(())
  }

  /// Remove an edge property
  #[napi]
  pub fn unset(&mut self, prop_name: String) -> Result<()> {
    self.updates.insert(prop_name, None);
    Ok(())
  }

  /// Set multiple edge properties at once
  #[napi]
  pub fn set_all(&mut self, env: Env, props: Object) -> Result<()> {
    let props_map = js_props_to_map(&env, Some(props))?;
    for (prop_name, value) in props_map {
      self.updates.insert(prop_name, Some(value));
    }
    Ok(())
  }

  /// Execute the upsert
  #[napi]
  pub fn execute(&self) -> Result<()> {
    if self.tx_state.lock().is_some() {
      return with_tx_handle_mut(&self.ray, &self.tx_state, |ray, handle| {
        let mut updates = Vec::with_capacity(self.updates.len());
        for (prop_name, value_opt) in &self.updates {
          let prop_key_id = ray.raw().get_or_create_propkey(prop_name);
          updates.push((prop_key_id, value_opt.clone()));
        }

        upsert_edge_with_props(handle, self.src, self.etype_id, self.dst, updates)
          .map_err(|e| Error::from_reason(format!("Failed to upsert edge: {e}")))?;
        Ok(())
      });
    }

    let mut guard = self.ray.write();
    let ray = guard
      .as_mut()
      .ok_or_else(|| Error::from_reason("Kite is closed"))?;

    let mut handle =
      begin_tx(ray.raw()).map_err(|e| Error::from_reason(format!("Failed to begin tx: {e}")))?;

    let mut updates = Vec::with_capacity(self.updates.len());
    for (prop_name, value_opt) in &self.updates {
      let prop_key_id = ray.raw().get_or_create_propkey(prop_name);
      updates.push((prop_key_id, value_opt.clone()));
    }

    if let Err(e) = upsert_edge_with_props(&mut handle, self.src, self.etype_id, self.dst, updates)
    {
      let _ = rollback(&mut handle);
      return Err(Error::from_reason(format!("Failed to upsert edge: {e}")));
    }

    commit(&mut handle).map_err(|e| Error::from_reason(format!("Failed to commit: {e}")))?;
    Ok(())
  }
}
