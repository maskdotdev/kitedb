//! NAPI bindings for the high-level Ray API

use napi::bindgen_prelude::*;
use napi_derive::napi;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::api::pathfinding::{bfs, dijkstra, yen_k_shortest, PathConfig, PathResult};
use crate::api::ray::{
  EdgeDef, NodeDef, PropDef, PropType as RayPropType, Ray as RustRay, RayOptions,
};
use crate::api::traversal::{TraversalBuilder, TraversalDirection, TraverseOptions};
use crate::graph::edges::edge_exists_db;
use crate::graph::iterators::{
  list_edges as graph_list_edges, list_nodes as graph_list_nodes, ListEdgesOptions,
};
use crate::graph::key_index::get_node_key as graph_get_node_key;
use crate::graph::nodes::{get_node_by_key_db, get_node_props_db};
use crate::graph::tx::{begin_tx, commit, rollback};
use crate::types::{ETypeId, Edge, EdgePatch, NodeId, PropValue};

use super::database::{JsFullEdge, JsPropValue, PropType as DbPropType};
use super::traversal::{JsTraversalDirection, JsTraverseOptions};

// =============================================================================
// Schema Input Types
// =============================================================================

#[napi(object)]
pub struct JsPropSpec {
  pub r#type: String,
  pub optional: Option<bool>,
  pub r#default: Option<JsPropValue>,
}

#[napi(object)]
pub struct JsKeySpec {
  pub kind: String,
  pub prefix: Option<String>,
  pub template: Option<String>,
  pub fields: Option<Vec<String>>,
  pub separator: Option<String>,
}

#[napi(object)]
pub struct JsNodeSpec {
  pub name: String,
  pub key: Option<JsKeySpec>,
  pub props: Option<HashMap<String, JsPropSpec>>,
}

#[napi(object)]
pub struct JsEdgeSpec {
  pub name: String,
  pub props: Option<HashMap<String, JsPropSpec>>,
}

#[napi(object)]
pub struct JsRayOptions {
  pub nodes: Vec<JsNodeSpec>,
  pub edges: Vec<JsEdgeSpec>,
  pub read_only: Option<bool>,
  pub create_if_missing: Option<bool>,
  pub lock_file: Option<bool>,
}

// =============================================================================
// Key Specs
// =============================================================================

#[derive(Clone, Debug)]
enum KeySpec {
  Prefix {
    prefix: String,
  },
  Template {
    prefix: String,
    template: String,
  },
  Parts {
    prefix: String,
    fields: Vec<String>,
    separator: String,
  },
}

impl KeySpec {
  fn prefix(&self) -> &str {
    match self {
      KeySpec::Prefix { prefix } => prefix,
      KeySpec::Template { prefix, .. } => prefix,
      KeySpec::Parts { prefix, .. } => prefix,
    }
  }
}

fn parse_key_spec(node_name: &str, spec: Option<JsKeySpec>) -> Result<KeySpec> {
  let spec = match spec {
    Some(spec) => spec,
    None => {
      return Ok(KeySpec::Prefix {
        prefix: format!("{node_name}:"),
      })
    }
  };

  let kind = spec.kind.as_str();
  match kind {
    "prefix" => Ok(KeySpec::Prefix {
      prefix: spec.prefix.unwrap_or_else(|| format!("{node_name}:")),
    }),
    "template" => {
      let template = spec
        .template
        .ok_or_else(|| Error::from_reason("template key spec requires template"))?;
      let prefix = spec
        .prefix
        .unwrap_or_else(|| infer_prefix_from_template(&template));
      Ok(KeySpec::Template { prefix, template })
    }
    "parts" => {
      let fields = spec
        .fields
        .ok_or_else(|| Error::from_reason("parts key spec requires fields"))?;
      if fields.is_empty() {
        return Err(Error::from_reason(
          "parts key spec requires at least one field",
        ));
      }
      Ok(KeySpec::Parts {
        prefix: spec.prefix.unwrap_or_else(|| format!("{node_name}:")),
        fields,
        separator: spec.separator.unwrap_or_else(|| ":".to_string()),
      })
    }
    _ => Err(Error::from_reason(format!("unknown key spec kind: {kind}"))),
  }
}

fn infer_prefix_from_template(template: &str) -> String {
  match template.find('{') {
    Some(pos) => template[..pos].to_string(),
    None => "".to_string(),
  }
}

// =============================================================================
// Prop Spec Conversion
// =============================================================================

fn prop_spec_to_def(name: &str, spec: &JsPropSpec) -> Result<PropDef> {
  let mut prop = match spec.r#type.as_str() {
    "string" => PropDef::string(name),
    "int" => PropDef::int(name),
    "float" => PropDef::float(name),
    "bool" => PropDef::bool(name),
    "vector" => PropDef {
      name: name.to_string(),
      prop_type: RayPropType::Any,
      required: false,
      default: None,
    },
    "any" => PropDef {
      name: name.to_string(),
      prop_type: RayPropType::Any,
      required: false,
      default: None,
    },
    other => return Err(Error::from_reason(format!("unknown prop type: {other}"))),
  };

  let optional = spec.optional.unwrap_or(false);
  if !optional {
    prop = prop.required();
  }

  if let Some(default_value) = spec.r#default.clone() {
    prop = prop.default(default_value.into());
  }

  Ok(prop)
}

// =============================================================================
// JS Value Conversion
// =============================================================================

fn js_value_to_prop_value(_env: &Env, value: Unknown) -> Result<PropValue> {
  match value.get_type()? {
    ValueType::Undefined => Ok(PropValue::Null),
    ValueType::Null => Ok(PropValue::Null),
    ValueType::Boolean => Ok(PropValue::Bool(value.coerce_to_bool()?)),
    ValueType::Number => Ok(PropValue::F64(value.coerce_to_number()?.get_double()?)),
    ValueType::String => Ok(PropValue::String(
      value.coerce_to_string()?.into_utf8()?.as_str()?.to_string(),
    )),
    ValueType::BigInt => {
      let big: BigInt = unsafe { value.cast()? };
      let (v, _lossless) = big.get_i64();
      Ok(PropValue::I64(v))
    }
    ValueType::Object => {
      let obj = value.coerce_to_object()?;
      if obj.is_array()? {
        let values: Vec<f64> = unsafe { value.cast()? };
        let values = values.into_iter().map(|v| v as f32).collect();
        return Ok(PropValue::VectorF32(values));
      }

      // Check for JsPropValue-style object
      if obj.has_named_property("propType")? {
        let prop_type: DbPropType = obj.get_named_property("propType")?;
        let bool_value: Option<bool> = obj.get_named_property("boolValue")?;
        let int_value: Option<i64> = obj.get_named_property("intValue")?;
        let float_value: Option<f64> = obj.get_named_property("floatValue")?;
        let string_value: Option<String> = obj.get_named_property("stringValue")?;
        let vector_value: Option<Vec<f64>> = obj.get_named_property("vectorValue")?;
        let prop_value = JsPropValue {
          prop_type,
          bool_value,
          int_value,
          float_value,
          string_value,
          vector_value,
        };
        return Ok(prop_value.into());
      }

      Err(Error::from_reason(
        "Object props must be plain values or JsPropValue",
      ))
    }
    _ => Err(Error::from_reason("Unsupported prop value type")),
  }
}

fn js_props_to_map(env: &Env, props: Option<Object>) -> Result<HashMap<String, PropValue>> {
  let mut result = HashMap::new();
  let props = match props {
    Some(props) => props,
    None => return Ok(result),
  };

  for name in Object::keys(&props)? {
    let value: Unknown = props.get_named_property(&name)?;
    result.insert(name, js_value_to_prop_value(env, value)?);
  }

  Ok(result)
}

fn js_value_to_string(_env: &Env, value: Unknown, field: &str) -> Result<String> {
  match value.get_type()? {
    ValueType::String => Ok(value.coerce_to_string()?.into_utf8()?.as_str()?.to_string()),
    ValueType::Number => Ok(value.coerce_to_number()?.get_double()?.to_string()),
    ValueType::Boolean => Ok(value.coerce_to_bool()?.to_string()),
    ValueType::BigInt => {
      let big: BigInt = unsafe { value.cast()? };
      let (v, _lossless) = big.get_i64();
      Ok(v.to_string())
    }
    _ => Err(Error::from_reason(format!(
      "Invalid key field '{field}' value type"
    ))),
  }
}

fn render_template(template: &str, args: &HashMap<String, String>) -> Result<String> {
  let mut out = String::new();
  let mut chars = template.chars().peekable();
  while let Some(ch) = chars.next() {
    if ch == '{' {
      let mut field = String::new();
      while let Some(c) = chars.next() {
        if c == '}' {
          break;
        }
        field.push(c);
      }
      if field.is_empty() {
        return Err(Error::from_reason("Empty template field"));
      }
      let value = args
        .get(&field)
        .ok_or_else(|| Error::from_reason(format!("Missing key field: {field}")))?;
      out.push_str(value);
    } else {
      out.push(ch);
    }
  }
  Ok(out)
}

fn key_suffix_from_js(env: &Env, spec: &KeySpec, value: Unknown) -> Result<String> {
  let prefix = spec.prefix();
  match value.get_type()? {
    ValueType::String => {
      let raw = value.coerce_to_string()?.into_utf8()?.as_str()?.to_string();
      if raw.starts_with(prefix) {
        Ok(raw[prefix.len()..].to_string())
      } else {
        match spec {
          KeySpec::Prefix { .. } => Ok(raw),
          _ => Err(Error::from_reason(
            "Key spec requires object or full key string",
          )),
        }
      }
    }
    ValueType::Object => {
      let obj = value.coerce_to_object()?;

      match spec {
        KeySpec::Prefix { .. } => {
          if obj.has_named_property("id")? {
            let val: Unknown = obj.get_named_property("id")?;
            return js_value_to_string(env, val, "id");
          }
          Err(Error::from_reason("Key object must include 'id'"))
        }
        KeySpec::Template { prefix, template } => {
          let mut args = HashMap::new();
          for name in Object::keys(&obj)? {
            let val: Unknown = obj.get_named_property(&name)?;
            args.insert(name.clone(), js_value_to_string(env, val, &name)?);
          }
          let full_key = render_template(template, &args)?;
          if !full_key.starts_with(prefix) {
            return Err(Error::from_reason(
              "Template key does not start with prefix",
            ));
          }
          Ok(full_key[prefix.len()..].to_string())
        }
        KeySpec::Parts {
          fields, separator, ..
        } => {
          let mut parts = Vec::with_capacity(fields.len());
          for field in fields {
            let val: Unknown = obj
              .get_named_property(field)
              .map_err(|_| Error::from_reason(format!("Missing key field: {field}")))?;
            parts.push(js_value_to_string(env, val, field)?);
          }
          Ok(parts.join(separator))
        }
      }
    }
    _ => Err(Error::from_reason("Invalid key value")),
  }
}

// =============================================================================
// Node Ref Helpers
// =============================================================================

fn prop_value_to_js(env: &Env, value: PropValue) -> Result<Unknown> {
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

fn node_to_js(
  env: &Env,
  node_id: NodeId,
  node_key: Option<String>,
  node_type: &str,
  props: HashMap<String, PropValue>,
) -> Result<Object<'static>> {
  let mut obj = Object::new(env)?;
  obj.set_named_property("$id", node_id as i64)?;
  obj.set_named_property("$key", node_key.as_deref().unwrap_or(""))?;
  obj.set_named_property("$type", node_type)?;

  for (name, value) in props {
    let js_value = prop_value_to_js(env, value)?;
    obj.set_named_property(&name, js_value)?;
  }

  Ok(Object::from_raw(env.raw(), obj.raw()))
}

fn get_node_props(ray: &RustRay, node_id: NodeId) -> HashMap<String, PropValue> {
  let mut props = HashMap::new();
  if let Some(props_by_id) = get_node_props_db(ray.raw(), node_id) {
    for (key_id, value) in props_by_id {
      if let Some(name) = ray.raw().get_propkey_name(key_id) {
        props.insert(name, value);
      }
    }
  }
  props
}

fn get_neighbors(
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
          if etype.is_none() || etype == Some(patch.etype) {
            if !edges
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
          if etype.is_none() || etype == Some(patch.etype) {
            if !edges
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
    }
    TraversalDirection::Both => {
      edges.extend(get_neighbors(db, node_id, TraversalDirection::Out, etype));
      edges.extend(get_neighbors(db, node_id, TraversalDirection::In, etype));
    }
  }

  edges
}

// =============================================================================
// Ray Handle
// =============================================================================

#[napi]
pub struct Ray {
  inner: Arc<Mutex<Option<RustRay>>>,
  node_specs: Arc<HashMap<String, KeySpec>>,
}

impl Ray {
  fn with_ray<R>(&self, f: impl FnOnce(&RustRay) -> Result<R>) -> Result<R> {
    let guard = self.inner.lock();
    let ray = guard
      .as_ref()
      .ok_or_else(|| Error::from_reason("Ray is closed"))?;
    f(ray)
  }

  fn with_ray_mut<R>(&self, f: impl FnOnce(&mut RustRay) -> Result<R>) -> Result<R> {
    let mut guard = self.inner.lock();
    let ray = guard
      .as_mut()
      .ok_or_else(|| Error::from_reason("Ray is closed"))?;
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
impl Ray {
  /// Open a Ray database
  #[napi(factory)]
  pub fn open(path: String, options: JsRayOptions) -> Result<Self> {
    let mut node_specs: HashMap<String, KeySpec> = HashMap::new();
    let mut ray_opts = RayOptions::new();
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

    let ray = RustRay::open(path, ray_opts).map_err(|e| Error::from_reason(e.to_string()))?;

    Ok(Ray {
      inner: Arc::new(Mutex::new(Some(ray))),
      node_specs: Arc::new(node_specs),
    })
  }

  /// Close the database
  #[napi]
  pub fn close(&self) -> Result<()> {
    let mut guard = self.inner.lock();
    if let Some(ray) = guard.take() {
      ray.close().map_err(|e| Error::from_reason(e.to_string()))?;
    }
    Ok(())
  }

  /// Get a node by key (returns node object with props)
  #[napi]
  pub fn get(&self, env: Env, node_type: String, key: Unknown) -> Result<Option<Object>> {
    let spec = self.key_spec(&node_type)?.clone();
    let key_suffix = key_suffix_from_js(&env, &spec, key)?;
    self.with_ray(move |ray| {
      let node_ref = ray
        .get(&node_type, &key_suffix)
        .map_err(|e| Error::from_reason(e.to_string()))?;

      match node_ref {
        Some(node_ref) => {
          let props = get_node_props(ray, node_ref.id);
          let obj = node_to_js(&env, node_ref.id, node_ref.key, &node_ref.node_type, props)?;
          Ok(Some(obj))
        }
        None => Ok(None),
      }
    })
  }

  /// Get a node by ID (returns node object with props)
  #[napi]
  pub fn get_by_id(&self, env: Env, node_id: i64) -> Result<Option<Object>> {
    self.with_ray(move |ray| {
      let node_ref = ray
        .get_by_id(node_id as NodeId)
        .map_err(|e| Error::from_reason(e.to_string()))?;
      match node_ref {
        Some(node_ref) => {
          let props = get_node_props(ray, node_ref.id);
          let obj = node_to_js(&env, node_ref.id, node_ref.key, &node_ref.node_type, props)?;
          Ok(Some(obj))
        }
        None => Ok(None),
      }
    })
  }

  /// Check if a node exists
  #[napi]
  pub fn exists(&self, node_id: i64) -> Result<bool> {
    self.with_ray(|ray| Ok(ray.exists(node_id as NodeId)))
  }

  /// Delete a node by ID
  #[napi]
  pub fn delete_by_id(&self, node_id: i64) -> Result<bool> {
    self.with_ray_mut(|ray| {
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
    self.with_ray_mut(|ray| {
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
  pub fn insert(&self, node_type: String) -> Result<RayInsertBuilder> {
    let spec = self.key_spec(&node_type)?.clone();
    let prefix = spec.prefix().to_string();
    Ok(RayInsertBuilder {
      ray: self.inner.clone(),
      node_type,
      key_prefix: prefix,
      key_spec: spec,
    })
  }

  /// Create an update builder by node ID
  #[napi]
  pub fn update_by_id(&self, node_id: i64) -> Result<RayUpdateBuilder> {
    Ok(RayUpdateBuilder {
      ray: self.inner.clone(),
      node_id: node_id as NodeId,
      updates: HashMap::new(),
    })
  }

  /// Create an update builder by key
  #[napi]
  pub fn update_by_key(
    &self,
    env: Env,
    node_type: String,
    key: Unknown,
  ) -> Result<RayUpdateBuilder> {
    let spec = self.key_spec(&node_type)?.clone();
    let key_suffix = key_suffix_from_js(&env, &spec, key)?;
    let node_id = self.with_ray(|ray| {
      let full_key = ray
        .node_def(&node_type)
        .ok_or_else(|| Error::from_reason(format!("Unknown node type: {node_type}")))?
        .key(&key_suffix);
      Ok(get_node_by_key_db(ray.raw(), &full_key))
    })?;

    match node_id {
      Some(node_id) => Ok(RayUpdateBuilder {
        ray: self.inner.clone(),
        node_id,
        updates: HashMap::new(),
      }),
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
    self.with_ray_mut(|ray| {
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
    self.with_ray_mut(|ray| {
      ray
        .unlink(src as NodeId, &edge_type, dst as NodeId)
        .map_err(|e| Error::from_reason(e.to_string()))
    })
  }

  /// Check if an edge exists
  #[napi]
  pub fn has_edge(&self, src: i64, edge_type: String, dst: i64) -> Result<bool> {
    self.with_ray(move |ray| {
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

  /// List all nodes of a type (returns array of node objects)
  #[napi]
  pub fn all(&self, env: Env, node_type: String) -> Result<Vec<Object>> {
    self.with_ray(|ray| {
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
    self.with_ray(|ray| match node_type {
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
    self.with_ray(|ray| match edge_type {
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
    self.with_ray(|ray| {
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
            etype: edge.etype as u32,
            dst: edge.dst as i64,
          })
          .collect(),
      )
    })
  }

  /// Begin a traversal from a node ID
  #[napi]
  pub fn from(&self, node_id: i64) -> Result<RayTraversal> {
    Ok(RayTraversal {
      ray: self.inner.clone(),
      builder: TraversalBuilder::new(vec![node_id as NodeId]),
    })
  }

  /// Begin a traversal from multiple nodes
  #[napi]
  pub fn from_nodes(&self, node_ids: Vec<i64>) -> Result<RayTraversal> {
    Ok(RayTraversal {
      ray: self.inner.clone(),
      builder: TraversalBuilder::new(node_ids.into_iter().map(|id| id as NodeId).collect()),
    })
  }

  /// Begin a path finding query
  #[napi]
  pub fn path(&self, source: i64, target: i64) -> Result<RayPath> {
    Ok(RayPath::new(
      self.inner.clone(),
      source as NodeId,
      vec![target as NodeId],
    ))
  }

  /// Begin a path finding query to multiple targets
  #[napi]
  pub fn path_to_any(&self, source: i64, targets: Vec<i64>) -> Result<RayPath> {
    Ok(RayPath::new(
      self.inner.clone(),
      source as NodeId,
      targets.into_iter().map(|id| id as NodeId).collect(),
    ))
  }
}

/// Ray entrypoint (TS parity)
#[napi]
pub fn ray(path: String, options: JsRayOptions) -> Result<Ray> {
  Ray::open(path, options)
}

// =============================================================================
// Insert Builder
// =============================================================================

#[napi]
pub struct RayInsertBuilder {
  ray: Arc<Mutex<Option<RustRay>>>,
  node_type: String,
  key_prefix: String,
  key_spec: KeySpec,
}

#[napi]
impl RayInsertBuilder {
  /// Specify values for a single insert
  #[napi]
  pub fn values(
    &self,
    env: Env,
    key: Unknown,
    props: Option<Object>,
  ) -> Result<RayInsertExecutorSingle> {
    let key_suffix = key_suffix_from_js(&env, &self.key_spec, key)?;
    let full_key = format!("{}{}", self.key_prefix, key_suffix);
    let props_map = js_props_to_map(&env, props)?;
    Ok(RayInsertExecutorSingle {
      ray: self.ray.clone(),
      node_type: self.node_type.clone(),
      full_key,
      props: props_map,
    })
  }

  /// Specify values for multiple inserts
  #[napi]
  pub fn values_many(&self, env: Env, entries: Vec<Unknown>) -> Result<RayInsertExecutorMany> {
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
    Ok(RayInsertExecutorMany {
      ray: self.ray.clone(),
      node_type: self.node_type.clone(),
      entries: items,
    })
  }
}

#[napi]
pub struct RayInsertExecutorSingle {
  ray: Arc<Mutex<Option<RustRay>>>,
  node_type: String,
  full_key: String,
  props: HashMap<String, PropValue>,
}

#[napi]
impl RayInsertExecutorSingle {
  /// Execute the insert without returning
  #[napi]
  pub fn execute(&self) -> Result<()> {
    insert_single(&self.ray, &self.node_type, &self.full_key, &self.props).map(|_| ())
  }

  /// Execute the insert and return the node
  #[napi]
  pub fn returning(&self, env: Env) -> Result<Object> {
    let node_ref = insert_single(&self.ray, &self.node_type, &self.full_key, &self.props)?;
    let props = node_ref.1.unwrap_or_else(HashMap::new);
    node_to_js(
      &env,
      node_ref.0,
      Some(self.full_key.clone()),
      &self.node_type,
      props,
    )
  }
}

#[napi]
pub struct RayInsertExecutorMany {
  ray: Arc<Mutex<Option<RustRay>>>,
  node_type: String,
  entries: Vec<(String, HashMap<String, PropValue>)>,
}

#[napi]
impl RayInsertExecutorMany {
  /// Execute the inserts without returning
  #[napi]
  pub fn execute(&self) -> Result<()> {
    for (full_key, props) in &self.entries {
      insert_single(&self.ray, &self.node_type, full_key, props)?;
    }
    Ok(())
  }

  /// Execute the inserts and return nodes
  #[napi]
  pub fn returning(&self, env: Env) -> Result<Vec<Object>> {
    let mut out = Vec::with_capacity(self.entries.len());
    for (full_key, props) in &self.entries {
      let (node_id, _) = insert_single(&self.ray, &self.node_type, full_key, props)?;
      let props = props.clone();
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
  ray: &Arc<Mutex<Option<RustRay>>>,
  node_type: &str,
  full_key: &str,
  props: &HashMap<String, PropValue>,
) -> Result<(NodeId, Option<HashMap<String, PropValue>>)> {
  let mut guard = ray.lock();
  let ray = guard
    .as_mut()
    .ok_or_else(|| Error::from_reason("Ray is closed"))?;
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

// =============================================================================
// Update Builder
// =============================================================================

#[napi]
pub struct RayUpdateBuilder {
  ray: Arc<Mutex<Option<RustRay>>>,
  node_id: NodeId,
  updates: HashMap<String, Option<PropValue>>,
}

#[napi]
impl RayUpdateBuilder {
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

  /// Execute the update
  #[napi]
  pub fn execute(&self) -> Result<()> {
    let mut guard = self.ray.lock();
    let ray = guard
      .as_mut()
      .ok_or_else(|| Error::from_reason("Ray is closed"))?;

    if self.updates.is_empty() {
      return Ok(());
    }

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
// Traversal Builder
// =============================================================================

#[napi]
pub struct RayTraversal {
  ray: Arc<Mutex<Option<RustRay>>>,
  builder: TraversalBuilder,
}

#[napi]
impl RayTraversal {
  #[napi]
  pub fn out(&mut self, edge_type: Option<String>) -> Result<()> {
    let etype = self.resolve_etype(edge_type)?;
    self.builder = self.builder.clone().out(etype);
    Ok(())
  }

  #[napi(js_name = "in")]
  pub fn in_(&mut self, edge_type: Option<String>) -> Result<()> {
    let etype = self.resolve_etype(edge_type)?;
    self.builder = self.builder.clone().r#in(etype);
    Ok(())
  }

  #[napi]
  pub fn both(&mut self, edge_type: Option<String>) -> Result<()> {
    let etype = self.resolve_etype(edge_type)?;
    self.builder = self.builder.clone().both(etype);
    Ok(())
  }

  #[napi]
  pub fn traverse(&mut self, edge_type: Option<String>, options: JsTraverseOptions) -> Result<()> {
    let etype = self.resolve_etype(edge_type)?;
    let opts = TraverseOptions {
      max_depth: options.max_depth as usize,
      min_depth: options.min_depth.unwrap_or(1) as usize,
      direction: options
        .direction
        .map(|d| match d {
          JsTraversalDirection::Out => TraversalDirection::Out,
          JsTraversalDirection::In => TraversalDirection::In,
          JsTraversalDirection::Both => TraversalDirection::Both,
        })
        .unwrap_or(TraversalDirection::Out),
      unique: options.unique.unwrap_or(true),
      where_edge: None,
      where_node: None,
    };
    self.builder = self.builder.clone().traverse(etype, opts);
    Ok(())
  }

  #[napi]
  pub fn take(&mut self, limit: i64) -> Result<()> {
    self.builder = self.builder.clone().take(limit as usize);
    Ok(())
  }

  #[napi]
  pub fn select(&mut self, props: Vec<String>) -> Result<()> {
    let refs: Vec<&str> = props.iter().map(|p| p.as_str()).collect();
    self.builder = self.builder.clone().select_props(&refs);
    Ok(())
  }

  #[napi]
  pub fn nodes(&self) -> Result<Vec<i64>> {
    let ray = self.ray.clone();
    let guard = ray.lock();
    let ray = guard
      .as_ref()
      .ok_or_else(|| Error::from_reason("Ray is closed"))?;

    Ok(
      self
        .builder
        .clone()
        .collect_node_ids(|node_id, dir, etype| get_neighbors(ray.raw(), node_id, dir, etype))
        .into_iter()
        .map(|id| id as i64)
        .collect(),
    )
  }

  #[napi]
  pub fn edges(&self) -> Result<Vec<JsFullEdge>> {
    let ray = self.ray.clone();
    let guard = ray.lock();
    let ray = guard
      .as_ref()
      .ok_or_else(|| Error::from_reason("Ray is closed"))?;

    let edges = self
      .builder
      .clone()
      .execute(|node_id, dir, etype| get_neighbors(ray.raw(), node_id, dir, etype))
      .filter_map(|result| result.edge)
      .map(|edge| JsFullEdge {
        src: edge.src as i64,
        etype: edge.etype as u32,
        dst: edge.dst as i64,
      })
      .collect();

    Ok(edges)
  }

  #[napi]
  pub fn count(&self) -> Result<i64> {
    let ray = self.ray.clone();
    let guard = ray.lock();
    let ray = guard
      .as_ref()
      .ok_or_else(|| Error::from_reason("Ray is closed"))?;

    Ok(
      self
        .builder
        .clone()
        .count(|node_id, dir, etype| get_neighbors(ray.raw(), node_id, dir, etype)) as i64,
    )
  }

  fn resolve_etype(&self, edge_type: Option<String>) -> Result<Option<ETypeId>> {
    let edge_type = match edge_type {
      Some(edge_type) => edge_type,
      None => return Ok(None),
    };
    let guard = self.ray.lock();
    let ray = guard
      .as_ref()
      .ok_or_else(|| Error::from_reason("Ray is closed"))?;
    let edge_def = ray
      .edge_def(&edge_type)
      .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {edge_type}")))?;
    let etype_id = edge_def
      .etype_id
      .ok_or_else(|| Error::from_reason("Edge type not initialized"))?;
    Ok(Some(etype_id))
  }
}

// =============================================================================
// Path Builder
// =============================================================================

#[napi]
pub struct RayPath {
  ray: Arc<Mutex<Option<RustRay>>>,
  source: NodeId,
  targets: HashSet<NodeId>,
  allowed_etypes: HashSet<ETypeId>,
  direction: TraversalDirection,
  max_depth: usize,
}

impl RayPath {
  fn new(ray: Arc<Mutex<Option<RustRay>>>, source: NodeId, targets: Vec<NodeId>) -> Self {
    Self {
      ray,
      source,
      targets: targets.into_iter().collect(),
      allowed_etypes: HashSet::new(),
      direction: TraversalDirection::Out,
      max_depth: 100,
    }
  }
}

#[napi]
impl RayPath {
  #[napi]
  pub fn via(&mut self, edge_type: String) -> Result<()> {
    let guard = self.ray.lock();
    let ray = guard
      .as_ref()
      .ok_or_else(|| Error::from_reason("Ray is closed"))?;
    let edge_def = ray
      .edge_def(&edge_type)
      .ok_or_else(|| Error::from_reason(format!("Unknown edge type: {edge_type}")))?;
    let etype_id = edge_def
      .etype_id
      .ok_or_else(|| Error::from_reason("Edge type not initialized"))?;
    self.allowed_etypes.insert(etype_id);
    Ok(())
  }

  #[napi]
  pub fn max_depth(&mut self, depth: i64) -> Result<()> {
    self.max_depth = depth as usize;
    Ok(())
  }

  #[napi]
  pub fn direction(&mut self, direction: String) -> Result<()> {
    self.direction = match direction.as_str() {
      "out" => TraversalDirection::Out,
      "in" => TraversalDirection::In,
      "both" => TraversalDirection::Both,
      _ => TraversalDirection::Out,
    };
    Ok(())
  }

  #[napi]
  pub fn bidirectional(&mut self) -> Result<()> {
    self.direction = TraversalDirection::Both;
    Ok(())
  }

  #[napi]
  pub fn find(&self) -> Result<JsPathResult> {
    let guard = self.ray.lock();
    let ray = guard
      .as_ref()
      .ok_or_else(|| Error::from_reason("Ray is closed"))?;
    let config = PathConfig {
      source: self.source,
      targets: self.targets.clone(),
      allowed_etypes: self.allowed_etypes.clone(),
      direction: self.direction,
      max_depth: self.max_depth,
    };
    let result = dijkstra(
      config,
      |node_id, dir, etype| get_neighbors(ray.raw(), node_id, dir, etype),
      |_src, _etype, _dst| 1.0,
    );
    Ok(JsPathResult::from(result))
  }

  #[napi]
  pub fn find_bfs(&self) -> Result<JsPathResult> {
    let guard = self.ray.lock();
    let ray = guard
      .as_ref()
      .ok_or_else(|| Error::from_reason("Ray is closed"))?;
    let config = PathConfig {
      source: self.source,
      targets: self.targets.clone(),
      allowed_etypes: self.allowed_etypes.clone(),
      direction: self.direction,
      max_depth: self.max_depth,
    };
    let result = bfs(config, |node_id, dir, etype| {
      get_neighbors(ray.raw(), node_id, dir, etype)
    });
    Ok(JsPathResult::from(result))
  }

  #[napi]
  pub fn find_k_shortest(&self, k: i64) -> Result<Vec<JsPathResult>> {
    let guard = self.ray.lock();
    let ray = guard
      .as_ref()
      .ok_or_else(|| Error::from_reason("Ray is closed"))?;
    let config = PathConfig {
      source: self.source,
      targets: self.targets.clone(),
      allowed_etypes: self.allowed_etypes.clone(),
      direction: self.direction,
      max_depth: self.max_depth,
    };
    let results = yen_k_shortest(
      config,
      k as usize,
      |node_id, dir, etype| get_neighbors(ray.raw(), node_id, dir, etype),
      |_src, _etype, _dst| 1.0,
    );
    Ok(results.into_iter().map(JsPathResult::from).collect())
  }
}

#[napi(object)]
pub struct JsPathEdge {
  pub src: i64,
  pub etype: i64,
  pub dst: i64,
}

#[napi(object)]
pub struct JsPathResult {
  pub path: Vec<i64>,
  pub edges: Vec<JsPathEdge>,
  pub total_weight: f64,
  pub found: bool,
}

impl From<PathResult> for JsPathResult {
  fn from(result: PathResult) -> Self {
    JsPathResult {
      path: result.path.into_iter().map(|id| id as i64).collect(),
      edges: result
        .edges
        .into_iter()
        .map(|(src, etype, dst)| JsPathEdge {
          src: src as i64,
          etype: etype as i64,
          dst: dst as i64,
        })
        .collect(),
      total_weight: result.total_weight,
      found: result.found,
    }
  }
}
