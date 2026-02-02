//! Read operations for SingleFileDB
//!
//! Handles all query operations: get properties, get edges, key lookups,
//! label checks, and neighbor traversal.

use std::collections::HashMap;

use crate::mvcc::visibility::{
  edge_exists as mvcc_edge_exists, get_visible_version, node_exists as mvcc_node_exists,
};
use crate::types::*;

use super::SingleFileDB;

impl SingleFileDB {
  // ========================================================================
  // Node Property Reads
  // ========================================================================

  /// Get all properties for a node
  ///
  /// Returns None if the node doesn't exist or is deleted.
  /// Merges properties from snapshot with delta modifications.
  pub fn get_node_props(&self, node_id: NodeId) -> Option<HashMap<PropKeyId, PropValue>> {
    let tx_handle = self.current_tx_handle();
    let tx_guard = tx_handle.as_ref().map(|tx| tx.lock());
    let pending = tx_guard.as_ref().map(|tx| &tx.pending);

    if pending.is_some_and(|p| p.is_node_deleted(node_id)) {
      return None;
    }

    let mut txid = 0;
    let mut tx_snapshot_ts = 0;
    if let Some(mvcc) = self.mvcc.as_ref() {
      if let Some(tx) = tx_guard.as_ref() {
        txid = tx.txid;
        tx_snapshot_ts = tx.snapshot_ts;
      } else {
        tx_snapshot_ts = mvcc.tx_manager.lock().get_next_commit_ts();
      }
    }

    let delta = self.delta.read();

    let mut props = HashMap::new();
    let snapshot = self.snapshot.read();

    // Get properties from snapshot first
    if let Some(ref snap) = *snapshot {
      if let Some(phys) = snap.get_phys_node(node_id) {
        if let Some(snapshot_props) = snap.get_node_props(phys) {
          props = snapshot_props;
        }
      }
    }

    // Apply committed delta modifications
    if let Some(node_delta) = delta.get_node_delta(node_id) {
      if let Some(ref delta_props) = node_delta.props {
        for (&key_id, value) in delta_props {
          match value {
            Some(v) => {
              props.insert(key_id, v.clone());
            }
            None => {
              props.remove(&key_id);
            }
          }
        }
      }
    }

    let mut mvcc_node_visible = None;
    if let Some(mvcc) = self.mvcc.as_ref() {
      let vc = mvcc.version_chain.lock();
      if let Some(version) = vc.get_node_version(node_id) {
        mvcc_node_visible = Some(mvcc_node_exists(Some(version), tx_snapshot_ts, txid));
      }
      for key_id in vc.node_prop_keys(node_id) {
        if let Some(prop_version) = vc.get_node_prop_version(node_id, key_id) {
          if let Some(visible) = get_visible_version(&prop_version, tx_snapshot_ts, txid) {
            match &visible.data {
              Some(v) => {
                props.insert(key_id, v.clone());
              }
              None => {
                props.remove(&key_id);
              }
            }
          }
        }
      }
    }

    // Apply pending modifications (overlay)
    if let Some(pending_delta) = pending {
      if let Some(node_delta) = pending_delta.get_node_delta(node_id) {
        if let Some(ref delta_props) = node_delta.props {
          for (&key_id, value) in delta_props {
            match value {
              Some(v) => {
                props.insert(key_id, v.clone());
              }
              None => {
                props.remove(&key_id);
              }
            }
          }
        }
      }
    }

    // Check if node exists at all
    let node_exists_in_pending =
      pending.is_some_and(|p| p.is_node_created(node_id) || p.get_node_delta(node_id).is_some());
    let node_exists = if node_exists_in_pending {
      true
    } else if let Some(visible) = mvcc_node_visible {
      visible
    } else if delta.is_node_deleted(node_id) {
      false
    } else {
      let node_exists_in_delta =
        delta.is_node_created(node_id) || delta.get_node_delta(node_id).is_some();
      if node_exists_in_delta {
        true
      } else if let Some(ref snap) = *snapshot {
        snap.get_phys_node(node_id).is_some()
      } else {
        false
      }
    };

    if !node_exists {
      return None;
    }

    if let Some(mvcc) = self.mvcc.as_ref() {
      if txid != 0 {
        let mut tx_mgr = mvcc.tx_manager.lock();
        for key_id in props.keys() {
          tx_mgr.record_read(txid, format!("nodeprop:{node_id}:{key_id}"));
        }
      }
    }

    Some(props)
  }

  /// Get a specific property for a node
  ///
  /// Returns None if the node doesn't exist, is deleted, or doesn't have the property.
  pub fn get_node_prop(&self, node_id: NodeId, key_id: PropKeyId) -> Option<PropValue> {
    let tx_handle = self.current_tx_handle();
    if let Some(handle) = tx_handle.as_ref() {
      let tx = handle.lock();
      if tx.pending.is_node_deleted(node_id) {
        return None;
      }
      if let Some(node_delta) = tx.pending.get_node_delta(node_id) {
        if let Some(ref delta_props) = node_delta.props {
          if let Some(value) = delta_props.get(&key_id) {
            return value.clone();
          }
        }
      }
      if tx.pending.is_node_created(node_id) {
        return None;
      }
    }

    let mut mvcc_node_visible = None;
    if let Some(mvcc) = self.mvcc.as_ref() {
      let (txid, tx_snapshot_ts) = if let Some(handle) = tx_handle.as_ref() {
        let tx = handle.lock();
        (tx.txid, tx.snapshot_ts)
      } else {
        (0, mvcc.tx_manager.lock().get_next_commit_ts())
      };
      if txid != 0 {
        let mut tx_mgr = mvcc.tx_manager.lock();
        tx_mgr.record_read(txid, format!("nodeprop:{node_id}:{key_id}"));
      }
      let vc = mvcc.version_chain.lock();
      if let Some(version) = vc.get_node_version(node_id) {
        mvcc_node_visible = Some(mvcc_node_exists(Some(version), tx_snapshot_ts, txid));
      }
      if let Some(prop_version) = vc.get_node_prop_version(node_id, key_id) {
        if let Some(visible) = get_visible_version(&prop_version, tx_snapshot_ts, txid) {
          return visible.data.clone();
        }
      }
    }

    let delta = self.delta.read();

    // Check if node is deleted (unless MVCC snapshot says otherwise)
    if mvcc_node_visible == Some(false) {
      return None;
    }
    if mvcc_node_visible.is_none() && delta.is_node_deleted(node_id) {
      return None;
    }

    // Check delta first (for modifications)
    if let Some(node_delta) = delta.get_node_delta(node_id) {
      if let Some(ref delta_props) = node_delta.props {
        if let Some(value) = delta_props.get(&key_id) {
          // None means explicitly deleted
          return value.clone();
        }
      }
    }

    // Fall back to snapshot
    let snapshot = self.snapshot.read();
    if let Some(ref snap) = *snapshot {
      if let Some(phys) = snap.get_phys_node(node_id) {
        return snap.get_node_prop(phys, key_id);
      }
    }

    // Check if node exists at all (in delta as created)
    if delta.is_node_created(node_id) {
      // Node exists but doesn't have this property
      return None;
    }

    None
  }

  // ========================================================================
  // Edge Property Reads
  // ========================================================================

  /// Get all properties for an edge
  ///
  /// Returns None if the edge doesn't exist.
  /// Merges properties from snapshot with delta modifications.
  pub fn get_edge_props(
    &self,
    src: NodeId,
    etype: ETypeId,
    dst: NodeId,
  ) -> Option<HashMap<PropKeyId, PropValue>> {
    let tx_handle = self.current_tx_handle();
    let tx_guard = tx_handle.as_ref().map(|tx| tx.lock());
    let pending = tx_guard.as_ref().map(|tx| &tx.pending);

    if pending.is_some_and(|p| p.is_node_deleted(src) || p.is_node_deleted(dst)) {
      return None;
    }
    if pending.is_some_and(|p| p.is_edge_deleted(src, etype, dst)) {
      return None;
    }

    let mut txid = 0;
    let mut tx_snapshot_ts = 0;
    if let Some(mvcc) = self.mvcc.as_ref() {
      if let Some(tx) = tx_guard.as_ref() {
        txid = tx.txid;
        tx_snapshot_ts = tx.snapshot_ts;
      } else {
        tx_snapshot_ts = mvcc.tx_manager.lock().get_next_commit_ts();
      }
    }

    let delta = self.delta.read();

    let mut mvcc_src_visible = None;
    let mut mvcc_dst_visible = None;
    let mut mvcc_edge_visible = None;

    let mut props = HashMap::new();
    let snapshot = self.snapshot.read();

    // First, determine if edge exists
    let edge_added_in_delta = delta.is_edge_added(src, etype, dst);
    let edge_added_in_pending = pending.is_some_and(|p| p.is_edge_added(src, etype, dst));
    let mut edge_exists_in_snapshot = false;

    // Check snapshot for edge existence and get base properties
    if let Some(ref snap) = *snapshot {
      if let Some(src_phys) = snap.get_phys_node(src) {
        if let Some(dst_phys) = snap.get_phys_node(dst) {
          if let Some(edge_idx) = snap.find_edge_index(src_phys, etype, dst_phys) {
            edge_exists_in_snapshot = true;
            // Get properties from snapshot
            if let Some(snapshot_props) = snap.get_edge_props(edge_idx) {
              props = snapshot_props;
            }
          }
        }
      }
    }

    if let Some(mvcc) = self.mvcc.as_ref() {
      let vc = mvcc.version_chain.lock();
      if let Some(version) = vc.get_node_version(src) {
        mvcc_src_visible = Some(mvcc_node_exists(Some(version), tx_snapshot_ts, txid));
      }
      if let Some(version) = vc.get_node_version(dst) {
        mvcc_dst_visible = Some(mvcc_node_exists(Some(version), tx_snapshot_ts, txid));
      }
      if let Some(version) = vc.get_edge_version(src, etype, dst) {
        mvcc_edge_visible = Some(mvcc_edge_exists(Some(version), tx_snapshot_ts, txid));
      }
      for key_id in vc.edge_prop_keys(src, etype, dst) {
        if let Some(prop_version) = vc.get_edge_prop_version(src, etype, dst, key_id) {
          if let Some(visible) = get_visible_version(&prop_version, tx_snapshot_ts, txid) {
            match &visible.data {
              Some(v) => {
                props.insert(key_id, v.clone());
              }
              None => {
                props.remove(&key_id);
              }
            }
          }
        }
      }
    }

    if mvcc_src_visible == Some(false) || mvcc_dst_visible == Some(false) {
      return None;
    }
    if mvcc_src_visible.is_none() && delta.is_node_deleted(src) {
      return None;
    }
    if mvcc_dst_visible.is_none() && delta.is_node_deleted(dst) {
      return None;
    }
    if mvcc_edge_visible == Some(false) {
      return None;
    }
    if mvcc_edge_visible.is_none() && delta.is_edge_deleted(src, etype, dst) {
      return None;
    }

    // Edge must exist either in delta or snapshot (unless MVCC says visible)
    if mvcc_edge_visible != Some(true)
      && !edge_added_in_delta
      && !edge_added_in_pending
      && !edge_exists_in_snapshot
    {
      return None;
    }

    // Apply committed delta modifications (only if edge exists)
    if let Some(delta_props) = delta.get_edge_props_delta(src, etype, dst) {
      for (&key_id, value) in delta_props {
        match value {
          Some(v) => {
            props.insert(key_id, v.clone());
          }
          None => {
            props.remove(&key_id);
          }
        }
      }
    }

    // Apply pending modifications
    if let Some(pending_delta) = pending {
      if let Some(delta_props) = pending_delta.get_edge_props_delta(src, etype, dst) {
        for (&key_id, value) in delta_props {
          match value {
            Some(v) => {
              props.insert(key_id, v.clone());
            }
            None => {
              props.remove(&key_id);
            }
          }
        }
      }
    }

    if let Some(mvcc) = self.mvcc.as_ref() {
      if txid != 0 {
        let mut tx_mgr = mvcc.tx_manager.lock();
        for key_id in props.keys() {
          tx_mgr.record_read(txid, format!("edgeprop:{src}:{etype}:{dst}:{key_id}"));
        }
      }
    }

    Some(props)
  }

  /// Get a specific property for an edge
  ///
  /// Returns None if the edge doesn't exist or doesn't have the property.
  pub fn get_edge_prop(
    &self,
    src: NodeId,
    etype: ETypeId,
    dst: NodeId,
    key_id: PropKeyId,
  ) -> Option<PropValue> {
    let tx_handle = self.current_tx_handle();
    if let Some(handle) = tx_handle.as_ref() {
      let tx = handle.lock();
      if tx.pending.is_node_deleted(src) || tx.pending.is_node_deleted(dst) {
        return None;
      }
      if tx.pending.is_edge_deleted(src, etype, dst) {
        return None;
      }
      if let Some(delta_props) = tx.pending.get_edge_props_delta(src, etype, dst) {
        if let Some(value) = delta_props.get(&key_id) {
          return value.clone();
        }
      }
    }

    let mut mvcc_src_visible = None;
    let mut mvcc_dst_visible = None;
    let mut mvcc_edge_visible = None;
    if let Some(mvcc) = self.mvcc.as_ref() {
      let (txid, tx_snapshot_ts) = if let Some(handle) = tx_handle.as_ref() {
        let tx = handle.lock();
        (tx.txid, tx.snapshot_ts)
      } else {
        (0, mvcc.tx_manager.lock().get_next_commit_ts())
      };
      if txid != 0 {
        let mut tx_mgr = mvcc.tx_manager.lock();
        tx_mgr.record_read(txid, format!("edgeprop:{src}:{etype}:{dst}:{key_id}"));
      }
      let vc = mvcc.version_chain.lock();
      if let Some(version) = vc.get_node_version(src) {
        mvcc_src_visible = Some(mvcc_node_exists(Some(version), tx_snapshot_ts, txid));
      }
      if let Some(version) = vc.get_node_version(dst) {
        mvcc_dst_visible = Some(mvcc_node_exists(Some(version), tx_snapshot_ts, txid));
      }
      if let Some(version) = vc.get_edge_version(src, etype, dst) {
        mvcc_edge_visible = Some(mvcc_edge_exists(Some(version), tx_snapshot_ts, txid));
      }
      if let Some(prop_version) = vc.get_edge_prop_version(src, etype, dst, key_id) {
        if let Some(visible) = get_visible_version(&prop_version, tx_snapshot_ts, txid) {
          return visible.data.clone();
        }
      }
    }

    let delta = self.delta.read();

    if mvcc_src_visible == Some(false) || mvcc_dst_visible == Some(false) {
      return None;
    }

    // Check if either node is deleted
    if mvcc_src_visible.is_none() && delta.is_node_deleted(src) {
      return None;
    }
    if mvcc_dst_visible.is_none() && delta.is_node_deleted(dst) {
      return None;
    }

    // Check if edge is deleted in delta
    if mvcc_edge_visible == Some(false) {
      return None;
    }
    if mvcc_edge_visible.is_none() && delta.is_edge_deleted(src, etype, dst) {
      return None;
    }

    // First, determine if edge exists at all
    let edge_added_in_delta = delta.is_edge_added(src, etype, dst);
    let edge_added_in_pending = tx_handle
      .as_ref()
      .map(|handle| handle.lock().pending.is_edge_added(src, etype, dst))
      .unwrap_or(false);
    let snapshot = self.snapshot.read();
    let edge_exists_in_snapshot = if let Some(ref snap) = *snapshot {
      if let Some(src_phys) = snap.get_phys_node(src) {
        if let Some(dst_phys) = snap.get_phys_node(dst) {
          snap.find_edge_index(src_phys, etype, dst_phys).is_some()
        } else {
          false
        }
      } else {
        false
      }
    } else {
      false
    };

    // Edge must exist either in delta or snapshot
    if mvcc_edge_visible != Some(true)
      && !edge_added_in_delta
      && !edge_added_in_pending
      && !edge_exists_in_snapshot
    {
      return None;
    }

    // Check delta first (for modifications)
    if let Some(delta_props) = delta.get_edge_props_delta(src, etype, dst) {
      if let Some(value) = delta_props.get(&key_id) {
        // Some(None) means explicitly deleted
        return value.clone();
      }
    }

    // Fall back to snapshot
    if let Some(ref snap) = *snapshot {
      if let Some(src_phys) = snap.get_phys_node(src) {
        if let Some(dst_phys) = snap.get_phys_node(dst) {
          if let Some(edge_idx) = snap.find_edge_index(src_phys, etype, dst_phys) {
            // Get property from snapshot
            if let Some(snapshot_props) = snap.get_edge_props(edge_idx) {
              if let Some(value) = snapshot_props.get(&key_id) {
                return Some(value.clone());
              }
            }
          }
        }
      }
    }

    None
  }

  // ========================================================================
  // Edge Traversal
  // ========================================================================

  /// Get outgoing edges for a node
  ///
  /// Returns edges as (edge_type_id, destination_node_id) pairs.
  /// Merges edges from snapshot with delta additions/deletions.
  /// Filters out edges to deleted nodes.
  pub fn get_out_edges(&self, node_id: NodeId) -> Vec<(ETypeId, NodeId)> {
    let tx_handle = self.current_tx_handle();
    let tx_guard = tx_handle.as_ref().map(|tx| tx.lock());
    let pending = tx_guard.as_ref().map(|tx| &tx.pending);

    // If node is deleted, no edges
    if pending.is_some_and(|p| p.is_node_deleted(node_id)) {
      return Vec::new();
    }

    let delta = self.delta.read();

    // If node is deleted in committed state, no edges
    if delta.is_node_deleted(node_id) {
      return Vec::new();
    }

    let mut edges = Vec::new();
    let snapshot = self.snapshot.read();

    // Get edges from snapshot
    if let Some(ref snap) = *snapshot {
      if let Some(phys) = snap.get_phys_node(node_id) {
        for (dst_phys, etype) in snap.iter_out_edges(phys) {
          // Convert physical dst to NodeId
          if let Some(dst_node_id) = snap.get_node_id(dst_phys) {
            // Skip edges to deleted nodes
            if delta.is_node_deleted(dst_node_id)
              || pending.is_some_and(|p| p.is_node_deleted(dst_node_id))
            {
              continue;
            }
            // Skip edges deleted in delta
            if delta.is_edge_deleted(node_id, etype, dst_node_id)
              || pending.is_some_and(|p| p.is_edge_deleted(node_id, etype, dst_node_id))
            {
              continue;
            }
            edges.push((etype, dst_node_id));
          }
        }
      }
    }

    // Add edges from delta
    if let Some(added_edges) = delta.out_add.get(&node_id) {
      for edge_patch in added_edges {
        // Skip edges to deleted nodes
        if delta.is_node_deleted(edge_patch.other)
          || pending.is_some_and(|p| p.is_node_deleted(edge_patch.other))
        {
          continue;
        }
        if pending.is_some_and(|p| p.is_edge_deleted(node_id, edge_patch.etype, edge_patch.other))
        {
          continue;
        }
        edges.push((edge_patch.etype, edge_patch.other));
      }
    }

    if let Some(added_edges) = pending.and_then(|p| p.out_add.get(&node_id)) {
      for edge_patch in added_edges {
        if delta.is_node_deleted(edge_patch.other)
          || pending.is_some_and(|p| p.is_node_deleted(edge_patch.other))
        {
          continue;
        }
        edges.push((edge_patch.etype, edge_patch.other));
      }
    }

    // Sort by (etype, dst) for consistent ordering
    edges.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    edges
  }

  /// Get incoming edges for a node
  ///
  /// Returns edges as (edge_type_id, source_node_id) pairs.
  /// Merges edges from snapshot with delta additions/deletions.
  /// Filters out edges from deleted nodes.
  pub fn get_in_edges(&self, node_id: NodeId) -> Vec<(ETypeId, NodeId)> {
    let tx_handle = self.current_tx_handle();
    let tx_guard = tx_handle.as_ref().map(|tx| tx.lock());
    let pending = tx_guard.as_ref().map(|tx| &tx.pending);

    // If node is deleted, no edges
    if pending.is_some_and(|p| p.is_node_deleted(node_id)) {
      return Vec::new();
    }

    let delta = self.delta.read();

    // If node is deleted, no edges
    if delta.is_node_deleted(node_id) {
      return Vec::new();
    }

    let mut edges = Vec::new();
    let snapshot = self.snapshot.read();

    // Get edges from snapshot
    if let Some(ref snap) = *snapshot {
      if let Some(phys) = snap.get_phys_node(node_id) {
        for (src_phys, etype, _out_index) in snap.iter_in_edges(phys) {
          // Convert physical src to NodeId
          if let Some(src_node_id) = snap.get_node_id(src_phys) {
            // Skip edges from deleted nodes
            if delta.is_node_deleted(src_node_id)
              || pending.is_some_and(|p| p.is_node_deleted(src_node_id))
            {
              continue;
            }
            // Skip edges deleted in delta
            if delta.is_edge_deleted(src_node_id, etype, node_id)
              || pending.is_some_and(|p| p.is_edge_deleted(src_node_id, etype, node_id))
            {
              continue;
            }
            edges.push((etype, src_node_id));
          }
        }
      }
    }

    // Add edges from delta (in_add stores patches where other=src)
    if let Some(added_edges) = delta.in_add.get(&node_id) {
      for edge_patch in added_edges {
        // Skip edges from deleted nodes
        if delta.is_node_deleted(edge_patch.other)
          || pending.is_some_and(|p| p.is_node_deleted(edge_patch.other))
        {
          continue;
        }
        if pending.is_some_and(|p| p.is_edge_deleted(edge_patch.other, edge_patch.etype, node_id))
        {
          continue;
        }
        edges.push((edge_patch.etype, edge_patch.other));
      }
    }

    if let Some(added_edges) = pending.and_then(|p| p.in_add.get(&node_id)) {
      for edge_patch in added_edges {
        if delta.is_node_deleted(edge_patch.other)
          || pending.is_some_and(|p| p.is_node_deleted(edge_patch.other))
        {
          continue;
        }
        edges.push((edge_patch.etype, edge_patch.other));
      }
    }

    // Sort by (etype, src) for consistent ordering
    edges.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    edges
  }

  /// Get out-degree (number of outgoing edges) for a node
  pub fn get_out_degree(&self, node_id: NodeId) -> usize {
    self.get_out_edges(node_id).len()
  }

  /// Get in-degree (number of incoming edges) for a node
  pub fn get_in_degree(&self, node_id: NodeId) -> usize {
    self.get_in_edges(node_id).len()
  }

  /// Get neighbors via outgoing edges of a specific type
  ///
  /// Returns destination node IDs for edges of the given type.
  pub fn get_out_neighbors(&self, node_id: NodeId, etype: ETypeId) -> Vec<NodeId> {
    let neighbors: Vec<NodeId> = self
      .get_out_edges(node_id)
      .into_iter()
      .filter(|(e, _)| *e == etype)
      .map(|(_, dst)| dst)
      .collect();
    if let Some(mvcc) = self.mvcc.as_ref() {
      let tx_handle = self.current_tx_handle();
      if let Some(handle) = tx_handle.as_ref() {
        let tx = handle.lock();
        let mut tx_mgr = mvcc.tx_manager.lock();
        tx_mgr.record_read(tx.txid, format!("neighbors_out:{node_id}:{etype}"));
      }
    }
    neighbors
  }

  /// Get neighbors via incoming edges of a specific type
  ///
  /// Returns source node IDs for edges of the given type.
  pub fn get_in_neighbors(&self, node_id: NodeId, etype: ETypeId) -> Vec<NodeId> {
    let neighbors: Vec<NodeId> = self
      .get_in_edges(node_id)
      .into_iter()
      .filter(|(e, _)| *e == etype)
      .map(|(_, src)| src)
      .collect();
    if let Some(mvcc) = self.mvcc.as_ref() {
      let tx_handle = self.current_tx_handle();
      if let Some(handle) = tx_handle.as_ref() {
        let tx = handle.lock();
        let mut tx_mgr = mvcc.tx_manager.lock();
        tx_mgr.record_read(tx.txid, format!("neighbors_in:{node_id}:{etype}"));
      }
    }
    neighbors
  }

  /// Check if there are any outgoing edges of a specific type
  pub fn has_out_edges(&self, node_id: NodeId, etype: ETypeId) -> bool {
    self.get_out_edges(node_id).iter().any(|(e, _)| *e == etype)
  }

  /// Check if there are any incoming edges of a specific type
  pub fn has_in_edges(&self, node_id: NodeId, etype: ETypeId) -> bool {
    self.get_in_edges(node_id).iter().any(|(e, _)| *e == etype)
  }

  // ========================================================================
  // Node Label Reads
  // ========================================================================

  /// Check if a node has a specific label
  pub fn node_has_label(&self, node_id: NodeId, label_id: LabelId) -> bool {
    let tx_handle = self.current_tx_handle();
    let tx_guard = tx_handle.as_ref().map(|tx| tx.lock());
    let pending = tx_guard.as_ref().map(|tx| &tx.pending);

    if pending.is_some_and(|p| p.is_node_deleted(node_id)) {
      return false;
    }
    if pending.is_some_and(|p| p.is_label_removed(node_id, label_id)) {
      return false;
    }
    if pending.is_some_and(|p| p.is_label_added(node_id, label_id)) {
      return true;
    }

    let delta = self.delta.read();

    // Check if node is deleted
    if delta.is_node_deleted(node_id) {
      return false;
    }

    // Check if label was removed in delta
    if delta.is_label_removed(node_id, label_id) {
      return false;
    }

    // Check if label was added in delta
    if delta.is_label_added(node_id, label_id) {
      return true;
    }

    // Check snapshot for label (if present)
    if let Some(ref snapshot) = *self.snapshot.read() {
      if let Some(phys) = snapshot.get_phys_node(node_id) {
        if let Some(labels) = snapshot.get_node_labels(phys) {
          return labels.contains(&label_id);
        }
      }
    }

    false
  }

  /// Get all labels for a node
  pub fn get_node_labels(&self, node_id: NodeId) -> Vec<LabelId> {
    let tx_handle = self.current_tx_handle();
    let tx_guard = tx_handle.as_ref().map(|tx| tx.lock());
    let pending = tx_guard.as_ref().map(|tx| &tx.pending);

    if pending.is_some_and(|p| p.is_node_deleted(node_id)) {
      return Vec::new();
    }

    let delta = self.delta.read();

    // Check if node is deleted
    if delta.is_node_deleted(node_id) {
      return Vec::new();
    }

    let mut labels = std::collections::HashSet::new();

    // Load labels from snapshot first (if present)
    if let Some(ref snapshot) = *self.snapshot.read() {
      if let Some(phys) = snapshot.get_phys_node(node_id) {
        if let Some(snapshot_labels) = snapshot.get_node_labels(phys) {
          labels.extend(snapshot_labels);
        }
      }
    }

    // Add labels from committed delta
    if let Some(added) = delta.get_added_labels(node_id) {
      labels.extend(added.iter().copied());
    }

    // Remove labels deleted in committed delta
    if let Some(removed) = delta.get_removed_labels(node_id) {
      for &label_id in removed {
        labels.remove(&label_id);
      }
    }

    // Apply pending label changes
    if let Some(pending_delta) = pending {
      if let Some(added) = pending_delta.get_added_labels(node_id) {
        labels.extend(added.iter().copied());
      }
      if let Some(removed) = pending_delta.get_removed_labels(node_id) {
        for &label_id in removed {
          labels.remove(&label_id);
        }
      }
    }

    let mut result: Vec<_> = labels.into_iter().collect();
    result.sort_unstable();
    result
  }

  // ========================================================================
  // Key Lookups
  // ========================================================================

  /// Look up a node by its key
  ///
  /// Returns the NodeId if found, None otherwise.
  /// Checks delta key index first, then falls back to snapshot.
  pub fn get_node_by_key(&self, key: &str) -> Option<NodeId> {
    let tx_handle = self.current_tx_handle();
    let tx_guard = tx_handle.as_ref().map(|tx| tx.lock());
    let pending = tx_guard.as_ref().map(|tx| &tx.pending);

    if let Some(mvcc) = self.mvcc.as_ref() {
      if let Some(handle) = tx_handle.as_ref() {
        let tx = handle.lock();
        let mut tx_mgr = mvcc.tx_manager.lock();
        tx_mgr.record_read(tx.txid, format!("key:{key}"));
      }
    }

    let delta = self.delta.read();

    // Check pending key index first
    if pending.is_some_and(|p| p.key_index_deleted.contains(key)) {
      return None;
    }

    if let Some(&node_id) = pending.and_then(|p| p.key_index.get(key)) {
      if !pending.is_some_and(|p| p.is_node_deleted(node_id))
        && !delta.is_node_deleted(node_id)
      {
        return Some(node_id);
      }
    }

    // Check committed delta key index
    if delta.key_index_deleted.contains(key) {
      return None;
    }

    if let Some(&node_id) = delta.key_index.get(key) {
      // Verify node isn't deleted
      if !delta.is_node_deleted(node_id)
        && !pending.is_some_and(|p| p.is_node_deleted(node_id))
      {
        return Some(node_id);
      }
    }

    // Fall back to snapshot
    let snapshot = self.snapshot.read();
    if let Some(ref snap) = *snapshot {
      if let Some(node_id) = snap.lookup_by_key(key) {
        // Verify node isn't deleted in delta
        if !delta.is_node_deleted(node_id)
          && !pending.is_some_and(|p| p.is_node_deleted(node_id))
        {
          return Some(node_id);
        }
      }
    }

    None
  }

  /// Get the key for a node
  ///
  /// Returns the key string if the node has one, None otherwise.
  pub fn get_node_key(&self, node_id: NodeId) -> Option<String> {
    let tx_handle = self.current_tx_handle();
    let tx_guard = tx_handle.as_ref().map(|tx| tx.lock());
    let pending = tx_guard.as_ref().map(|tx| &tx.pending);

    if pending.is_some_and(|p| p.is_node_deleted(node_id)) {
      return None;
    }

    if let Some(node_delta) = pending.and_then(|p| p.created_nodes.get(&node_id)) {
      return node_delta.key.clone();
    }

    let delta = self.delta.read();

    // Check if node is deleted
    if delta.is_node_deleted(node_id) {
      return None;
    }

    // Check created nodes in delta first
    if let Some(node_delta) = delta.created_nodes.get(&node_id) {
      return node_delta.key.clone();
    }

    // Fall back to snapshot
    let snapshot = self.snapshot.read();
    if let Some(ref snap) = *snapshot {
      if let Some(phys) = snap.get_phys_node(node_id) {
        return snap.get_node_key(phys);
      }
    }

    None
  }
}
