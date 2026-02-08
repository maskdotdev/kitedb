//! Replication token/cursor types.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum ReplicationRole {
  #[default]
  Disabled,
  Primary,
  Replica,
}

impl fmt::Display for ReplicationRole {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let value = match self {
      ReplicationRole::Disabled => "disabled",
      ReplicationRole::Primary => "primary",
      ReplicationRole::Replica => "replica",
    };
    write!(f, "{value}")
  }
}

impl FromStr for ReplicationRole {
  type Err = ReplicationParseError;

  fn from_str(raw: &str) -> Result<Self, Self::Err> {
    match raw {
      "disabled" => Ok(Self::Disabled),
      "primary" => Ok(Self::Primary),
      "replica" => Ok(Self::Replica),
      _ => Err(ReplicationParseError::new(format!(
        "invalid replication role: {raw}"
      ))),
    }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationParseError {
  message: String,
}

impl ReplicationParseError {
  fn new(message: impl Into<String>) -> Self {
    Self {
      message: message.into(),
    }
  }
}

impl fmt::Display for ReplicationParseError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.message)
  }
}

impl std::error::Error for ReplicationParseError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommitToken {
  pub epoch: u64,
  pub log_index: u64,
}

impl CommitToken {
  pub const fn new(epoch: u64, log_index: u64) -> Self {
    Self { epoch, log_index }
  }
}

impl fmt::Display for CommitToken {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{}:{}", self.epoch, self.log_index)
  }
}

impl Ord for CommitToken {
  fn cmp(&self, other: &Self) -> Ordering {
    self
      .epoch
      .cmp(&other.epoch)
      .then_with(|| self.log_index.cmp(&other.log_index))
  }
}

impl PartialOrd for CommitToken {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl FromStr for CommitToken {
  type Err = ReplicationParseError;

  fn from_str(raw: &str) -> Result<Self, Self::Err> {
    let mut parts = raw.split(':');
    let epoch = parse_u64_component(parts.next(), "epoch", raw)?;
    let log_index = parse_u64_component(parts.next(), "log_index", raw)?;

    if parts.next().is_some() {
      return Err(ReplicationParseError::new(format!(
        "invalid token format: {raw}"
      )));
    }

    Ok(Self::new(epoch, log_index))
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReplicationCursor {
  pub epoch: u64,
  pub segment_id: u64,
  pub segment_offset: u64,
  pub log_index: u64,
}

impl ReplicationCursor {
  pub const fn new(epoch: u64, segment_id: u64, segment_offset: u64, log_index: u64) -> Self {
    Self {
      epoch,
      segment_id,
      segment_offset,
      log_index,
    }
  }
}

impl fmt::Display for ReplicationCursor {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "{}:{}:{}:{}",
      self.epoch, self.segment_id, self.segment_offset, self.log_index
    )
  }
}

impl Ord for ReplicationCursor {
  fn cmp(&self, other: &Self) -> Ordering {
    self
      .epoch
      .cmp(&other.epoch)
      .then_with(|| self.log_index.cmp(&other.log_index))
      .then_with(|| self.segment_id.cmp(&other.segment_id))
      .then_with(|| self.segment_offset.cmp(&other.segment_offset))
  }
}

impl PartialOrd for ReplicationCursor {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl FromStr for ReplicationCursor {
  type Err = ReplicationParseError;

  fn from_str(raw: &str) -> Result<Self, Self::Err> {
    let mut parts = raw.split(':');

    let epoch = parse_u64_component(parts.next(), "epoch", raw)?;
    let segment_id = parse_u64_component(parts.next(), "segment_id", raw)?;
    let segment_offset = parse_u64_component(parts.next(), "segment_offset", raw)?;
    let log_index = parse_u64_component(parts.next(), "log_index", raw)?;

    if parts.next().is_some() {
      return Err(ReplicationParseError::new(format!(
        "invalid cursor format: {raw}"
      )));
    }

    Ok(Self::new(epoch, segment_id, segment_offset, log_index))
  }
}

fn parse_u64_component(
  value: Option<&str>,
  component: &'static str,
  original: &str,
) -> Result<u64, ReplicationParseError> {
  let value = value.ok_or_else(|| {
    ReplicationParseError::new(format!(
      "invalid replication identifier ({component} missing): {original}"
    ))
  })?;

  if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
    return Err(ReplicationParseError::new(format!(
      "invalid {component}: {value}"
    )));
  }

  value.parse::<u64>().map_err(|_| {
    ReplicationParseError::new(format!(
      "invalid replication identifier ({component} overflow): {original}"
    ))
  })
}

#[cfg(test)]
mod tests {
  use super::{CommitToken, ReplicationCursor};
  use rand::{rngs::StdRng, Rng, SeedableRng};
  use std::str::FromStr;

  #[test]
  fn token_roundtrip_fuzz_like() {
    let mut rng = StdRng::seed_from_u64(0xdecafbad);

    for _ in 0..2_000 {
      let token = CommitToken::new(rng.gen_range(0..10_000), rng.gen_range(0..10_000_000));
      let parsed = CommitToken::from_str(&token.to_string()).expect("parse token");
      assert_eq!(parsed, token);
    }
  }

  #[test]
  fn cursor_roundtrip_fuzz_like() {
    let mut rng = StdRng::seed_from_u64(0xabba_cafe);

    for _ in 0..2_000 {
      let cursor = ReplicationCursor::new(
        rng.gen_range(0..1024),
        rng.gen_range(0..4096),
        rng.gen_range(0..1_000_000),
        rng.gen_range(0..10_000_000),
      );

      let parsed = ReplicationCursor::from_str(&cursor.to_string()).expect("parse cursor");
      assert_eq!(parsed, cursor);
    }
  }
}
