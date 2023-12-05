//! Internal pipeline framework

use pallas::network::miniprotocols::Point;
use serde::Deserialize;
use std::fmt::Debug;
use std::path::PathBuf;
use std::str::FromStr;

// we use UtxoRpc as our canonical representation of a Block and Tx
pub use utxorpc::proto::cardano::v1::Block;
pub use utxorpc::proto::cardano::v1::Tx;

// we use GenesisValues from Pallas as our ChainConfig
pub use pallas::ledger::traverse::wellknown::GenesisValues;

pub mod cursor;
pub mod errors;

pub use cursor::*;
pub use errors::*;

#[derive(Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ChainConfig {
    Mainnet,
    Testnet,
    PreProd,
    Preview,
    Custom(GenesisValues),
}

impl Default for ChainConfig {
    fn default() -> Self {
        Self::Mainnet
    }
}

impl From<ChainConfig> for GenesisValues {
    fn from(other: ChainConfig) -> Self {
        match other {
            ChainConfig::Mainnet => GenesisValues::mainnet(),
            ChainConfig::Testnet => GenesisValues::testnet(),
            ChainConfig::PreProd => GenesisValues::preprod(),
            ChainConfig::Preview => GenesisValues::preview(),
            ChainConfig::Custom(x) => x,
        }
    }
}

pub struct Context {
    pub chain: ChainConfig,
    pub intersect: IntersectConfig,
    pub cursor: Cursor,
    pub finalize: Option<FinalizeConfig>,
    pub current_dir: PathBuf,
}

use serde_json::{json, Value as JsonValue};

#[derive(Debug, Clone)]
pub enum Record {
    CborBlock(Vec<u8>),
    CborTx(Vec<u8>),
    GenericJson(JsonValue),
    ParsedTx(Tx),
    ParsedBlock(Block),
}

impl From<Record> for JsonValue {
    fn from(value: Record) -> Self {
        match value {
            Record::CborBlock(x) => json!({ "hex": hex::encode(x) }),
            Record::CborTx(x) => json!({ "hex": hex::encode(x) }),
            Record::ParsedTx(x) => json!(x),
            Record::GenericJson(x) => x,
            Record::ParsedBlock(x) => json!(x),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ChainEvent {
    Apply(Point, Record),
    Undo(Point, Record),
    Reset(Point),
}

impl ChainEvent {
    pub fn apply(point: Point, record: impl Into<Record>) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::Apply(point, record.into()),
        }
    }

    pub fn undo(point: Point, record: impl Into<Record>) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::Undo(point, record.into()),
        }
    }

    pub fn reset(point: Point) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::Reset(point),
        }
    }

    pub fn point(&self) -> &Point {
        match self {
            Self::Apply(x, _) => x,
            Self::Undo(x, _) => x,
            Self::Reset(x) => x,
        }
    }

    pub fn record(&self) -> Option<&Record> {
        match self {
            Self::Apply(_, x) => Some(x),
            Self::Undo(_, x) => Some(x),
            _ => None,
        }
    }

    pub fn map_record(self, f: fn(Record) -> Record) -> Self {
        match self {
            Self::Apply(p, x) => Self::Apply(p, f(x)),
            Self::Undo(p, x) => Self::Undo(p, f(x)),
            Self::Reset(x) => Self::Reset(x),
        }
    }

    pub fn try_map_record<E>(self, f: fn(Record) -> Result<Record, E>) -> Result<Self, E> {
        let out = match self {
            Self::Apply(p, x) => Self::Apply(p, f(x)?),
            Self::Undo(p, x) => Self::Undo(p, f(x)?),
            Self::Reset(x) => Self::Reset(x),
        };

        Ok(out)
    }

    pub fn try_map_record_to_many<E>(
        self,
        f: fn(Record) -> Result<Vec<Record>, E>,
    ) -> Result<Vec<Self>, E> {
        let out = match self {
            Self::Apply(p, x) => f(x)?
                .into_iter()
                .map(|i| Self::Apply(p.clone(), i))
                .collect(),
            Self::Undo(p, x) => f(x)?
                .into_iter()
                .map(|i| Self::Undo(p.clone(), i))
                .collect(),
            Self::Reset(x) => vec![Self::Reset(x)],
        };

        Ok(out)
    }
}

fn point_to_json(point: Point) -> JsonValue {
    match &point {
        pallas::network::miniprotocols::Point::Origin => JsonValue::from("origin"),
        pallas::network::miniprotocols::Point::Specific(slot, hash) => {
            json!({ "slot": slot, "hash": hex::encode(hash)})
        }
    }
}

impl From<ChainEvent> for JsonValue {
    fn from(value: ChainEvent) -> Self {
        match value {
            ChainEvent::Apply(point, record) => {
                json!({
                    "event": "apply",
                    "point": point_to_json(point),
                    "record": JsonValue::from(record.clone())
                })
            }
            ChainEvent::Undo(point, record) => {
                json!({
                    "event": "undo",
                    "point": point_to_json(point),
                    "record": JsonValue::from(record.clone())
                })
            }
            ChainEvent::Reset(point) => {
                json!({
                    "event": "reset",
                    "point": point_to_json(point)
                })
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum StorageEvent {
    CRDT(CRDTCommand),
    RDBMS(RDBMSCommand),
}

pub type Set = String;
pub type Member = String;
pub type Key = String;
pub type Delta = i64;
pub type Timestamp = u64;

#[derive(Clone, Debug)]
pub enum Value {
    String(String),
    BigInt(i128),
    Cbor(Vec<u8>),
    Json(serde_json::Value),
}

impl From<String> for Value {
    fn from(x: String) -> Self {
        Value::String(x)
    }
}

impl From<Vec<u8>> for Value {
    fn from(x: Vec<u8>) -> Self {
        Value::Cbor(x)
    }
}

impl From<serde_json::Value> for Value {
    fn from(x: serde_json::Value) -> Self {
        Value::Json(x)
    }
}

#[derive(Clone, Debug)]
pub enum CRDTCommand {
    BlockStarting(Point),
    SetAdd(Set, Member),
    SetRemove(Set, Member),
    SortedSetAdd(Set, Member, Delta),
    SortedSetRemove(Set, Member, Delta),
    TwoPhaseSetAdd(Set, Member),
    TwoPhaseSetRemove(Set, Member),
    GrowOnlySetAdd(Set, Member),
    LastWriteWins(Key, Value, Timestamp),
    AnyWriteWins(Key, Value),
    PNCounter(Key, Delta),
    HashCounter(Key, Member, Delta),
    HashSetValue(Key, Member, Value),
    HashUnsetKey(Key, Member),
    BlockFinished(Point),
}

impl CRDTCommand {
    pub fn block_starting(block: &Block) -> CRDTCommand {
        let header = block.header.as_ref().unwrap();
        let point = Point::Specific(header.slot, header.hash.to_vec());
        CRDTCommand::BlockStarting(point)
    }

    pub fn set_add(prefix: Option<&str>, key: &str, member: String) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        CRDTCommand::SetAdd(key, member)
    }

    pub fn set_remove(prefix: Option<&str>, key: &str, member: String) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        CRDTCommand::SetRemove(key, member)
    }

    pub fn sorted_set_add(
        prefix: Option<&str>,
        key: &str,
        member: String,
        delta: i64,
    ) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        CRDTCommand::SortedSetAdd(key, member, delta)
    }

    pub fn sorted_set_remove(
        prefix: Option<&str>,
        key: &str,
        member: String,
        delta: i64,
    ) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        CRDTCommand::SortedSetRemove(key, member, delta)
    }

    pub fn any_write_wins<K, V>(prefix: Option<&str>, key: K, value: V) -> CRDTCommand
    where
        K: ToString,
        V: Into<Value>,
    {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key.to_string()),
            None => key.to_string(),
        };

        CRDTCommand::AnyWriteWins(key, value.into())
    }

    pub fn last_write_wins<V>(
        prefix: Option<&str>,
        key: &str,
        value: V,
        ts: Timestamp,
    ) -> CRDTCommand
    where
        V: Into<Value>,
    {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key),
            None => key.to_string(),
        };

        CRDTCommand::LastWriteWins(key, value.into(), ts)
    }

    pub fn hash_set_value<V>(
        prefix: Option<&str>,
        key: &str,
        member: String,
        value: V,
    ) -> CRDTCommand
    where
        V: Into<Value>,
    {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key.to_string()),
            None => key.to_string(),
        };

        CRDTCommand::HashSetValue(key, member, value.into())
    }

    pub fn hash_del_key(prefix: Option<&str>, key: &str, member: String) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key.to_string()),
            None => key.to_string(),
        };

        CRDTCommand::HashUnsetKey(key, member)
    }

    pub fn hash_counter(
        prefix: Option<&str>,
        key: &str,
        member: String,
        delta: i64,
    ) -> CRDTCommand {
        let key = match prefix {
            Some(prefix) => format!("{}.{}", prefix, key.to_string()),
            None => key.to_string(),
        };

        CRDTCommand::HashCounter(key, member, delta)
    }

    pub fn block_finished(block: &Block) -> CRDTCommand {
        let header = block.header.as_ref().unwrap();
        let point = Point::Specific(header.slot, header.hash.to_vec());
        CRDTCommand::BlockFinished(point)
    }

    pub fn from_json(value: &JsonValue) -> Result<CRDTCommand, String> {
        let obj = value.as_object().ok_or("Expected a JSON object")?;

        match obj.get("command").and_then(JsonValue::as_str) {
            Some("SetAdd") => {
                let set = extract_string(obj, "set")?;
                let member = extract_string(obj, "member")?;
                Ok(CRDTCommand::SetAdd(set, member))
            }
            Some("SetRemove") => {
                let set = extract_string(obj, "set")?;
                let member = extract_string(obj, "member")?;
                Ok(CRDTCommand::SetRemove(set, member))
            }
            Some("SortedSetAdd") => {
                let set = extract_string(obj, "set")?;
                let member = extract_string(obj, "member")?;
                let delta = extract_delta(obj, "delta")?;
                Ok(CRDTCommand::SortedSetAdd(set, member, delta))
            }
            Some("SortedSetRemove") => {
                let set = extract_string(obj, "set")?;
                let member = extract_string(obj, "member")?;
                let delta = extract_delta(obj, "delta")?;
                Ok(CRDTCommand::SortedSetRemove(set, member, delta))
            }
            Some("AnyWriteWins") => {
                let key = extract_string(obj, "key")?;
                let value = extract_value(obj, "value")?;
                Ok(CRDTCommand::AnyWriteWins(key, value))
            }
            Some("LastWriteWins") => {
                let key = extract_string(obj, "key")?;
                let value = extract_value(obj, "value")?;
                let ts = extract_timestamp(obj, "timestamp")?;
                Ok(CRDTCommand::LastWriteWins(key, value, ts))
            }
            Some("PNCounter") => {
                let key = extract_string(obj, "key")?;
                let delta = extract_delta(obj, "value")?;
                Ok(CRDTCommand::PNCounter(key, delta))
            }
            Some("HashCounter") => {
                let key = extract_string(obj, "key")?;
                let member = extract_string(obj, "member")?;
                let delta = extract_delta(obj, "delta")?;
                Ok(CRDTCommand::HashCounter(key, member, delta))
            }
            Some("HashSetValue") => {
                let key = extract_string(obj, "key")?;
                let member = extract_string(obj, "member")?;
                let value = extract_value(obj, "value")?;
                Ok(CRDTCommand::HashSetValue(key, member, value))
            }
            Some("HashUnsetKey") => {
                let key = extract_string(obj, "key")?;
                let member = extract_string(obj, "member")?;
                Ok(CRDTCommand::HashUnsetKey(key, member))
            }
            _ => Err("Unknown CRDTCommand".into()),
        }
    }
}

fn extract_string(obj: &serde_json::Map<String, JsonValue>, key: &str) -> Result<String, String> {
    obj.get(key)
        .and_then(JsonValue::as_str)
        .map(String::from)
        .ok_or_else(|| format!("Expected a string for key {}", key))
}

fn extract_delta(obj: &serde_json::Map<String, JsonValue>, key: &str) -> Result<i64, String> {
    match obj.get(key) {
        Some(JsonValue::Number(num)) if num.is_i64() => num
            .as_i64()
            .ok_or_else(|| format!("Expected an integer delta for key {}", key)),
        Some(JsonValue::String(s)) => i64::from_str(s)
            .map_err(|_| format!("Failed to parse stringified integer for key {}", key)),
        _ => Err(format!(
            "Expected an integer or stringified integer delta for key {}",
            key
        )),
    }
}

fn extract_timestamp(obj: &serde_json::Map<String, JsonValue>, key: &str) -> Result<u64, String> {
    obj.get(key)
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| format!("Expected a timestamp for key {}", key))
}

fn extract_value(obj: &serde_json::Map<String, JsonValue>, key: &str) -> Result<Value, String> {
    obj.get(key)
        .cloned()
        .map(Value::Json)
        .ok_or_else(|| format!("Expected a value for key {}", key))
}

#[derive(Clone, Debug)]
pub enum RDBMSCommand {
    BlockStarting(Point),
    ExecuteSQL(String),
    BlockFinished(Point),
}

pub type SourceOutputPort = gasket::messaging::tokio::OutputPort<ChainEvent>;
pub type ReduceInputPort = gasket::messaging::tokio::InputPort<ChainEvent>;
pub type ReduceOutputPort = gasket::messaging::tokio::OutputPort<StorageEvent>;
pub type StorageInputPort = gasket::messaging::tokio::InputPort<StorageEvent>;
pub type InputAdapter<I> = gasket::messaging::tokio::ChannelRecvAdapter<I>;
pub type OutputAdapter<O> = gasket::messaging::tokio::ChannelSendAdapter<O>;

pub trait StageBootstrapper<I, O> {
    fn connect_input(&mut self, adapter: InputAdapter<I>);
    fn connect_output(&mut self, adapter: OutputAdapter<O>);
    fn spawn(self, policy: gasket::runtime::Policy) -> gasket::runtime::Tether;
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", content = "value")]
pub enum IntersectConfig {
    Tip,
    Origin,
    Point(u64, String),
    Breadcrumbs(Vec<(u64, String)>),
}

impl IntersectConfig {
    pub fn points(&self) -> Option<Vec<Point>> {
        match self {
            IntersectConfig::Breadcrumbs(all) => {
                let mapped = all
                    .iter()
                    .map(|(slot, hash)| {
                        let hash = hex::decode(hash).expect("valid hex hash");
                        Point::Specific(*slot, hash)
                    })
                    .collect();

                Some(mapped)
            }
            IntersectConfig::Point(slot, hash) => {
                let hash = hex::decode(hash).expect("valid hex hash");
                Some(vec![Point::Specific(*slot, hash)])
            }
            _ => None,
        }
    }
}

/// Optional configuration to stop processing new blocks after processing:
///   1. a block with the given hash
///   2. the first block on or after a given absolute slot
///   3. TODO: a total of X blocks
#[derive(Deserialize, Debug, Clone)]
pub struct FinalizeConfig {
    until_hash: Option<String>,
    max_block_slot: Option<u64>,
    // max_block_quantity: Option<u64>,
}

pub fn should_finalize(
    config: &Option<FinalizeConfig>,
    last_point: &Point,
    // block_count: u64,
) -> bool {
    let config = match config {
        Some(x) => x,
        None => return false,
    };

    if let Some(expected) = &config.until_hash {
        if let Point::Specific(_, current) = last_point {
            return expected == &hex::encode(current);
        }
    }

    if let Some(max) = config.max_block_slot {
        if last_point.slot_or_default() >= max {
            return true;
        }
    }

    // if let Some(max) = config.max_block_quantity {
    //     if block_count >= max {
    //         return true;
    //     }
    // }

    false
}
