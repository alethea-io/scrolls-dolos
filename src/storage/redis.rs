use std::ops::DerefMut;

use gasket::framework::*;
use pallas::network::miniprotocols::Point;
use r2d2_redis::{
    r2d2::{self, Pool},
    redis::{self, Commands, RedisWrite, ToRedisArgs},
    RedisConnectionManager,
};
use serde::Deserialize;
use tracing;

use crate::framework::*;

impl<'a> ToRedisArgs for &'a Value {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match *self {
            Value::String(ref x) => x.write_redis_args(out),
            Value::BigInt(ref x) => x.to_string().write_redis_args(out),
            Value::Cbor(ref x) => x.write_redis_args(out),
            Value::Json(ref x) => todo!("{}", x),
        }
    }
}

pub struct Worker {
    pool: Pool<RedisConnectionManager>,
    stream: String,
    maxlen: Option<usize>,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let manager = RedisConnectionManager::new(stage.config.url.clone()).or_panic()?;
        let pool = r2d2::Pool::builder().build(manager).or_panic()?;

        let stream = stage
            .config
            .stream_name
            .clone()
            .unwrap_or(String::from("scrolls-sink"));

        let maxlen = stage.config.stream_max_length;

        Ok(Self {
            pool,
            stream,
            maxlen,
        })
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<StorageEvent>, WorkerError> {
        let msg = stage.input.recv().await.or_panic()?;
        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(
        &mut self,
        event: &StorageEvent,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        let mut conn = self.pool.get().or_restart()?;

        match event {
            StorageEvent::CRDT(crdt_command) => {
                match crdt_command {
                    CRDTCommand::BlockStarting(_) => {
                        // start redis transaction
                        redis::cmd("MULTI").query(conn.deref_mut()).or_restart()?;
                    }
                    CRDTCommand::GrowOnlySetAdd(key, value) => {
                        conn.sadd(key, value).or_restart()?;
                    }
                    CRDTCommand::TwoPhaseSetAdd(key, value) => {
                        tracing::debug!("adding to 2-phase set [{}], value [{}]", key, value);

                        conn.sadd(key, value).or_restart()?;
                    }
                    CRDTCommand::TwoPhaseSetRemove(key, value) => {
                        tracing::debug!("removing from 2-phase set [{}], value [{}]", key, value);

                        conn.sadd(format!("{}.ts", key), value).or_restart()?;
                    }
                    CRDTCommand::SetAdd(key, value) => {
                        tracing::debug!("adding to set [{}], value [{}]", key, value);

                        conn.sadd(key, value).or_restart()?;
                    }
                    CRDTCommand::SetRemove(key, value) => {
                        tracing::debug!("removing from set [{}], value [{}]", key, value);

                        conn.srem(key, value).or_restart()?;
                    }
                    CRDTCommand::LastWriteWins(key, value, ts) => {
                        tracing::debug!("last write for [{}], slot [{}]", key, ts);

                        conn.zadd(key, value, *ts).or_restart()?;
                    }
                    CRDTCommand::SortedSetAdd(key, value, delta) => {
                        tracing::debug!(
                            "sorted set add [{}], value [{}], delta [{}]",
                            key,
                            value,
                            delta
                        );

                        conn.zincr(key, value, *delta).or_restart()?;
                    }
                    CRDTCommand::SortedSetRemove(key, value, delta) => {
                        tracing::debug!(
                            "sorted set remove [{}], value [{}], delta [{}]",
                            key,
                            value,
                            delta
                        );

                        conn.zincr(key, value, *delta).or_restart()?;

                        // removal of dangling scores  (aka garbage collection)
                        conn.zrembyscore(key, 0, 0).or_restart()?;
                    }
                    CRDTCommand::AnyWriteWins(key, value) => {
                        tracing::debug!("overwrite [{}]", key);

                        conn.set(key, value).or_restart()?;
                    }
                    CRDTCommand::PNCounter(key, value) => {
                        tracing::debug!("increasing counter [{}], by [{}]", key, value);

                        conn.incr(key, *value).or_restart()?;
                    }
                    CRDTCommand::HashSetValue(key, member, value) => {
                        tracing::debug!("setting hash key {} member {}", key, member);

                        conn.hset(key, member, value).or_restart()?;
                    }
                    CRDTCommand::HashCounter(key, member, delta) => {
                        tracing::debug!(
                            "increasing hash key {} member {} by {}",
                            key,
                            member,
                            delta
                        );

                        conn.hincr(key, member, *delta).or_restart()?;
                    }
                    CRDTCommand::HashUnsetKey(key, member) => {
                        tracing::debug!("deleting hash key {} member {}", key, member);

                        conn.hdel(member, key).or_restart()?;
                    }
                    CRDTCommand::BlockFinished(point) => {
                        if let Point::Specific(slot, _hash) = point {
                            // End redis transaction
                            redis::cmd("EXEC").query(conn.deref_mut()).or_restart()?;

                            stage.ops_count.inc(1);
                            stage.latest_block.set(*slot as i64);
                        }
                    }
                };
            }
            _ => {}
        }

        Ok(())
    }
}

#[derive(Stage)]
#[stage(name = "storage-redis", unit = "StorageEvent", worker = "Worker")]
pub struct Stage {
    pub input: StorageInputPort,

    config: Config,
    cursor: Cursor,

    #[metric]
    ops_count: gasket::metrics::Counter,

    #[metric]
    latest_block: gasket::metrics::Gauge,
}

#[derive(Debug, Clone, Deserialize)]
pub enum StreamStrategy {
    ByBlock,
}

#[derive(Default, Debug, Deserialize)]
pub struct Config {
    pub url: String,
    pub stream_name: Option<String>,
    pub stream_max_length: Option<usize>,
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Stage, Error> {
        let stage = Stage {
            input: Default::default(),
            config: self,
            cursor: ctx.cursor.clone(),
            ops_count: Default::default(),
            latest_block: Default::default(),
        };

        Ok(stage)
    }
}
