use gasket::framework::*;
use pallas::network::miniprotocols::Point;
use r2d2_postgres::postgres::NoTls;
use r2d2_postgres::r2d2;
use r2d2_postgres::PostgresConnectionManager;

use serde::Deserialize;

use crate::framework::*;

pub struct Worker {
    pool: r2d2::Pool<PostgresConnectionManager<NoTls>>,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let manager = PostgresConnectionManager::new(stage.config.url.parse().or_panic()?, NoTls);
        let pool = r2d2::Pool::builder().build(manager).or_panic()?;

        Ok(Self { pool })
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
            StorageEvent::RDBMS(rdbms_command) => {
                match rdbms_command {
                    RDBMSCommand::BlockStarting(_) => {
                        conn.execute("BEGIN", &[]).or_restart()?;
                    }
                    RDBMSCommand::ExecuteSQL(sql) => {
                        conn.execute(sql, &[]).or_restart()?;
                    }
                    RDBMSCommand::BlockFinished(point) => {
                        if let Point::Specific(slot, _hash) = point {
                            conn.execute("COMMIT", &[]).or_restart()?;

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
#[stage(name = "storage-postgres", unit = "StorageEvent", worker = "Worker")]
pub struct Stage {
    pub input: StorageInputPort,

    config: Config,
    cursor: Cursor,

    #[metric]
    ops_count: gasket::metrics::Counter,

    #[metric]
    latest_block: gasket::metrics::Gauge,
}

#[derive(Default, Debug, Deserialize)]
pub struct Config {
    pub url: String,
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
