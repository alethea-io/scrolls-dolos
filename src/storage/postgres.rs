use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use gasket::framework::*;
use pallas::network::miniprotocols::Point;
use tokio_postgres::NoTls;

use serde::Deserialize;

use crate::framework::*;

pub struct Worker {
    pool: Pool<PostgresConnectionManager<NoTls>>,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let manager =
            PostgresConnectionManager::new_from_stringlike(stage.url.clone(), NoTls).or_panic()?;
        let pool = Pool::builder().build(manager).await.or_panic()?;
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
        let conn = self.pool.get().await.or_restart()?;

        match event {
            StorageEvent::RDBMS(rdbms_command) => {
                match rdbms_command {
                    RDBMSCommand::BlockStarting(_) => {
                        conn.execute("BEGIN", &[]).await.or_restart()?;
                    }
                    RDBMSCommand::ExecuteSQL(sql) => {
                        conn.execute(sql, &[]).await.or_restart()?;
                    }
                    RDBMSCommand::BlockFinished(point) => {
                        if let Point::Specific(slot, _hash) = point {
                            conn.execute("COMMIT", &[]).await.or_restart()?;

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
    url: String,

    pub input: StorageInputPort,

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
            cursor: ctx.cursor.clone(),
            url: self.url,
            ops_count: Default::default(),
            latest_block: Default::default(),
        };

        Ok(stage)
    }
}
