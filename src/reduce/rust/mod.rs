use gasket::framework::*;
use pallas::network::miniprotocols::Point;
use serde::Deserialize;
use utxorpc::proto::cardano::v1::Block;

use crate::framework::*;

pub mod balance_by_address;
pub mod balance_by_stake_address;

pub struct Worker {
    reducers: Vec<Reducer>,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        Ok(Self {
            reducers: stage.reducers.clone(),
        })
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<ChainEvent>, WorkerError> {
        let msg = stage.input.recv().await.or_panic()?;

        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &ChainEvent, stage: &mut Stage) -> Result<(), WorkerError> {
        match unit {
            ChainEvent::Apply(Point::Specific(slot, hash), Record::ParsedBlock(block)) => {
                stage
                    .output
                    .send(gasket::messaging::Message::from(StorageEvent::CRDT(
                        CRDTCommand::block_starting(&block),
                    )))
                    .await
                    .or_panic()?;

                for reducer in self.reducers.iter_mut() {
                    reducer
                        .apply(block.clone(), &mut stage.output)
                        .await
                        .or_panic()?;
                    stage.ops_count.inc(1);
                }

                stage
                    .output
                    .send(gasket::messaging::Message::from(StorageEvent::CRDT(
                        CRDTCommand::block_finished(&block),
                    )))
                    .await
                    .or_panic()?;
            }
            ChainEvent::Undo(Point::Specific(slot, hash), Record::ParsedBlock(block)) => {
                stage
                    .output
                    .send(gasket::messaging::Message::from(StorageEvent::CRDT(
                        CRDTCommand::block_starting(&block),
                    )))
                    .await
                    .or_panic()?;

                for reducer in self.reducers.iter_mut() {
                    reducer
                        .undo(block.clone(), &mut stage.output)
                        .await
                        .or_panic()?;
                    stage.ops_count.inc(1);
                }

                stage
                    .output
                    .send(gasket::messaging::Message::from(StorageEvent::CRDT(
                        CRDTCommand::block_finished(&block),
                    )))
                    .await
                    .or_panic()?;
            }
            _ => panic!("Unhandled ChainEvent variant or Record type in execute"),
        }

        Ok(())
    }
}

#[derive(Stage)]
#[stage(name = "reduce-rust", unit = "ChainEvent", worker = "Worker")]
pub struct Stage {
    pub input: ReduceInputPort,
    pub output: ReduceOutputPort,
    reducers: Vec<Reducer>,
    #[metric]
    ops_count: gasket::metrics::Counter,
    #[metric]
    chain_tip: gasket::metrics::Gauge,
}

#[derive(Deserialize)]
pub struct Config {
    reducers: Vec<ReducerConfig>,
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Stage, Error> {
        let stage = Stage {
            input: Default::default(),
            output: Default::default(),
            reducers: self.reducers.into_iter().map(|x| x.plugin()).collect(),
            ops_count: Default::default(),
            chain_tip: Default::default(),
        };

        Ok(stage)
    }
}

#[derive(Deserialize)]
#[serde(tag = "name")]
pub enum ReducerConfig {
    BalanceByAddress(balance_by_address::Config),
    BalanceByStakeAddress(balance_by_stake_address::Config),
}

impl ReducerConfig {
    fn plugin(self) -> Reducer {
        match self {
            ReducerConfig::BalanceByAddress(c) => c.plugin(),
            ReducerConfig::BalanceByStakeAddress(c) => c.plugin(),
        }
    }
}

#[derive(Clone)]
pub enum Reducer {
    BalanceByAddress(balance_by_address::Reducer),
    BalanceByStakeAddress(balance_by_stake_address::Reducer),
}

impl Reducer {
    pub async fn apply(
        &mut self,
        block: Block,
        output: &mut ReduceOutputPort,
    ) -> Result<(), WorkerError> {
        match self {
            Reducer::BalanceByAddress(x) => x.apply(block, output).await,
            Reducer::BalanceByStakeAddress(x) => x.apply(block, output).await,
        }
    }
    pub async fn undo(
        &mut self,
        block: Block,
        output: &mut ReduceOutputPort,
    ) -> Result<(), WorkerError> {
        match self {
            Reducer::BalanceByAddress(x) => x.undo(block, output).await,
            Reducer::BalanceByStakeAddress(x) => x.undo(block, output).await,
        }
    }
}
