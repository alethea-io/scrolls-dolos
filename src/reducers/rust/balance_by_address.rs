use gasket::framework::AsWorkError;
use pallas::ledger::addresses::Address;
use serde::Deserialize;
use utxorpc::proto::cardano::v1::{Block, TxOutput};

use crate::framework::{CRDTCommand, ReducerOutputPort, StorageEvent};

#[derive(Clone, Deserialize)]
pub struct Config {
    pub key_prefix: Option<String>,
}

impl Config {
    pub fn plugin(self) -> super::Reducer {
        let reducer = Reducer { config: self };

        super::Reducer::BalanceByAddress(reducer)
    }
}

#[derive(Clone)]
pub struct Reducer {
    config: Config,
}

enum TxOperation {
    Consumed,
    Produced,
}

impl Reducer {
    pub async fn apply(
        &mut self,
        block: Block,
        output: &mut ReducerOutputPort,
    ) -> Result<(), gasket::framework::WorkerError> {
        for tx in &block.body.as_ref().unwrap().tx {
            for txi in &tx.inputs {
                if let Some(as_output) = &txi.as_output {
                    self.process_txo(as_output.clone(), TxOperation::Consumed, output)
                        .await
                        .or_panic()?;
                }
            }

            for txo in &tx.outputs {
                self.process_txo(txo.clone(), TxOperation::Produced, output)
                    .await
                    .or_panic()?;
            }
        }

        Ok(())
    }

    pub async fn undo(
        &mut self,
        block: Block,
        output: &mut ReducerOutputPort,
    ) -> Result<(), gasket::framework::WorkerError> {
        for tx in &block.body.as_ref().unwrap().tx {
            for txi in &tx.inputs {
                if let Some(as_output) = &txi.as_output {
                    self.process_txo(as_output.clone(), TxOperation::Produced, output)
                        .await
                        .or_panic()?;
                }
            }

            for txo in &tx.outputs {
                self.process_txo(txo.clone(), TxOperation::Consumed, output)
                    .await
                    .or_panic()?;
            }
        }

        Ok(())
    }

    fn key_prefix(&self) -> String {
        self.config
            .key_prefix
            .clone()
            .unwrap_or_else(|| "balance_by_address".to_string())
    }

    async fn process_txo(
        &mut self,
        txo: TxOutput,
        operation: TxOperation,
        output: &mut ReducerOutputPort,
    ) -> Result<(), gasket::framework::WorkerError> {
        let address = Address::from_bytes(&txo.address).unwrap();

        let key = format!("{}.{}", self.key_prefix(), address.to_string());

        let value = match operation {
            TxOperation::Consumed => -1 * txo.coin as i64,
            TxOperation::Produced => txo.coin as i64,
        };

        let crdt = CRDTCommand::PNCounter(key, value);
        output
            .send(gasket::messaging::Message::from(StorageEvent::CRDT(crdt)))
            .await
            .or_panic()?;

        Ok(())
    }
}
