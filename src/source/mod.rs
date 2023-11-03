use gasket::{messaging::SendPort, runtime::Tether};
use serde::Deserialize;

use crate::framework::*;

pub mod utxorpc;

pub enum Bootstrapper {
    UtxoRPC(utxorpc::Stage),
}

impl StageBootstrapper<ChainEvent, ChainEvent> for Bootstrapper {
    fn connect_input(&mut self, _: InputAdapter<ChainEvent>) {
        panic!("attempted to use source stage as receiver");
    }

    fn connect_output(&mut self, adapter: OutputAdapter<ChainEvent>) {
        match self {
            Bootstrapper::UtxoRPC(p) => p.output.connect(adapter),
        }
    }

    fn spawn(self, policy: gasket::runtime::Policy) -> Tether {
        match self {
            Bootstrapper::UtxoRPC(x) => gasket::runtime::spawn_stage(x, policy),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    UtxoRPC(utxorpc::Config),
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Bootstrapper, Error> {
        match self {
            Config::UtxoRPC(c) => Ok(Bootstrapper::UtxoRPC(c.bootstrapper(ctx)?)),
        }
    }
}
