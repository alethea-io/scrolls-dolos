use gasket::{
    messaging::{RecvPort, SendPort},
    runtime::Tether,
};
use serde::Deserialize;

use crate::framework::*;

pub mod rust;

pub enum Bootstrapper {
    Rust(rust::Stage),
}

impl StageBootstrapper<ChainEvent, StorageEvent> for Bootstrapper {
    fn connect_input(&mut self, adapter: InputAdapter<ChainEvent>) {
        match self {
            Bootstrapper::Rust(p) => p.input.connect(adapter),
        }
    }

    fn connect_output(&mut self, adapter: OutputAdapter<StorageEvent>) {
        match self {
            Bootstrapper::Rust(p) => p.output.connect(adapter),
        }
    }

    fn spawn(self, policy: gasket::runtime::Policy) -> Tether {
        match self {
            Bootstrapper::Rust(x) => gasket::runtime::spawn_stage(x, policy),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    Rust(rust::Config),
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Bootstrapper, Error> {
        match self {
            Config::Rust(c) => Ok(Bootstrapper::Rust(c.bootstrapper(ctx)?)),
        }
    }
}
