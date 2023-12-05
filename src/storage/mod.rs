use gasket::{messaging::RecvPort, runtime::Tether};
use serde::Deserialize;

use crate::framework::*;

mod postgres;
mod redis;

pub enum Bootstrapper {
    Redis(redis::Stage),
    Postgres(postgres::Stage),
}

impl StageBootstrapper<StorageEvent, StorageEvent> for Bootstrapper {
    fn connect_input(&mut self, adapter: InputAdapter<StorageEvent>) {
        match self {
            Bootstrapper::Redis(p) => p.input.connect(adapter),
            Bootstrapper::Postgres(p) => p.input.connect(adapter),
        }
    }

    fn connect_output(&mut self, _: OutputAdapter<StorageEvent>) {
        panic!("attempted to use sink stage as sender");
    }

    fn spawn(self, policy: gasket::runtime::Policy) -> Tether {
        match self {
            Bootstrapper::Redis(x) => gasket::runtime::spawn_stage(x, policy),
            Bootstrapper::Postgres(x) => gasket::runtime::spawn_stage(x, policy),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum Config {
    Redis(redis::Config),
    Postgres(postgres::Config),
}

impl Config {
    pub fn bootstrapper(self, ctx: &Context) -> Result<Bootstrapper, Error> {
        match self {
            Config::Redis(c) => Ok(Bootstrapper::Redis(c.bootstrapper(ctx)?)),
            Config::Postgres(c) => Ok(Bootstrapper::Postgres(c.bootstrapper(ctx)?)),
        }
    }
}
