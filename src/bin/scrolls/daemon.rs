use gasket::runtime::Tether;
use serde::Deserialize;
use std::{collections::VecDeque, time::Duration};
use tracing::{info, warn};

use scrolls::{framework::*, reducers, source, storage};

use crate::console;

#[derive(Deserialize)]
pub struct ConfigRoot {
    intersect: IntersectConfig,
    source: source::Config,
    reducers: Vec<reducers::Config>,
    storage: storage::Config,
    chain: Option<ChainConfig>,
    finalize: Option<FinalizeConfig>,
    retries: Option<gasket::retries::Policy>,
}

impl ConfigRoot {
    pub fn new(explicit_file: &Option<std::path::PathBuf>) -> Result<Self, config::ConfigError> {
        let mut s = config::Config::builder();

        // our base config will always be in /etc/scrolls
        s = s.add_source(config::File::with_name("/etc/scrolls/daemon.toml").required(false));

        // but we can override it by having a file in the working dir
        s = s.add_source(config::File::with_name("daemon.toml").required(false));

        // if an explicit file was passed, then we load it as mandatory
        if let Some(explicit) = explicit_file.as_ref().and_then(|x| x.to_str()) {
            s = s.add_source(config::File::with_name(explicit).required(true));
        }

        // finally, we use env vars to make some last-step overrides
        s = s.add_source(config::Environment::with_prefix("SCROLLS").separator("_"));

        s.build()?.try_deserialize()
    }
}

struct Runtime {
    source: Tether,
    reducers: Vec<Tether>,
    storage: Tether,
}

impl Runtime {
    fn all_tethers(&self) -> impl Iterator<Item = &Tether> {
        std::iter::once(&self.source)
            .chain(self.reducers.iter())
            .chain(std::iter::once(&self.storage))
    }

    fn should_stop(&self) -> bool {
        self.all_tethers().any(|tether| match tether.check_state() {
            gasket::runtime::TetherState::Alive(x) => {
                matches!(x, gasket::runtime::StagePhase::Ended)
            }
            _ => true,
        })
    }

    fn shutdown(&self) {
        for tether in self.all_tethers() {
            let state = tether.check_state();
            warn!("dismissing stage: {} with state {:?}", tether.name(), state);
            tether.dismiss_stage().expect("stage stops");

            // Can't join the stage because there's a risk of deadlock, usually
            // because a stage gets stuck sending into a port which depends on a
            // different stage not yet dismissed. The solution is to either
            // create a DAG of dependencies and dismiss in the
            // correct order, or implement a 2-phase teardown where
            // ports are disconnected and flushed before joining the
            // stage.

            //tether.join_stage();
        }
    }
}

fn define_gasket_policy(config: Option<&gasket::retries::Policy>) -> gasket::runtime::Policy {
    let default_policy = gasket::retries::Policy {
        max_retries: 20,
        backoff_unit: Duration::from_secs(1),
        backoff_factor: 2,
        max_backoff: Duration::from_secs(60),
        dismissible: false,
    };

    gasket::runtime::Policy {
        tick_timeout: None,
        bootstrap_retry: config.cloned().unwrap_or(default_policy.clone()),
        work_retry: config.cloned().unwrap_or(default_policy.clone()),
        teardown_retry: config.cloned().unwrap_or(default_policy.clone()),
    }
}

fn chain_stages<'a>(
    source: &'a mut dyn StageBootstrapper<ChainEvent, ChainEvent>,
    reducers: Vec<&'a mut dyn StageBootstrapper<ChainEvent, StorageEvent>>,
    storage: &'a mut dyn StageBootstrapper<StorageEvent, StorageEvent>,
) {
    let (to_reducers, from_source) = gasket::messaging::tokio::broadcast_channel(1000);
    let (to_storage, from_reducers) = gasket::messaging::tokio::mpsc_channel(1000);

    for reducer in reducers {
        source.connect_output(to_reducers.clone());
        reducer.connect_input(from_source.clone());
        reducer.connect_output(to_storage.clone());
    }

    storage.connect_input(from_reducers);
}

fn bootstrap(
    mut source: source::Bootstrapper,
    mut reducers: Vec<reducers::Bootstrapper>,
    mut storage: storage::Bootstrapper,
    policy: gasket::runtime::Policy,
) -> Result<Runtime, Error> {
    chain_stages(
        &mut source,
        reducers
            .iter_mut()
            .map(|x| x as &mut dyn StageBootstrapper<ChainEvent, StorageEvent>)
            .collect::<Vec<_>>(),
        &mut storage,
    );

    let runtime = Runtime {
        source: source.spawn(policy.clone()),
        reducers: reducers
            .into_iter()
            .map(|x| x.spawn(policy.clone()))
            .collect(),
        storage: storage.spawn(policy),
    };

    Ok(runtime)
}

pub fn run(args: &Args) -> Result<(), Error> {
    console::initialize(&args.console);

    let config = ConfigRoot::new(&args.config).map_err(Error::config)?;

    let chain = config.chain.unwrap_or_default();
    let intersect = config.intersect;
    let finalize = config.finalize;
    let current_dir = std::env::current_dir().unwrap();

    // TODO: load from persistence mechanism
    let cursor = Cursor::new(VecDeque::new());

    let ctx = Context {
        chain,
        intersect,
        finalize,
        cursor,
        current_dir,
    };

    let source = config.source.bootstrapper(&ctx)?;

    let reducers = config
        .reducers
        .into_iter()
        .map(|x| x.bootstrapper(&ctx))
        .collect::<Result<_, _>>()?;

    let storage = config.storage.bootstrapper(&ctx)?;

    let retries = define_gasket_policy(config.retries.as_ref());
    let runtime = bootstrap(source, reducers, storage, retries)?;

    info!("scrolls is running...");

    while !runtime.should_stop() {
        console::refresh(&args.console, runtime.all_tethers());
        std::thread::sleep(Duration::from_millis(1500));
    }

    info!("Scrolls is stopping...");
    runtime.shutdown();

    Ok(())
}

#[derive(clap::Args)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    #[clap(long, value_parser)]
    //#[clap(description = "config file to load by the daemon")]
    config: Option<std::path::PathBuf>,

    #[clap(long, value_parser)]
    //#[clap(description = "type of progress to display")],
    console: Option<console::Mode>,
}
