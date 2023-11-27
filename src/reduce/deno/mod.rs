use deno_runtime::deno_core;
use deno_runtime::deno_core::op2;
use deno_runtime::deno_core::ModuleSpecifier;
use deno_runtime::deno_core::OpState;
use deno_runtime::permissions::PermissionsContainer;
use deno_runtime::worker::MainWorker as DenoWorker;
use deno_runtime::worker::WorkerOptions;
use gasket::framework::*;
use serde::Deserialize;
use std::path::{Path, PathBuf};

use crate::framework::*;

deno_core::extension!(deno_filter, ops = [op_pop_record, op_put_record]);

#[op2]
#[serde]
pub fn op_pop_record(state: &mut OpState) -> Result<serde_json::Value, deno_core::error::AnyError> {
    let r: Record = state.take();
    let j = serde_json::Value::from(r);
    Ok(j)
}

#[op2]
pub fn op_put_record(
    state: &mut OpState,
    #[serde] value: serde_json::Value,
) -> Result<(), deno_core::error::AnyError> {
    match value {
        serde_json::Value::Null => (),
        _ => state.put(value),
    };

    Ok(())
}

async fn setup_deno(reducers: &[PathBuf]) -> DenoWorker {
    let empty_module = deno_core::ModuleSpecifier::parse("data:text/javascript;base64,").unwrap();

    let mut worker = DenoWorker::bootstrap_from_options(
        empty_module,
        PermissionsContainer::allow_all(),
        WorkerOptions {
            extensions: vec![deno_filter::init_ops()],
            ..Default::default()
        },
    );

    for reducer in reducers {
        let file_specifier = format!("file://{}", reducer.display());
        let file_stem = reducer
            .file_stem()
            .and_then(|os_str| os_str.to_str())
            .unwrap();

        let module_code = deno_core::FastString::from(std::fs::read_to_string(reducer).unwrap());
        let module_specifier = ModuleSpecifier::parse(&file_specifier).unwrap();

        worker
            .js_runtime
            .load_side_module(&module_specifier, Some(module_code))
            .await
            .unwrap();

        let runtime_code = deno_core::FastString::from(format!(
            r#"import("{}").then(({{ apply, undo }}) => {{globalThis["{}_apply"] = apply; globalThis["{}_undo"] = undo;}});"#,
            file_specifier, file_stem, file_stem
        ));

        let res = worker.execute_script("[runtime.js]", runtime_code);
        worker.run_event_loop(false).await.unwrap();
        res.unwrap();
    }

    worker
}

pub struct Worker {
    runtime: DenoWorker,
    modules: Vec<ModuleSpecifier>,
}

impl Worker {
    async fn reduce(
        &mut self,
        module: ModuleSpecifier,
        method: &str,
        record: Record,
    ) -> Result<Option<serde_json::Value>, String> {
        let deno = &mut self.runtime;
        deno.js_runtime.op_state().borrow_mut().put(record);

        let file_stem = Path::new(module.path())
            .file_stem()
            .and_then(|os_str| os_str.to_str())
            .unwrap();

        let script = format!(
            r#"Deno[Deno.internal].core.ops.op_put_record({}_{}(Deno[Deno.internal].core.ops.op_pop_record()));"#,
            file_stem, method
        );

        let script = deno_core::FastString::from(script);
        let res = deno.execute_script("<anon>", script);

        deno.run_event_loop(false).await.unwrap();

        res.unwrap();

        let output = deno.js_runtime.op_state().borrow_mut().try_take();

        Ok(output)
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let reducers = &stage.reducers;
        let modules: Vec<ModuleSpecifier> = reducers
            .iter()
            .map(|reducer| {
                ModuleSpecifier::parse(&format!("file://{}", reducer.display())).unwrap()
            })
            .collect();

        // Setup Deno runtime and load modules
        let runtime = setup_deno(reducers).await;

        Ok(Self { runtime, modules })
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<ChainEvent>, WorkerError> {
        let msg = stage.input.recv().await.or_panic()?;

        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &ChainEvent, stage: &mut Stage) -> Result<(), WorkerError> {
        let modules = self.modules.clone();

        match unit {
            ChainEvent::Apply(_, record) | ChainEvent::Undo(_, record) => {
                let block = if let Record::ParsedBlock(block) = record {
                    block
                } else {
                    return Err(WorkerError::Panic);
                };

                let method = match unit {
                    ChainEvent::Apply(_, _) => "apply",
                    ChainEvent::Undo(_, _) => "undo",
                    _ => unreachable!(), // We've already matched these variants
                };

                stage
                    .output
                    .send(gasket::messaging::Message::from(StorageEvent::CRDT(
                        CRDTCommand::block_starting(block),
                    )))
                    .await
                    .or_panic()?;

                for module in modules {
                    let reduced = self.reduce(module, method, record.clone()).await.unwrap();

                    if let Some(reduced) = reduced {
                        match reduced {
                            serde_json::Value::Array(items) => {
                                for item in items {
                                    stage
                                        .output
                                        .send(gasket::messaging::Message::from(StorageEvent::CRDT(
                                            CRDTCommand::from_json(&item).unwrap(),
                                        )))
                                        .await
                                        .or_panic()?;
                                }
                            }
                            _ => {
                                stage
                                    .output
                                    .send(gasket::messaging::Message::from(StorageEvent::CRDT(
                                        CRDTCommand::from_json(&reduced).unwrap(),
                                    )))
                                    .await
                                    .or_panic()?;
                            }
                        }

                        stage.ops_count.inc(1);
                    }
                }

                stage
                    .output
                    .send(gasket::messaging::Message::from(StorageEvent::CRDT(
                        CRDTCommand::block_finished(block),
                    )))
                    .await
                    .or_panic()?;
            }
            _ => panic!("Unhandled ChainEvent variant or Record type in execute"),
        };

        Ok(())
    }
}

#[derive(Stage)]
#[stage(name = "reduce-deno", unit = "ChainEvent", worker = "Worker")]
pub struct Stage {
    reducers: Vec<PathBuf>,

    pub input: ReduceInputPort,
    pub output: ReduceOutputPort,

    #[metric]
    ops_count: gasket::metrics::Counter,
}

#[derive(Deserialize)]
pub struct Config {
    reducers: Vec<String>,
}

impl Config {
    pub fn bootstrapper(self, _ctx: &Context) -> Result<Stage, Error> {
        let stage = Stage {
            reducers: self.reducers.iter().map(PathBuf::from).collect(),
            input: Default::default(),
            output: Default::default(),
            ops_count: Default::default(),
        };

        Ok(stage)
    }
}
