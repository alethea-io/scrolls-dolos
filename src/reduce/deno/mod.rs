use deno_core::error::AnyError;
use deno_runtime::deno_core;
use deno_runtime::deno_core::op2;
use deno_runtime::deno_core::ModuleSpecifier;
use deno_runtime::deno_core::OpState;
use deno_runtime::permissions::PermissionsContainer;
use deno_runtime::worker::MainWorker as DenoWorker;
use deno_runtime::worker::WorkerOptions;
use gasket::framework::*;
use serde::Deserialize;
use std::path::PathBuf;
use std::rc::Rc;

use crate::framework::*;

deno_core::extension!(
    deno_filter,
    ops = [
        op_pop_record,
        op_put_record,
        op_read_file,
        op_write_file,
        op_remove_file
    ]
);

#[op2]
#[serde]
pub fn op_pop_record(state: &mut OpState) -> Result<serde_json::Value, AnyError> {
    let r: Record = state.take();
    let j = serde_json::Value::from(r);
    Ok(j)
}

#[op2]
pub fn op_put_record(
    state: &mut OpState,
    #[serde] value: serde_json::Value,
) -> Result<(), AnyError> {
    match value {
        serde_json::Value::Null => (),
        _ => state.put(value),
    };

    Ok(())
}

#[op2(async)]
#[string]
async fn op_read_file(#[string] path: String) -> Result<String, AnyError> {
    let contents = tokio::fs::read_to_string(path).await?;
    Ok(contents)
}

#[op2(async)]
async fn op_write_file(#[string] path: String, #[string] contents: String) -> Result<(), AnyError> {
    tokio::fs::write(path, contents).await?;
    Ok(())
}

#[op2(fast)]
fn op_remove_file(#[string] path: String) -> Result<(), AnyError> {
    std::fs::remove_file(path)?;
    Ok(())
}

async fn setup_deno(main_module: &PathBuf) -> DenoWorker {
    let empty_module = deno_core::ModuleSpecifier::parse("data:text/javascript;base64,").unwrap();

    let mut deno = DenoWorker::bootstrap_from_options(
        empty_module,
        PermissionsContainer::allow_all(),
        WorkerOptions {
            module_loader: Rc::new(deno_core::FsModuleLoader),
            extensions: vec![deno_filter::init_ops()],
            ..Default::default()
        },
    );

    let module_code = deno_core::FastString::from(std::fs::read_to_string(main_module).unwrap());

    deno.js_runtime
        .load_side_module(
            &ModuleSpecifier::from_file_path(main_module).unwrap(),
            Some(module_code),
        )
        .await
        .unwrap();

    let runtime_js = format!(
        r#"
        import("file://{}").then(({{ apply, undo }}) => {{
            globalThis.scrolls = {{
                apply: apply,
                undo: undo,
            }}
        }});
        "#,
        main_module.clone().display().to_string()
    );

    let runtime_code = deno_core::FastString::from(runtime_js);

    let res = deno.execute_script("[scrolls:runtime.js]", runtime_code);
    deno.run_event_loop(false).await.unwrap();
    res.unwrap();

    deno
}

pub struct Worker {
    runtime: DenoWorker,
}

impl Worker {
    async fn reduce(
        &mut self,
        method: &str,
        record: Record,
    ) -> Result<Option<serde_json::Value>, String> {
        let deno = &mut self.runtime;

        deno.js_runtime.op_state().borrow_mut().put(record);

        let script = format!(
            r#"Deno[Deno.internal].core.ops.op_put_record(scrolls.{}(Deno[Deno.internal].core.ops.op_pop_record()));"#,
            method
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
        let runtime = setup_deno(&stage.main_module).await;

        Ok(Self { runtime })
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
            ChainEvent::Apply(_, record) | ChainEvent::Undo(_, record) => {
                let block = if let Record::ParsedBlock(block) = record {
                    block
                } else {
                    return Err(WorkerError::Panic);
                };

                let method = match unit {
                    ChainEvent::Apply(_, _) => "apply",
                    ChainEvent::Undo(_, _) => "undo",
                    _ => unreachable!(),
                };

                let event = match stage.storage_event.as_str() {
                    "CRDT" => StorageEvent::CRDT(CRDTCommand::block_starting(block)),
                    "RDBMS" => StorageEvent::RDBMS(RDBMSCommand::block_starting(block)),
                    _ => return Err(WorkerError::Panic),
                };

                stage
                    .output
                    .send(gasket::messaging::Message::from(event))
                    .await
                    .or_panic()?;

                let reduced = self.reduce(method, record.clone()).await.unwrap();

                if let Some(reduced) = reduced {
                    match reduced {
                        serde_json::Value::Array(items) => {
                            for item in items {
                                let event = match stage.storage_event.as_str() {
                                    "CRDT" => {
                                        StorageEvent::CRDT(CRDTCommand::from_json(&item).unwrap())
                                    }
                                    "RDBMS" => {
                                        StorageEvent::RDBMS(RDBMSCommand::from_json(&item).unwrap())
                                    }
                                    _ => return Err(WorkerError::Panic),
                                };

                                stage
                                    .output
                                    .send(gasket::messaging::Message::from(event))
                                    .await
                                    .or_panic()?;
                            }
                        }
                        _ => {
                            let event = match stage.storage_event.as_str() {
                                "CRDT" => {
                                    StorageEvent::CRDT(CRDTCommand::from_json(&reduced).unwrap())
                                }
                                "RDBMS" => {
                                    StorageEvent::RDBMS(RDBMSCommand::from_json(&reduced).unwrap())
                                }
                                _ => return Err(WorkerError::Panic),
                            };

                            stage
                                .output
                                .send(gasket::messaging::Message::from(event))
                                .await
                                .or_panic()?;
                        }
                    }

                    stage.ops_count.inc(1);
                }

                let event = match stage.storage_event.as_str() {
                    "CRDT" => StorageEvent::CRDT(CRDTCommand::block_finished(block)),
                    "RDBMS" => StorageEvent::RDBMS(RDBMSCommand::block_finished(block)),
                    _ => return Err(WorkerError::Panic),
                };

                stage
                    .output
                    .send(gasket::messaging::Message::from(event))
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
    main_module: PathBuf,
    storage_event: String,

    pub input: ReduceInputPort,
    pub output: ReduceOutputPort,

    #[metric]
    ops_count: gasket::metrics::Counter,
}

#[derive(Deserialize)]
pub struct Config {
    main_module: String,
    storage_event: String,
}

impl Config {
    pub fn bootstrapper(self, _ctx: &Context) -> Result<Stage, Error> {
        let stage = Stage {
            main_module: PathBuf::from(self.main_module),
            storage_event: self.storage_event,
            input: Default::default(),
            output: Default::default(),
            ops_count: Default::default(),
        };

        Ok(stage)
    }
}
