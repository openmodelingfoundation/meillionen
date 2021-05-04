use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::{env, fmt};

use itertools::Itertools;
use serde_derive::{Deserialize, Serialize};
use thiserror::Error;

use std::fmt::Formatter;
use arrow::array::{BinaryBuilder, StringBuilder};
use arrow::record_batch::RecordBatch;
use arrow::ipc::reader::StreamReader;
use stable_eyre;
use stable_eyre::eyre::{WrapErr, ContextCompat};
use arrow::ipc::writer::StreamWriter;

#[derive(Debug, Error, Deserialize, Serialize)]
pub enum FuncRequestSchemaError {
    #[error("missing sinks {0:?}")]
    MissingSinks(Vec<String>),
    #[error("missing sources {0:?}")]
    MissingSources(Vec<String>),
}

#[derive(Debug, Error)]
pub enum MeillionenError {
    Schema(FuncRequestSchemaError),
    IO(std::io::Error),
    JsonError(serde_json::Error),
}

impl fmt::Display for MeillionenError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use MeillionenError::*;
        match &self {
            Schema(ref schema) => write!(f, "{:?}", schema),
            IO(ref io) => io.fmt(f),
            JsonError(ref je) => je.fmt(f),
        }
    }
}

impl From<serde_json::Error> for MeillionenError {
    fn from(e: serde_json::Error) -> Self {
        MeillionenError::JsonError(e)
    }
}

impl From<std::io::Error> for MeillionenError {
    fn from(e: std::io::Error) -> Self {
        MeillionenError::IO(e)
    }
}

impl From<FuncRequestSchemaError> for MeillionenError {
    fn from(e: FuncRequestSchemaError) -> Self {
        MeillionenError::Schema(e)
    }
}

#[derive(Debug)]
pub struct ResourceBuilder {
    program_name: String,
    field: StringBuilder,
    name: StringBuilder,
    resource: StringBuilder,
    payload: BinaryBuilder
}

impl ResourceBuilder {
    pub fn new(program_name: &str) -> Self {
        Self {
            program_name: program_name.to_string(),
            field: StringBuilder::new(4),
            name: StringBuilder::new(4),
            resource: StringBuilder::new(4),
            payload: BinaryBuilder::new(4),
        }
    }

    pub fn add(&mut self, field: &str, name: &str, resource: &str, payload: &[u8]) -> arrow::error::Result<()> {
        self.field.append_value(field)?;
        self.name.append_value(name)?;
        self.resource.append_value(resource)?;
        self.payload.append_value(payload)?;
        Ok(())
    }

    fn build_arrow_schema(&self) -> Arc<arrow::datatypes::Schema> {
        use arrow::datatypes::{DataType, Field, Schema};
        let mut metadata = HashMap::new();
        metadata.insert("meillionen-name".to_string(), self.program_name.clone());
        let schema = Schema::new_with_metadata(vec![
            Field::new("field", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("resource", DataType::Utf8, false),
            Field::new("payload", DataType::Binary, false),
        ], metadata);
        Arc::new(schema)
    }

    pub fn extract_to_recordbatch(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            self.build_arrow_schema(),
            vec![
                Arc::new(self.field.finish()),
                Arc::new(self.name.finish()),
                Arc::new(self.resource.finish()),
                Arc::new(self.payload.finish())
            ]
        ).expect("schema and columns to match")
    }
}

pub fn client_call_cli(
    program_path: &str,
    rb: &RecordBatch,
) -> stable_eyre::Result<Output> {
    let mut cmd = Command::new(program_path)
        .arg("run")
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap_or_else(|_| panic!("could not spawn process {}", program_path));

    let stdin = cmd.stdin.take().wrap_err("could not open stdin")?;
    let mut sw = StreamWriter::try_new(stdin, rb.schema().as_ref()).wrap_err(format!("could not create input stream writer for {}", program_path))?;
    sw.write(rb).wrap_err("could not write record batch to input stream")?;
    sw.finish().wrap_err("failed to finish record batch stream to stdout")?;
    cmd.wait_with_output().wrap_err_with(|| format!("waiting for program {} to finish failed", program_path))
}

pub fn client_create_interface_from_cli(path: &str) -> stable_eyre::Result<RecordBatch> {
    let child = Command::new(path)
        .arg("interface")
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .wrap_err_with(|| format!("calling cli program {} failed", path))?
        .wait_with_output()
        .wrap_err_with(|| format!("executing cli program {} failed", path))?;
    let mut sr = StreamReader::try_new(child.stdout.as_slice())
        .wrap_err_with(|| format!("could not read stdout from program {}", path))?;
    let res = sr.next()
        .ok_or(stable_eyre::eyre::eyre!("record batch stream for program {} empty", path))?;
    if let Ok(rb) = res {
        Ok(rb)
    } else {
        Err(stable_eyre::eyre::eyre!("could not read record batch for program {}", path))
    }
}

pub fn server_respond_from_cli(name: &str, interface: &RecordBatch) -> stable_eyre::Result<RecordBatch> {
    let mut app = clap::App::new(name).subcommand(
        clap::SubCommand::with_name("interface").about("json describing the model interface"),
    );
    let run = clap::SubCommand::with_name("run").about("run the model");
    app = app.subcommand(run);
    let matches = app.get_matches_from(
        vec![OsString::from(name)]
            .into_iter()
            .chain(env::args_os().dropping(2)),
    );
    if matches.subcommand_matches("interface").is_some() {
        let mut sw =
            StreamWriter::try_new(std::io::stdout(), interface.schema().as_ref())
                .wrap_err("failed to open stdout stream")?;
        sw.write(interface)
            .wrap_err("failed to write interface record batch to stdout")?;
        sw.finish().wrap_err("failed to finish record batch stream to stdout")?;
        std::process::exit(0);
    } else if matches.subcommand_matches("run").is_some() {
        let mut sr = StreamReader::try_new(std::io::stdin())
            .wrap_err("could not read input of run command")?;
        let res = sr.next()
            .ok_or(stable_eyre::eyre::eyre!("could not read one record batch from stdin"))?;
        if let Ok(rb) = res {
            Ok(rb)
        } else {
            Err(stable_eyre::eyre::eyre!("error deserializing record batch"))
        }
    } else {
        std::process::exit(1);
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SerializedResource {
    dtype: String,
    payload: Vec<u8>
}
pub type ResourceMap = BTreeMap<String, Arc<SerializedResource>>;
