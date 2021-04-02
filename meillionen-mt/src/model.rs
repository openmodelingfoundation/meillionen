use std::collections::BTreeMap;
use std::ffi::OsString;
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::{env, fmt};

use itertools::Itertools;
use serde_derive::{Deserialize, Serialize};
use thiserror::Error;

use crate::arg::req::{SinkResource, SinkResourceMap, SourceResource, SourceResourceMap};
use crate::arg::ArgValidatorType;
use std::fmt::Formatter;

#[derive(Debug, Error, Deserialize, Serialize)]
pub enum FuncRequestSchemaError {
    #[error("missing sinks {0:?}")]
    MissingSinks(Vec<String>),
    #[error("missing sources {0:?}")]
    MissingSources(Vec<String>),
}

#[derive(Debug, Error)]
pub enum FuncCallError {
    Schema(FuncRequestSchemaError),
    IO(std::io::Error),
}

impl fmt::Display for FuncCallError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use FuncCallError::*;
        match &self {
            Schema(ref schema) => write!(f, "{:?}", schema),
            IO(ref io) => io.fmt(f),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResourceSchema {
    description: String,
    data_type: Arc<ArgValidatorType>,
}

impl ResourceSchema {
    pub fn new(description: String, data_type: Arc<ArgValidatorType>) -> Self {
        Self {
            description,
            data_type,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FuncInterface {
    name: String,
    sources: BTreeMap<String, Arc<ResourceSchema>>,
    sinks: BTreeMap<String, Arc<ResourceSchema>>,
}

impl FuncInterface {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            sources: BTreeMap::new(),
            sinks: BTreeMap::new(),
        }
    }

    pub fn set_source(&mut self, s: String, arg: Arc<ResourceSchema>) {
        self.sources.insert(s, arg);
    }

    pub fn set_sink(&mut self, s: String, arg: Arc<ResourceSchema>) {
        self.sinks.insert(s, arg);
    }

    pub fn get_sink(&self, s: &str) -> Option<Arc<ResourceSchema>> {
        self.sinks.get(s).cloned()
    }

    pub fn get_source(&self, s: &str) -> Option<Arc<ResourceSchema>> {
        self.sources.get(s).cloned()
    }

    pub fn to_cli(&self) -> FuncRequest {
        let _cli_option_data = self
            .sources
            .iter()
            .map(|(s, _a)| format!("source:{}", s))
            .chain(self.sinks.iter().map(|(s, _a)| format!("sink:{}", s)))
            .collect_vec();
        let mut app = clap::App::new(self.name.as_str()).subcommand(
            clap::SubCommand::with_name("interface").about("json describing the model interface"),
        );
        let run = clap::SubCommand::with_name("run").about("run the model");
        app = app.subcommand(run);
        let matches = app.get_matches_from(
            vec![OsString::from(self.name.as_str())]
                .into_iter()
                .chain(env::args_os().dropping(2)),
        );
        if matches.subcommand_matches("interface").is_some() {
            serde_json::to_writer(std::io::stdout(), self)
                .expect("failed to serialize model interface");
            std::process::exit(0);
        } else if matches.subcommand_matches("run").is_some() {
            let fr: FuncRequest =
                serde_json::from_reader(std::io::stdin()).expect("deserialize of func call failed");
            match self.validate(&fr) {
                Some(errors) => {
                    serde_json::to_writer(std::io::stderr(), &errors)
                        .expect("failed to write missing sinks and sources to stderr");
                    std::process::exit(1);
                }
                None => fr,
            }
        } else {
            std::process::exit(1);
        }
    }

    fn validate(&self, fr: &FuncRequest) -> Option<FuncRequestSchemaError> {
        let sinks = &self.sinks;
        let sources = &self.sources;
        let missing_sinks = fr
            .sinks
            .keys()
            .filter(|s| !sinks.contains_key(s.as_str()))
            .map(|s| s.to_string())
            .collect_vec();
        if !missing_sinks.is_empty() {
            return Some(FuncRequestSchemaError::MissingSinks(missing_sinks));
        }
        let missing_sources = fr
            .sources
            .keys()
            .filter(|s| !sources.contains_key(s.as_str()))
            .map(|s| s.to_string())
            .collect_vec();
        if !missing_sources.is_empty() {
            return Some(FuncRequestSchemaError::MissingSources(missing_sources));
        }
        None
    }

    pub fn build_request(
        &self,
        sinks: SinkResourceMap,
        sources: SourceResourceMap,
    ) -> Result<FuncRequest, FuncRequestSchemaError> {
        let fr = FuncRequest { sinks, sources };
        match self.validate(&fr) {
            Some(errors) => Err(errors),
            None => Ok(fr),
        }
    }

    pub fn call_cli(&self, program_path: &str, fc: &FuncRequest) -> Result<Output, FuncCallError> {
        let mut cmd = Command::new(program_path)
            .arg("run")
            .stdin(Stdio::piped())
            .stderr(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap_or_else(|_| panic!("could not spawn process {}", program_path));

        let stdin = cmd.stdin.take().expect("could not open stdin");
        serde_json::to_writer(stdin, fc).expect("could not write request to stdin");

        cmd.wait_with_output().map_err(FuncCallError::IO)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FuncRequest {
    sinks: SinkResourceMap,
    sources: SourceResourceMap,
}

impl FuncRequest {
    pub fn new() -> Self {
        Self {
            sinks: SinkResourceMap::new(),
            sources: SourceResourceMap::new(),
        }
    }

    pub fn get_source(&self, s: &str) -> Option<SourceResource> {
        self.sources.get(s).cloned()
    }

    pub fn get_sink(&self, s: &str) -> Option<SinkResource> {
        self.sinks.get(s).cloned()
    }

    pub fn set_source(&mut self, s: &str, sr: &SourceResource) {
        self.sources.insert(s.to_string(), sr.clone());
    }

    pub fn set_sink(&mut self, s: &str, si: &SinkResource) {
        self.sinks.insert(s.to_string(), si.clone());
    }
}
