use std::collections::{BTreeMap, HashMap};
use std::env;
use std::ffi::OsString;
use std::process::{Command, Stdio};


use itertools::Itertools;
use serde_derive::{Deserialize, Serialize};

use crate::arg::ArgResource;

use crate::arg::ArgValidatorType;
use std::sync::Arc;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ArgDescription {
    description: String,
    data_type: Arc<ArgValidatorType>,
}

impl ArgDescription {
    pub fn new(description: String, data_type: Arc<ArgValidatorType>) -> Self {
        Self {
            description,
            data_type
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FuncInterface {
    name: String,
    sources: BTreeMap<String, Arc<ArgDescription>>,
    sinks: BTreeMap<String, Arc<ArgDescription>>,
}

impl FuncInterface {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            sources: BTreeMap::new(),
            sinks: BTreeMap::new(),
        }
    }

    pub fn set_source(&mut self, s: String, arg: Arc<ArgDescription>) {
        self.sources.insert(s, arg);
    }

    pub fn set_sink(&mut self, s: String, arg: Arc<ArgDescription>) {
        self.sinks.insert(s, arg);
    }

    pub fn get_sink(&self, s: &str) -> Option<Arc<ArgDescription>> {
        self.sinks.get(s).cloned()
    }

    pub fn get_source(&self, s: &str) -> Option<Arc<ArgDescription>> {
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
            let fc: FuncCall =
                serde_json::from_reader(std::io::stdin()).expect("deserialize of func call failed");
            match fc.validate(self) {
                Err(errors) => {
                    serde_json::to_writer(std::io::stderr(), &errors)
                        .expect("failed to write missing sinks and sources to stderr");
                    std::process::exit(1);
                }
                Ok(data) => data,
            }
        } else {
            std::process::exit(1);
        }
    }

    pub fn call_cli(
        &self,
        program_path: &str,
        fc: &FuncRequest,
    ) -> std::io::Result<std::process::ExitStatus> {
        let mut cmd = Command::new(program_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap_or_else(|_| panic!("could not spawn process {}", program_path));

        let stdin = cmd.stdin.take().expect("could not open stdin");
        serde_json::to_writer(stdin, fc).expect("could not write request to stdin");

        cmd.wait()
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FuncRequest {
    sinks: HashMap<String, Arc<ArgResource>>,
    sources: HashMap<String, Arc<ArgResource>>,
}

impl FuncRequest {
    pub fn new() -> Self {
        Self {
            sinks: HashMap::new(),
            sources: HashMap::new(),
        }
    }

    pub fn get_source(&self, s: &str) -> Option<Arc<ArgResource>> {
        self.sources.get(s).cloned()
    }

    pub fn get_sink(&self, s: &str) -> Option<Arc<ArgResource>> {
        self.sinks.get(s).cloned()
    }

    pub fn set_source(&mut self, s: &str, sr: Arc<ArgResource>) {
        self.sources.insert(s.to_string(), sr);
    }

    pub fn set_sink(&mut self, s: &str, si: Arc<ArgResource>) {
        self.sinks.insert(s.to_string(), si);
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FuncCall {
    sources: BTreeMap<String, Arc<ArgResource>>,
    sinks: BTreeMap<String, Arc<ArgResource>>,
}

impl FuncCall {
    pub fn new() -> Self {
        Self {
            sources: BTreeMap::new(),
            sinks: BTreeMap::new(),
        }
    }

    pub fn add_source(&mut self, name: &str, s: Arc<ArgResource>) {
        self.sources.insert(name.to_string(), s);
    }

    pub fn add_sink(&mut self, name: &str, s: Arc<ArgResource>) {
        self.sinks.insert(name.to_string(), s);
    }

    pub fn get_source(&self, s: &str) -> Option<Arc<ArgResource>> {
        self.sources.get(s).cloned()
    }

    pub fn get_sink(&self, s: &str) -> Option<Arc<ArgResource>> {
        self.sinks.get(s).cloned()
    }

    pub fn validate(
        &self,
        fi: &FuncInterface,
    ) -> Result<FuncRequest, HashMap<String, Vec<String>>> {
        let sinks = &fi.sinks;
        let sources = &fi.sources;
        let missing_sinks = self
            .sources
            .keys()
            .filter(|s| !sinks.contains_key(s.as_str()))
            .map(|s| s.to_string())
            .collect_vec();
        let missing_sources = self
            .sinks
            .keys()
            .filter(|s| !sources.contains_key(s.as_str()))
            .map(|s| s.to_string())
            .collect_vec();
        if missing_sinks.is_empty() && missing_sources.is_empty() {
            let mut hm = HashMap::new();
            hm.insert("sinks".to_string(), missing_sinks);
            hm.insert("sources".to_string(), missing_sources);
            Err(hm)
        } else {
            Ok(FuncRequest {
                sinks: self
                    .sinks
                    .iter()
                    .filter(|(k, _v)| sinks.contains_key(k.as_str()))
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect(),
                sources: self
                    .sources
                    .iter()
                    .filter(|(k, _v)| sources.contains_key(k.as_str()))
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect(),
            })
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use serde_json;
//     use super::*;
//
//     #[test]
//     fn datatype_serialize() {
//         let dt = DataType::Other;
//         assert_eq!(serde_json::to_string(&dt).unwrap(), "\"Other\"");
//
//         let mut d = BTreeMap::new();
//         d.insert("max_temp".to_string(),ColType::F64);
//         let dt = DataType::Table(TableSchema(d));
//         assert_eq!(serde_json::to_string(&dt).unwrap(), r#"{"Table":{"max_temp":"F64"}}"#);
//
//         let mut f = BTreeMap::new();
//         f.insert(
//             "daily".to_string(),
//             Arg {
//                 description: "daily data".to_string(),
//                 datatype: dt,
//             });
//         let fi = FuncInterface {
//             name: "simplecrop".to_string(),
//             sinks: BTreeMap::new(),
//             sources: f
//         };
//         assert_eq!(
//         serde_json::to_string(&fi).unwrap(),
//         r#"{"name":"simplecrop","sources":{"daily":{"description":"daily data","datatype":{"Table":{"max_temp":"F64"}}}},"sinks":{}}"#)
//     }
// }
