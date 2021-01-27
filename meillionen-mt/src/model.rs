use clap;
use serde_derive::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::ffi::{OsString, OsStr};
use core::fmt;
use itertools::Itertools;
use itertools::__std_iter::FromIterator;
use std::env;
use serde_json;
use std::process::{Command, Stdio};


#[derive(Deserialize, Serialize, Copy, Clone, Debug, PartialEq, Eq)]
pub enum ColType {
    F64,
    I64,
    Str
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TableSchema(BTreeMap<String, ColType>);

impl TableSchema {
    pub fn validate(&self, dfs: &TableSchema) -> Vec<String> {
        let mut errors = vec![];
        for (name, datatype) in self.0.iter() {
            if let Some(ref dt) = dfs.get_coltype(name) {
                if datatype != dt {
                    errors.push(format!("{}: expected {:?} but got {:?}", name, datatype, dt))
                }
            } else {
                errors.push(format!("{}: missing", name))
            }
        }
        errors
    }

    pub fn get_coltype(&self, s: &str) -> Option<ColType> {
        self.0.get(s).map(|ct| ct.clone())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum DataType {
    Table(TableSchema),
    Other
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum StoreRef {
    LocalPath(String),
    Inline(String)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Arg {
    description: String,
    datatype: DataType
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FuncInterface {
    name: String,
    sources: BTreeMap<String, Arg>,
    sinks: BTreeMap<String, Arg>
}

impl FuncInterface {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            sources: BTreeMap::new(),
            sinks: BTreeMap::new()
        }
    }

    pub fn add_source(&mut self, s: String, arg: &Arg) {
        self.sources.insert(s, arg.clone());
    }

    pub fn add_sink(&mut self, s: String, arg: &Arg) {
        self.sinks.insert(s, arg.clone());
    }

    pub fn get_sink(&self, s: &str) -> Option<Arg> { self.sinks.get(s).map(|a| a.clone()) }

    pub fn get_source(&self, s: &str) -> Option<Arg> { self.sources.get(s).map(|a| a.clone()) }

    pub fn to_cli(&self) -> FuncRequest {
        let cli_option_data= self.sources
            .iter().map(|(s, a)| format!("source:{}", s))
            .chain(self.sinks.iter().map(|(s,a)| format!("sink:{}", s)))
            .collect_vec();
        let mut app = clap::App::new(self.name.as_str())
            .subcommand(clap::SubCommand::with_name("interface")
                .about("json describing the model interface"));
        let mut run = clap::SubCommand::with_name("run")
            .about("run the model");
        app = app.subcommand(run);
        let matches = app.get_matches_from(
            vec![OsString::from(self.name.as_str())].into_iter()
                .chain(env::args_os().dropping(2)));
        if let Some(_) = matches.subcommand_matches("interface") {
            serde_json::to_writer(std::io::stdout(), self).expect("failed to serialize model interface");
            std::process::exit(0);
        } else if let Some(_) = matches.subcommand_matches("run") {
            let fc: FuncCall = serde_json::from_reader(std::io::stdin()).expect("deserialize of func call failed");
            match fc.validate(self) {
                Err(errors) => {
                    serde_json::to_writer(std::io::stderr(), &errors).expect("failed to write missing sinks and sources to stderr");
                    std::process::exit(1);
                },
                Ok(data) => return data
            }
        } else {
            std::process::exit(1);
        }
    }

    pub fn call_cli(&self, program_path: &str, fc: &FuncRequest) -> std::io::Result<std::process::ExitStatus> {
        let mut cmd = Command::new(program_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn().expect(format!("could not spawn process {}", program_path).as_str());

        let stdin = cmd.stdin.take().expect("could not open stdin");
        serde_json::to_writer(stdin, fc).expect("could not write request to stdin");

        cmd.wait()
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FuncRequest {
    sinks: HashMap<String, StoreRef>,
    sources: HashMap<String, StoreRef>
}

impl FuncRequest {
    pub fn get_source(&self, s: &str) -> Option<&StoreRef> {
        self.sources.get(s)
    }

    pub fn get_sink(&self, s: &str) -> Option<&StoreRef> {
        self.sinks.get(s)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FuncCall {
    sources: BTreeMap<String, StoreRef>,
    sinks: BTreeMap<String, StoreRef>
}

impl FuncCall {
    pub fn new() -> Self {
        Self {
            sources: BTreeMap::new(),
            sinks: BTreeMap::new()
        }
    }

    pub fn add_source(&mut self, name: &str, s: StoreRef) {
        self.sources.insert(name.to_string(), s);
    }

    pub fn add_sink(&mut self, name: &str, s: StoreRef) {
        self.sinks.insert(name.to_string(), s);
    }

    pub fn get_source(&self, s: &str) -> Option<&StoreRef> {
        self.sources.get(s)
    }

    pub fn get_sink(&self, s: &str) -> Option<&StoreRef> {
        self.sinks.get(s)
    }

    pub fn validate(&self, fi: &FuncInterface) -> Result<FuncRequest, HashMap<String, Vec<String>>> {
        let sinks = &fi.sinks;
        let sources = &fi.sources;
        let missing_sinks = self.sources.keys()
            .filter(|s| !sinks.contains_key(s.as_str()))
            .map(|s| s.to_string()).collect_vec();
        let missing_sources = self.sinks.keys()
            .filter(|s| !sources.contains_key(s.as_str()))
            .map(|s| s.to_string()).collect_vec();
        if missing_sinks.len() == 0 && missing_sources.len() == 0 {
            let mut hm = HashMap::new();
            hm.insert("sinks".to_string(), missing_sinks);
            hm.insert("sources".to_string(), missing_sources);
            Err(hm)
        } else {
            Ok(FuncRequest {
                sinks: self.sinks.iter()
                    .filter(|(k,v)| sinks.contains_key(k.as_str()))
                    .map(|(k,v)| (k.to_string(), v.clone())).collect(),
                sources: self.sources.iter()
                    .filter(|(k, v)| sources.contains_key(k.as_str()))
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect()
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json;
    use super::*;

    #[test]
    fn datatype_serialize() {
        let dt = DataType::Other;
        assert_eq!(serde_json::to_string(&dt).unwrap(), "\"Other\"");

        let mut d = BTreeMap::new();
        d.insert("max_temp".to_string(),ColType::F64);
        let dt = DataType::Table(TableSchema(d));
        assert_eq!(serde_json::to_string(&dt).unwrap(), "{\"Table\":{\"max_temp\":\"F64\"}}");

        let mut f = BTreeMap::new();
        f.insert(
            "daily".to_string(),
            Arg {
                description: "daily data".to_string(),
                datatype: dt,
            });
        let fi = FuncInterface {
            name: "simplecrop".to_string(),
            sinks: BTreeMap::new(),
            sources: f
        };
        assert_eq!(
        serde_json::to_string(&fi).unwrap(),
        r#"{"name":"simplecrop","sources":{"daily":{"description":"daily data","datatype":{"Table":{"max_temp":"F64"}}}},"sinks":{}}"#)
    }
}