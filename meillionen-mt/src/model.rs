use clap;
use serde_derive::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::ffi::OsString;
use core::fmt;
use itertools::Itertools;
use itertools::__std_iter::FromIterator;
use std::env;

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
    Table(TableSchema)
}

#[derive(Debug, Deserialize, Serialize)]
pub enum StoreRef {
    LocalPath(String)
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

    pub fn to_cli(&self) -> HashMap<String, String> {
        let cli_option_data= self.sources
            .iter().map(|(s, a)| format!("source:{}", s))
            .chain(self.sinks.iter().map(|(s,a)| format!("sink:{}", s)))
            .collect_vec();
        let mut app = clap::App::new(self.name.as_str());
        for (name, arg) in cli_option_data.iter()
            .zip(self.sources.values().chain(self.sinks.values())) {
            app = app.arg(clap::Arg::with_name(name.as_str())
                .long(name.as_ref())
                .value_name(name.as_ref())
                .help(arg.description.as_str())
                .required(true));
        }
        let matches = app.get_matches_from(
            vec![OsString::from(self.name.as_str())].into_iter()
                .chain(env::args_os().dropping(2)));
        HashMap::from_iter(matches.args.into_iter()
            .filter(|(k,v)| v.vals.get(0).is_some())
            .map(|(k,v)| (k.to_string(), v.vals.get(0).map(|s| s.clone().into_string().expect("UTF8 decode error")).unwrap())))
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
        assert_eq!(serde_json::to_string(&dt).unwrap(), "{\"DataFrame\":{\"max_temp\":\"F64\"}}");

        let mut f = BTreeMap::new();
        f.insert(
            "daily".to_string(),
            Arg {
                description: "daily data".to_string(),
                datatype: dt,
            });
        let fi = FuncInterface("simplecrop".to_string(), f);
        assert_eq!(
        serde_json::to_string(&fi).unwrap(),
        "{\"daily\":{\"description\":\"daily data\",\"datatype\":{\"DataFrame\":{\"max_temp\":\"F64\"}}}}")
    }
}