extern crate chrono;

use chrono::prelude::{DateTime, Utc};

#[derive(Debug, PartialEq)]
pub struct Irrigation {
    pub date: DateTime<Utc>,
    pub amount: f32,
}

#[derive(Debug, Default, PartialEq)]
pub struct IrrigationDataset(pub Vec<Irrigation>);

#[derive(Debug, PartialEq)]
pub struct Weather {
    pub date: DateTime<Utc>,
    pub srad: f32,
    pub tmax: f32,
    pub tmin: f32,
    pub rain: f32,
    pub par: f32
}

#[derive(Debug, Default, PartialEq)]
pub struct WeatherDataset(pub Vec<Weather>);

#[derive(Debug, Default, PartialEq)]
pub struct PlantConfig {
    pub lfmax: f32,
    pub emp2: f32,
    pub emp1: f32,
    pub pd: f32,
    pub nb: f32,
    pub rm: f32,
    pub fc: f32,
    pub tb: f32,
    pub intot: f32,
    pub n: f32,
    pub lai: f32,
    pub w: f32,
    pub wr: f32,
    pub wc: f32,
    pub p1: f32,
    pub f1: f32,
    pub sla: f32,
}

#[derive(Debug, Default, PartialEq)]
pub struct SoilConfig {
    pub wpp: f32,
    pub fcp: f32,
    pub stp: f32,
    pub dp: f32,
    pub drnp: f32,
    pub cn: f32,
    pub swc: f32
}

#[derive(Debug, Default, PartialEq)]
pub struct SimCtnlConfig {
    pub doyp: i32,
    pub frop: i32
}

#[derive(Debug, Default, PartialEq)]
pub struct PlantResult {
    pub doy: i32,
    pub n: f32,
    pub intc: f32,
    pub w: f32,
    pub wc: f32,
    pub wr: f32,
    pub wf: f32,
    pub lai: f32
}

#[derive(Debug, Default, PartialEq)]
pub struct PlantDataSet(pub Vec<PlantResult>);

#[derive(Debug, PartialEq)]
pub struct SoilResult {
    pub doy: i32,
    pub srad: f32,
    pub tmax: f32,
    pub tmin: f32,
    pub rain: f32,
    pub irr:  f32,
    pub rof:  f32,
    pub inf:  f32,
    pub drn:  f32,
    pub etp:  f32,
    pub esa:  f32,
    pub epa:  f32,
    pub swc:  f32,
    pub swc_dp: f32,
    pub swfac1: f32,
    pub swfac2: f32
}

#[derive(Debug, Default, PartialEq)]
pub struct SoilDataSet(pub Vec<SoilResult>);

#[derive(Debug, Default, PartialEq)]
pub struct SimpleCropConfig {
    pub irrigation_dataset: IrrigationDataset,
    pub plant: PlantConfig,
    pub simctrl: SimCtnlConfig,
    pub soil: SoilConfig,
    pub weather_dataset: WeatherDataset,
}

#[derive(Debug, Default, PartialEq)]
pub struct SimpleCropDataSet {
    pub soil: SoilDataSet,
    pub plant: PlantDataSet
}