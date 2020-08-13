use pyo3::prelude::*;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::DataType;
use arrow::datatypes::DateUnit;
use lazy_static::lazy_static;
use simplecrop_cli::SimCtnlConfig;

lazy_static! {
    static ref IRRIGATION_FIELDS: Vec<Field> =
        vec![
            Field::new("date", DataType::Date32(DateUnit::Day), false),
            Field::new("amount", DataType::Date32(DateUnit::Day), false)
        ];
}

lazy_static! {
    static ref WEATHER_FIELDS: Vec<Field> = vec![
        Field::new("date", DataType::Date32(DateUnit::Day), false),
        Field::new("solar_radiation", DataType::Float32, false),
        Field::new("temp_max", DataType::Float32, false),
        Field::new("temp_min", DataType::Float32, false),
        Field::new("rain", DataType::Float32, false),
        Field::new("par", DataType::Float32, false)
    ];
}

lazy_static! {
    static ref SIMPLECROP_SCHEMA: Schema = Schema::new(vec![
        Field::new("irrigation", DataType::List(Box::new(DataType::Struct(IRRIGATION_FIELDS.to_vec()))), false),
        Field::new("weather", DataType::List(Box::new(DataType::Struct(WEATHER_FIELDS.to_vec()))), false),
        Field::new("simctrl", DataType::Struct(vec![
            Field::new("day_of_planting", DataType::Int32, false),
            Field::new("days_between_printout", DataType::Int32, false)
        ]), false),
        Field::new("plant", DataType::Struct(vec![
            // maximum number of leaves
            Field::new("lfmax", DataType::Float32, false),
            // empirical coef. for expoilinear eq.
            Field::new("emp2", DataType::Float32, false),
            // empirical coef. for expoilinear eq.
            Field::new("emp1", DataType::Float32, false),
            // plant density m-2
            Field::new("pd", DataType::Float32, false),
            // empirical coef. for expoilinear eq.
            Field::new("nb", DataType::Float32, false),
            // maximum rate of leaf appearearance (day-1)
            Field::new("rm", DataType::Float32, false),
            // fraction of total crop growth partitioned to canopy
            Field::new("fc", DataType::Float32, false),
            // base temperature above which reproductive growth occurs (c)
            Field::new("tb", DataType::Float32, false),
            // duration of reproductive stage (degree days)
            Field::new("intot", DataType::Float32, false),
            // leaf number
            Field::new("n", DataType::Float32, false),
            // canopy leaf area index (m2 m-2)
            Field::new("lai", DataType::Float32, false),
            // total plant dry matter weight (g m-2)
            Field::new("w", DataType::Float32, false),
            // root dry matter weight (g m-2)
            Field::new("wr", DataType::Float32, false),
            // canopy dry matter weight (g m-2)
            Field::new("wc", DataType::Float32, false),
            // dry matter of leaves removed per plant per unit development after
            //   maximum number of leaves is reached (g)
            Field::new("p1", DataType::Float32, false),
            // code for development phase
            //   1=vegetative phase
            //   2=reproductive phase
            Field::new("fl", DataType::Float32, false),
            // specific leaf area (m2 g-1)
            Field::new("sla", DataType::Float32, false)
        ]), false),
        Field::new("soil", DataType::Struct(vec![
            // water content at wilting point (fraction of void space)
            Field::new("wpp", DataType::Float32, false),
            // water content at field capacity (fraction of void space)
            Field::new("fcp", DataType::Float32, false),
            // water content saturation (fraction of void space)
            Field::new("stp", DataType::Float32, false),
            // depth of the profile considered in the simulation (cm)
            Field::new("dp", DataType::Float32, false),
            // daily drainage percentage (fraction of void space)
            Field::new("drnp", DataType::Float32, false),
            // runoff curve number
            Field::new("cn", DataType::Float32, false),
            // actual soil water storage in the profile (mm)
            Field::new("swc", DataType::Float32, false)
        ]), false)
    ]);
}

#[pyclass]
pub struct PySimpleCrop {
    pub irrigation: RecordBatch,
    pub weather: RecordBatch,
    pub simctrl: SimCtnlConfig
}