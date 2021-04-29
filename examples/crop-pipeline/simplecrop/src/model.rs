#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::fs::{create_dir_all, File};
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Write, ErrorKind};
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float32Array, Int32Array, PrimitiveArray};
use arrow::datatypes::{ArrowPrimitiveType, Field, Schema};
use arrow::record_batch::RecordBatch;
use stable_eyre::eyre::WrapErr;

use meillionen_mt::arg::validation::{Columns, DataFrameValidator, Unvalidated};
use meillionen_mt::model::{FuncInterface, ResourceBuilder};
use std::convert::{TryInto};

macro_rules! make_field_vec {
    ($(($name: ident, $dt: ident)), *) => {
        vec![
            $(Field::new(stringify!($name), $dt, false)), *
        ]
    }
}

fn make_arg_description(
    name: &str,
    description: &str,
    resources: Vec<String>,
    fields: Vec<Field>,
) -> stable_eyre::Result<(String, Vec<u8>)> {
    let schema = Arc::new(Columns::new(fields));
    let dataframe_validator = DataFrameValidator::new(resources, description, schema);
    let serialized = (&dataframe_validator).try_into().wrap_err("serializing dataframe validator failed")?;
    Ok((
        name.to_string(),
        serialized
    ))
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DailyData<'a> {
    // irrigation related
    pub irrigation: &'a [f32],

    // weather related
    pub temp_max: &'a [f32],                   // tmax
    pub temp_min: &'a [f32],                   // tmin
    pub rainfall: &'a [f32],                   // rain
    pub photosynthetic_energy_flux: &'a [f32], // par
    pub energy_flux: &'a [f32],                // srad
}

impl<'a> DailyData<'a> {
    const NAME: &'static str = "daily";
    const DESCRIPTION: &'static str = "Daily inputs that influence crop yield";

    pub fn save_irrigation<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        let mut i = 1;
        for obs in self.irrigation.iter() {
            let row = format!("{:5}  {:1.1}\n", i, obs);
            buf.write_all(row.as_bytes())?;
            i += 1;
        }
        Ok(())
    }

    pub fn save_weather<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        for i in 0..self.temp_max.len() {
            let row = format!(
                "{:5}  {:>4.1}  {:>4.1}  {:>4.1}{:>6.1}              {:>4.1}\n",
                i + 1,
                self.energy_flux[i],
                self.temp_max[i],
                self.temp_min[i],
                self.rainfall[i],
                self.photosynthetic_energy_flux[i]
            );
            buf.write_all(row.as_bytes())?;
        }
        Ok(())
    }

    pub fn arg_description() -> stable_eyre::Result<(String, Vec<u8>)> {
        use arrow::datatypes::DataType::*;
        let fields = make_field_vec![
            (irrigation, Float32),
            (temp_max, Float32),
            (temp_min, Float32),
            (rainfall, Float32),
            (photosynthetic_energy_flux, Float32),
            (energy_flux, Float32)
        ];
        make_arg_description(
            Self::NAME.as_ref(),
            Self::DESCRIPTION.as_ref(),
            vec!["meillionen::FeatherResource".to_string(), "meillionen::ParquetResource".to_string()],
            fields,
        )
    }
}

#[derive(Debug, PartialEq)]
pub struct YearlyData {
    // plant config
    pub plant_leaves_max_number: f32, // lfmax
    pub plant_emp2: f32,
    pub plant_emp1: f32,
    pub plant_density: f32,                  // pd
    pub plant_nb: f32,                       // nb
    pub plant_leaf_max_appearance_rate: f32, // rm
    pub plant_growth_canopy_fraction: f32,
    pub plant_min_repro_growth_temp: f32,
    pub plant_repro_phase_duration: f32,
    pub plant_leaves_number_of: f32,
    pub plant_leaf_area_index: f32,
    pub plant_matter: f32,                // w
    pub plant_matter_root: f32,           // wr
    pub plant_matter_canopy: f32,         // wc
    pub plant_matter_leaves_removed: f32, // p1
    pub plant_development_phase: f32,     // fl
    pub plant_leaf_specific_area: f32,    // sla

    // soil config
    pub soil_water_content_wilting_point: f32,  // wpp
    pub soil_water_content_field_capacity: f32, // fcp
    pub soil_water_content_saturation: f32,     // stp
    pub soil_profile_depth: f32,                // dp
    pub soil_drainage_daily_percent: f32,       // drnp
    pub soil_runoff_curve_number: f32,          // cn
    pub soil_water_storage: f32,                // swc

    // simulation config
    pub day_of_planting: i32, //doyp
    pub printout_freq: i32,   // frop
}

impl YearlyData {
    const NAME: &'static str = "yearly";
    const DESCRIPTION: &'static str = "Yearly parameters influencing crop growth";

    fn value<T: ArrowPrimitiveType>(
        rb: &RecordBatch,
        name: &str,
        i: usize,
    ) -> stable_eyre::Result<T::Native> {
        let col_ind = rb.schema().index_of(&name)?;
        let col = rb.column(col_ind);
        col.as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .map(|a| a.value(i))
            .ok_or(stable_eyre::Report::msg(format!(
                "column type mismatch. expected {:#?} got {:#?}",
                T::DATA_TYPE,
                rb.column(col_ind).data_type()
            )))
    }

    pub fn arg_description() -> stable_eyre::Result<(String, Vec<u8>)> {
        use arrow::datatypes::DataType::*;
        let fields = make_field_vec![
            (plant_leaves_max_number, Float32),
            (plant_emp2, Float32),
            (plant_emp1, Float32),
            (plant_density, Float32),
            (plant_nb, Float32),
            (plant_leaf_max_appearance_rate, Float32),
            (plant_growth_canopy_fraction, Float32),
            (plant_min_repro_growth_temp, Float32),
            (plant_repro_phase_duration, Float32),
            (plant_leaves_number_of, Float32),
            (plant_leaf_area_index, Float32),
            (plant_matter, Float32),
            (plant_matter_root, Float32),
            (plant_matter_canopy, Float32),
            (plant_matter_leaves_removed, Float32),
            (plant_development_phase, Float32),
            (plant_leaf_specific_area, Float32),
            (soil_water_content_wilting_point, Float32),
            (soil_water_content_field_capacity, Float32),
            (soil_water_content_saturation, Float32),
            (soil_profile_depth, Float32),
            (soil_drainage_daily_percent, Float32),
            (soil_runoff_curve_number, Float32),
            (soil_water_storage, Float32),
            (day_of_planting, Int32),
            (printout_freq, Int32)
        ];
        make_arg_description(
            Self::NAME.as_ref(),
            Self::DESCRIPTION.as_ref(),
            vec!["meillionen::FeatherResource".to_string(), "meillionen::ParquetResource".to_string()],
            fields,
        )
    }

    pub fn from_recordbatch_row(rb: &RecordBatch, i: usize) -> stable_eyre::Result<Self> {
        use arrow::datatypes::*;
        macro_rules! make_inst_rb {
            ($(($field: ident, $ty: ty)), *) => {
                Self {
                    $($field: Self::value::<$ty>(&rb, stringify!($field), i)?),*
                }
            }
        }
        Ok(make_inst_rb![
            (plant_leaves_max_number, Float32Type),
            (plant_emp2, Float32Type),
            (plant_emp1, Float32Type),
            (plant_density, Float32Type),
            (plant_nb, Float32Type),
            (plant_leaf_max_appearance_rate, Float32Type),
            (plant_growth_canopy_fraction, Float32Type),
            (plant_min_repro_growth_temp, Float32Type),
            (plant_repro_phase_duration, Float32Type),
            (plant_leaves_number_of, Float32Type),
            (plant_leaf_area_index, Float32Type),
            (plant_matter, Float32Type),
            (plant_matter_root, Float32Type),
            (plant_matter_canopy, Float32Type),
            (plant_matter_leaves_removed, Float32Type),
            (plant_development_phase, Float32Type),
            (plant_leaf_specific_area, Float32Type),
            (soil_water_content_wilting_point, Float32Type),
            (soil_water_content_field_capacity, Float32Type),
            (soil_water_content_saturation, Float32Type),
            (soil_profile_depth, Float32Type),
            (soil_drainage_daily_percent, Float32Type),
            (soil_runoff_curve_number, Float32Type),
            (soil_water_storage, Float32Type),
            (day_of_planting, Int32Type),
            (printout_freq, Int32Type)
        ])
    }

    pub fn save_plant_config<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        let data = format!(
            " {:>7.4} {:>7.4} {:>7.4} {:>7.4} {:>7.4} {:>7.4} \
            {:>7.4} {:>7.4} {:>7.4} {:>7.4} {:>7.4} {:>7.4} \
            {:>7.4} {:>7.4} {:>7.4} {:>7.4} {:>7.4}\n",
            self.plant_leaves_max_number,
            self.plant_emp2,
            self.plant_emp1,
            self.plant_density,
            self.plant_nb,
            self.plant_leaf_max_appearance_rate,
            self.plant_growth_canopy_fraction,
            self.plant_min_repro_growth_temp,
            self.plant_repro_phase_duration,
            self.plant_leaves_number_of,
            self.plant_leaf_area_index,
            self.plant_matter,
            self.plant_matter_root,
            self.plant_matter_canopy,
            self.plant_matter_leaves_removed,
            self.plant_development_phase,
            self.plant_leaf_specific_area
        );
        buf.write_all(data.as_bytes())?;
        let footer: &'static str = "   Lfmax    EMP2    EMP1      PD      nb      rm      fc      tb   intot       n     lai       w      wr      wc      p1      f1    sla\n";
        buf.write_all(footer.as_bytes())?;
        Ok(())
    }

    pub fn save_simulation_config<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        let data = format!("{:>6} {:>5}\n", self.day_of_planting, self.printout_freq);
        buf.write_all(data.as_bytes())?;
        let footer: &'static str = "  DOYP  FROP\n";
        buf.write_all(footer.as_bytes())?;
        Ok(())
    }

    pub fn save_soil_config<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        let data = format!(
            "     {:>5.2}     {:>5.2}     {:>5.2}     {:>7.2}     {:>5.2}     {:>5.2}     {:>5.2}\n",
            self.soil_water_content_wilting_point, self.soil_water_content_field_capacity, self.soil_water_content_saturation, self.soil_profile_depth, self.soil_drainage_daily_percent, self.soil_runoff_curve_number, self.soil_water_storage);
        buf.write_all(data.as_bytes())?;
        let footer: &'static str =
            "       WPp       FCp       STp          DP      DRNp        CN        SWC\n";
        buf.write_all(footer.as_bytes())?;
        let units: &'static str =
            "  (cm3/cm3) (cm3/cm3) (cm3/cm3)        (cm)  (frac/d)        -       (mm)\n";
        buf.write_all(units.as_bytes())?;
        Ok(())
    }
}

impl Default for YearlyData {
    fn default() -> Self {
        Self {
            plant_leaves_max_number: 12.0,
            plant_emp2: 0.64,
            plant_emp1: 0.104,
            plant_density: 5.0,
            plant_nb: 5.3,
            plant_leaf_max_appearance_rate: 0.100,
            plant_growth_canopy_fraction: 0.85,
            plant_min_repro_growth_temp: 10.0,
            plant_repro_phase_duration: 300.0,
            plant_leaves_number_of: 2.0,
            plant_leaf_area_index: 0.013,
            plant_matter: 0.3,
            plant_matter_root: 0.045,
            plant_matter_canopy: 0.255,
            plant_matter_leaves_removed: 0.03,
            plant_development_phase: 0.028,
            plant_leaf_specific_area: 0.035,

            soil_water_content_wilting_point: 0.06,
            soil_water_content_field_capacity: 0.17,
            soil_water_content_saturation: 0.28,
            soil_profile_depth: 145.00,
            soil_drainage_daily_percent: 0.10,
            soil_runoff_curve_number: 55.00,
            soil_water_storage: 246.50,

            day_of_planting: 121,
            printout_freq: 3,
        }
    }
}

pub fn get_func_interface() -> stable_eyre::Result<FuncInterface> {
    let mut builder = ResourceBuilder::new("simplecrop_omf");

    let (daily_key, daily_source) = DailyData::arg_description()?;
    builder.add("source", daily_key.as_str(), "dataframe_validator", daily_source.as_slice())?;
    let (yearly_key, yearly_source) = YearlyData::arg_description()?;
    builder.add("source", yearly_key.as_str(), "dataframe_validator", yearly_source.as_slice())?;

    let (soil_key, soil_sink) = SoilDataSet::arg_description()?;
    builder.add("sink", soil_key.as_str(), "dataframe_validator", soil_sink.as_slice())?;
    let (plant_key, plant_sink) = PlantDataSet::arg_description()?;
    builder.add("sink", plant_key.as_str(), "dataframe_validator", plant_sink.as_slice())?;
    let unval: Vec<u8> = (&Unvalidated::new()).try_into().wrap_err("unavalidated serialization error")?;
    builder.add("sink", "tempdir", "unvalidated", unval.as_slice())?;
    let rb = builder.extract_to_recordbatch();

    let fi = FuncInterface::try_new(rb).unwrap();
    Ok(fi)
}

#[derive(Debug, Default)]
pub struct SoilDataSet {
    pub day_of_year: Vec<i32>,
    pub soil_daily_runoff: Vec<f32>,             // rof
    pub soil_daily_infiltration: Vec<f32>,       // int
    pub soil_daily_drainage: Vec<f32>,           // drn
    pub soil_evapotranspiration: Vec<f32>,       // etp
    pub soil_evaporation: Vec<f32>,              // esa
    pub plant_potential_transpiration: Vec<f32>, // epa
    pub soil_water_storage_depth: Vec<f32>,      // swc
    pub soil_water_profile_ratio: Vec<f32>,      // swc / dp
    pub soil_water_deficit_stress: Vec<f32>,     // swfac1
    pub soil_water_excess_stress: Vec<f32>,      // swfac2
}

impl SoilDataSet {
    const NAME: &'static str = "soil";
    const DESCRIPTION: &'static str = "Daily soil characteristics";

    fn deserialize(&mut self, vs: &Vec<&str>) -> Option<()> {
        let (sdoy, srest) = vs.split_first().unwrap();
        let doy = sdoy.parse::<i32>().ok()?;
        let fs = srest
            .iter()
            .map(|f| f.parse::<f32>().ok())
            .collect::<Option<Vec<f32>>>()?;
        if let [_srad, _tmax, _tmin, _rain, _irr, rof, inf, drn, etp, esa, epa, swc, swc_dp, swfac1, swfac2] =
            fs[..]
        {
            self.day_of_year.push(doy);
            self.soil_daily_runoff.push(rof);
            self.soil_daily_infiltration.push(inf);
            self.soil_daily_drainage.push(drn);
            self.soil_evapotranspiration.push(etp);
            self.soil_evaporation.push(esa);
            self.plant_potential_transpiration.push(epa);
            self.soil_water_storage_depth.push(swc);
            self.soil_water_profile_ratio.push(swc_dp);
            self.soil_water_deficit_stress.push(swfac1);
            self.soil_water_excess_stress.push(swfac2);
        }
        Some(())
    }

    fn load<P: AsRef<Path>>(p: P) -> stable_eyre::Result<Self> {
        let f = File::open(&p).wrap_err_with(||
            format!("Could not open {}", p.as_ref().to_string_lossy()))?;
        let rdr = BufReader::new(f);
        let mut results = SoilDataSet::default();
        for line in rdr.lines().skip(6) {
            let record = line?;
            let data: Vec<&str> = record.split_whitespace().collect();
            results.deserialize(&data);
        }
        Ok(results)
    }

    pub fn arg_description() -> stable_eyre::Result<(String, Vec<u8>)> {
        use arrow::datatypes::DataType::*;
        let fields = make_field_vec![
            (day_of_year, Int32),
            (soil_daily_runoff, Float32),
            (soil_daily_infiltration, Float32),
            (soil_daily_drainage, Float32),
            (soil_evapotranspiration, Float32),
            (soil_evaporation, Float32),
            (plant_potential_transpiration, Float32),
            (soil_water_storage_depth, Float32),
            (soil_water_profile_ratio, Float32),
            (soil_water_deficit_stress, Float32),
            (soil_water_excess_stress, Float32)
        ];
        make_arg_description(
            Self::NAME.as_ref(),
            Self::DESCRIPTION.as_ref(),
            vec!["meillionen::FeatherResource".to_string(), "meillionen::ParquetResource".to_string()],

            fields,
        )
    }
}

#[derive(Debug, Default)]
pub struct PlantDataSet {
    day_of_year: Vec<i32>,
    plant_leaf_count: Vec<f32>,
    air_accumulated_temp: Vec<f32>,
    plant_matter: Vec<f32>,
    plant_matter_canopy: Vec<f32>,
    plant_matter_fruit: Vec<f32>,
    plant_matter_root: Vec<f32>,
    plant_leaf_area_index: Vec<f32>,
}

impl PlantDataSet {
    const NAME: &'static str = "plant";
    const DESCRIPTION: &'static str = "Daily plant characteristic results";

    fn deserialize(&mut self, vs: &Vec<&str>) -> Option<()> {
        let (sdoy, srest) = vs.split_first().unwrap();
        let doy = sdoy.parse::<i32>().ok()?;
        let fs = srest
            .iter()
            .map(|f| f.parse::<f32>().ok())
            .collect::<Option<Vec<f32>>>()?;
        if let [n, intc, w, wc, wr, wf, lai] = fs[..] {
            self.day_of_year.push(doy);
            self.plant_leaf_count.push(n);
            self.air_accumulated_temp.push(intc);
            self.plant_matter.push(w);
            self.plant_matter_canopy.push(wc);
            self.plant_matter_fruit.push(wf);
            self.plant_matter_root.push(wr);
            self.plant_leaf_area_index.push(lai);
        }
        Some(())
    }

    fn load<P: AsRef<Path>>(p: P) -> stable_eyre::Result<Self> {
        println!(
            "Current Directory: {}",
            std::env::current_dir().unwrap().display()
        );
        let f = File::open(&p).wrap_err(
            format!("Could not open {}", p.as_ref().to_string_lossy()))?;
        let rdr = BufReader::new(f);
        let mut results = PlantDataSet::default();
        for line in rdr.lines().skip(9) {
            let record = line.unwrap();
            let data: Vec<&str> = record.split_whitespace().collect();
            results.deserialize(&data);
        }
        Ok(results)
    }

    pub fn arg_description() -> stable_eyre::Result<(String, Vec<u8>)> {
        use arrow::datatypes::DataType::*;
        let fields = make_field_vec![
            (day_of_year, Int32),
            (air_accumulated_temp, Float32),
            (plant_matter, Float32),
            (plant_matter_canopy, Float32),
            (plant_matter_fruit, Float32),
            (plant_matter_root, Float32),
            (plant_leaf_area_index, Float32)
        ];
        make_arg_description(
            Self::NAME.as_ref(),
            Self::DESCRIPTION.as_ref(),
            vec!["meillionen::FeatherResource".to_string(), "meillionen::ParquetResource".to_string()],

            fields,
        )
    }
}

fn load_output_data<P: AsRef<Path>>(dir: P) -> stable_eyre::Result<(RecordBatch, RecordBatch)> {
    let po = PlantDataSet::load(&dir.as_ref().join("output/plant.out"))?;
    let so = SoilDataSet::load(&dir.as_ref().join("output/soil.out"))?;
    use arrow::datatypes::DataType::*;
    let soil = {
        let (fields, cols): (Vec<Field>, Vec<ArrayRef>) = vec![
            ("soil_daily_drainage", so.soil_daily_drainage),
            ("soil_daily_infiltration", so.soil_daily_infiltration),
            ("soil_daily_runoff", so.soil_daily_runoff),
            ("soil_evaporation", so.soil_evaporation),
            ("soil_evapotranspiration", so.soil_evapotranspiration),
            ("soil_water_deficit_stress", so.soil_water_deficit_stress),
            ("soil_water_excess_stress", so.soil_water_excess_stress),
            ("soil_water_profile_ratio", so.soil_water_profile_ratio),
            ("soil_water_storage_depth", so.soil_water_storage_depth),
            (
                "plant_potential_transpiration",
                so.plant_potential_transpiration,
            ),
        ]
        .into_iter()
        .map(|(name, col)| -> (Field, ArrayRef) {
            (
                Field::new(name, Float32, false),
                Arc::new(Float32Array::from(col)),
            )
        })
        .chain({
            let day: ArrayRef = Arc::new(Int32Array::from(so.day_of_year));
            vec![(Field::new("day", Int32, false), day)]
        })
        .unzip();

        let schema_ref = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema_ref, cols).wrap_err("Cannot create soil record batch")
    }?;
    let plant = {
        let (fields, cols): (Vec<Field>, Vec<ArrayRef>) = vec![
            ("air_accumulated_temp", po.air_accumulated_temp),
            ("plant_leaf_area_index", po.plant_leaf_area_index),
            ("plant_leaf_count", po.plant_leaf_count),
            ("plant_matter", po.plant_matter),
            ("plant_matter_canopy", po.plant_matter_canopy),
            ("plant_matter_fruit", po.plant_matter_fruit),
            ("plant_matter_root", po.plant_matter_root),
        ]
        .into_iter()
        .map(|(name, col)| -> (Field, ArrayRef) {
            (
                Field::new(name, Float32, false),
                Arc::new(Float32Array::from(col)),
            )
        })
        .chain({
            let day: ArrayRef = Arc::new(Int32Array::from(po.day_of_year));
            vec![(Field::new("day", Int32, false), day)]
        })
        .unzip();
        let schema_ref = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema_ref, cols).wrap_err("Cannot create plant record batch")
    }?;
    Ok((plant, soil))
}

pub struct SimpleCropConfig<'a> {
    pub daily: DailyData<'a>,
    pub yearly: YearlyData,
}

impl<'a> SimpleCropConfig<'a> {
    fn save<P: AsRef<Path>>(&self, dir: P) -> stable_eyre::Result<()> {
        let dp = dir.as_ref().join("data");
        create_dir_all(&dp).wrap_err("Cannot create data dir")?;
        let write_f = |path: &str| File::create(&dp.join(path))
            .map(BufWriter::new)
            .wrap_err_with(|| format!("cannot create file {}", path));

        let mut weather_buf = write_f("weather.inp")?;
        self.daily
            .save_weather(&mut weather_buf)
            .wrap_err("weather save failed")?;

        let mut irrigation_buf = write_f("irrig.inp")?;
        self.daily
            .save_irrigation(&mut irrigation_buf)
            .wrap_err("irrigation save failed")?;

        let mut plant_buf = write_f("plant.inp")?;
        self.yearly
            .save_plant_config(&mut plant_buf)
            .wrap_err("plant save failed")?;

        let mut soil_buf = write_f("soil.inp")?;
        self.yearly
            .save_soil_config(&mut soil_buf)
            .wrap_err("soil save failed")?;

        let mut simctrl_buf = write_f("simctrl.inp")?;
        self.yearly
            .save_simulation_config(&mut simctrl_buf)
            .wrap_err("simctrl save failed")?;
        Ok(())
    }

    pub fn run(
        &self,
        cli_path: impl AsRef<Path>,
        dir: impl AsRef<Path>,
    ) -> stable_eyre::Result<(RecordBatch, RecordBatch)> {
        let cli_path = cli_path.as_ref();
        self.save(&dir)?;
        create_dir_all(&dir.as_ref().join("output")).wrap_err("Cannot create output dir")?;
        let _r = Command::new(cli_path).current_dir(&dir).spawn();
        if _r.is_err() {
            let e = _r.err().unwrap();
            let k = e.kind();
            if k == ErrorKind::NotFound {
                return Err(e).wrap_err_with(|| format!("Executable not found at {}", &cli_path.to_string_lossy()));
            }
            return Err(e).wrap_err_with(|| format!("Error executing simplecrop in dir {} (got {:?})", dir.as_ref().to_string_lossy(), k));
        } else {
            _r.unwrap().wait().wrap_err_with(|| format!("Error during simplecrop execution in dir {}", dir.as_ref().to_string_lossy()))?;
            load_output_data(&dir)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::read_to_string;
    use std::io::Cursor;
    use std::str;

    use crate::model::{DailyData, PlantDataSet, SoilDataSet, YearlyData};

    #[test]
    fn write_yearly_data() {
        let config = YearlyData::default();
        let mut cur = Cursor::new(Vec::new());
        config.save_plant_config(&mut cur).unwrap();
        let plant_ref_data = read_to_string("data/data/plant.inp").unwrap();
        assert_eq!(str::from_utf8(cur.get_ref()).unwrap(), plant_ref_data);

        let mut cur = Cursor::new(Vec::new());
        config.save_simulation_config(&mut cur).unwrap();
        let simctnl_ref_data = read_to_string("data/data/simctrl.inp").unwrap();
        assert_eq!(str::from_utf8(cur.get_ref()).unwrap(), simctnl_ref_data);

        let mut cur = Cursor::new(Vec::new());
        config.save_soil_config(&mut cur).unwrap();
        let soil_ref_data = read_to_string("data/data/soil.inp").unwrap();
        assert_eq!(str::from_utf8(cur.get_ref()).unwrap(), soil_ref_data);
    }

    #[test]
    fn write_daily_data() {
        let w = DailyData {
            irrigation: &[0f32, 1f32],
            energy_flux: &[5.1],
            temp_max: &[20.0f32],
            temp_min: &[4.4f32],
            rainfall: &[23.9],
            photosynthetic_energy_flux: &[10.7f32],
        };

        let mut cur = Cursor::new(Vec::new());
        w.save_weather(&mut cur).unwrap();
        assert_eq!(
            str::from_utf8(cur.get_ref()).unwrap(),
            "    1   5.1  20.0   4.4  23.9              10.7\n"
        );

        let mut cur = Cursor::new(Vec::new());
        w.save_irrigation(&mut cur).unwrap();
        assert_eq!(
            "    1  0.0\n    2  1.0\n",
            str::from_utf8(cur.get_ref()).unwrap()
        );
    }

    #[test]
    fn read_plant_t() {
        let data = PlantDataSet::load("data/output/plant.out").unwrap();
        assert_eq!(data.plant_leaf_count[0], 2.0);
        assert_eq!(data.air_accumulated_temp[0], 0.0);
        assert_eq!(data.plant_matter[0], 0.3);
        assert_eq!(data.plant_matter_canopy[0], 0.25);
        assert_eq!(data.plant_matter_fruit[0], 0.0);
        assert_eq!(data.plant_leaf_area_index[0], 0.01);
    }

    #[test]
    fn read_soil_t() {
        let data = SoilDataSet::load("data/output/soil.out").unwrap();
        assert_eq!(data.soil_daily_runoff[0], 0.0f32);
        assert_eq!(data.soil_daily_infiltration[0], 0.0f32);
        assert_eq!(data.soil_daily_drainage[0], 1.86f32);
        assert_eq!(data.soil_evapotranspiration[0], 2.25f32);
        assert_eq!(data.soil_evaporation[0], 2.23f32);
        assert_eq!(data.plant_potential_transpiration[0], 0.02f32);
        assert_eq!(data.soil_water_storage_depth[0], 260.97f32);
        assert_eq!(data.soil_water_profile_ratio[0], 1.8f32);
        assert_eq!(data.soil_water_deficit_stress[0], 1.0f32);
        assert_eq!(data.soil_water_excess_stress[0], 1.0f32);
    }
}
