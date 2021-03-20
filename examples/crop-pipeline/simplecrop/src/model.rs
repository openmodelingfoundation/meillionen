#![cfg_attr(not(debug_assertions), deny(warnings))]

use eyre::WrapErr;
use itertools::Itertools;
use std::fs::{create_dir_all, File};
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema, Field};
use meillionen_mt::model::DataType;
use std::sync::Arc;
use arrow::array::{Float32Array, ArrayRef, Int64Array, Int32Array};

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

#[derive(Debug, Default)]
pub struct SoilDataSetBuilder {
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

impl SoilDataSetBuilder {
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

    fn load<P: AsRef<Path>>(p: P) -> eyre::Result<Self> {
        let f = File::open(&p).map_err(|e| {
            eyre::eyre!(
                "Could not open {}. {}",
                p.as_ref().to_string_lossy(),
                e.to_string()
            )
        })?;
        let rdr = BufReader::new(f);
        let mut results = SoilDataSetBuilder::default();
        for line in rdr.lines().skip(6) {
            let record = line?;
            let data: Vec<&str> = record.split_whitespace().collect();
            results.deserialize(&data);
        }
        Ok(results)
    }
}

#[derive(Debug)]
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

#[derive(Debug, Default)]
pub struct PlantDataSetBuilder {
    day_of_year: Vec<i32>,
    plant_leaf_count: Vec<f32>,
    air_accumulated_temp: Vec<f32>,
    plant_matter: Vec<f32>,
    plant_matter_canopy: Vec<f32>,
    plant_matter_fruit: Vec<f32>,
    plant_matter_root: Vec<f32>,
    plant_leaf_area_index: Vec<f32>,
}

impl PlantDataSetBuilder {
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

    fn load<P: AsRef<Path>>(p: P) -> eyre::Result<Self> {
        println!(
            "Current Directory: {}",
            std::env::current_dir().unwrap().display()
        );
        let f = File::open(&p).map_err(|e| {
            eyre::eyre!(
                "Could not open {}. {}",
                p.as_ref().to_string_lossy(),
                e.to_string()
            )
        })?;
        let rdr = BufReader::new(f);
        let mut results = PlantDataSetBuilder::default();
        for line in rdr.lines().skip(9) {
            let record = line.unwrap();
            let data: Vec<&str> = record.split_whitespace().collect();
            results.deserialize(&data);
        }
        Ok(results)
    }
}

#[derive(Debug)]
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

fn import_output_data<P: AsRef<Path>>(dir: P) -> eyre::Result<RecordBatch> {
    let po = PlantDataSetBuilder::load(
        &dir.as_ref().join("output/plant.out"))?;
    let so = SoilDataSetBuilder::load(
        &dir.as_ref().join("output/soil.out"))?;
    use arrow::datatypes::DataType::*;
    let name_col_pairs: Vec<(Field, ArrayRef)> = vec![
        ("air_accumulated_temp", po.air_accumulated_temp),
        ("plant_leaf_area_index", po.plant_leaf_area_index),
        ("plant_leaf_count", po.plant_leaf_count),
        ("plant_matter", po.plant_matter),
        ("plant_matter_canopy", po.plant_matter_canopy),
        ("plant_matter_fruit", po.plant_matter_fruit),
        ("plant_matter_root", po.plant_matter_root),
        ("plant_potential_transpiration", so.plant_potential_transpiration),
        ("soil_daily_drainage", so.soil_daily_drainage),
        ("soil_daily_infiltration", so.soil_daily_infiltration),
        ("soil_daily_runoff", so.soil_daily_runoff),
        ("soil_evaporation", so.soil_evaporation),
        ("soil_evapotranspiration", so.soil_evapotranspiration),
        ("soil_water_deficit_stress", so.soil_water_deficit_stress),
        ("soil_water_excess_stress", so.soil_water_excess_stress),
        ("soil_water_profile_ratio", so.soil_water_profile_ratio),
        ("soil_water_storage_depth", so.soil_water_storage_depth),
    ]
        .into_iter()
        .map(|(name, col)| -> (Field, ArrayRef) {
            (Field::new(name, Float32, false), Arc::new(Float32Array::from(col)))
        })
        .chain({
            let day: ArrayRef = Arc::new(Int32Array::from(po.day_of_year));
            vec![(Field::new("day", Int32, false), day)]
        })
        .collect();
    let s = arrow::array::StructArray::from(name_col_pairs);
    Ok(RecordBatch::from(&s))
}

pub struct SimpleCropConfig<'a> {
    pub daily: DailyData<'a>,
    pub yearly: YearlyData,
}

impl<'a> SimpleCropConfig<'a> {
    fn save<P: AsRef<Path>>(&self, dir: P) -> eyre::Result<()> {
        let dp = dir.as_ref().join("data");
        create_dir_all(&dp)?;
        let write_f = |path: &str| File::create(&dp.join(path)).map(BufWriter::new).unwrap();

        let mut weather_buf = write_f("weather.inp");
        self.daily
            .save_weather(&mut weather_buf)
            .wrap_err("weather save failed")?;

        let mut irrigation_buf = write_f("irrig.inp");
        self.daily
            .save_irrigation(&mut irrigation_buf)
            .wrap_err("irrigation save failed")?;

        let mut plant_buf = write_f("plant.inp");
        self.yearly
            .save_plant_config(&mut plant_buf)
            .wrap_err("plant save failed")?;

        let mut soil_buf = write_f("soil.inp");
        self.yearly
            .save_soil_config(&mut soil_buf)
            .wrap_err("soil save failed")?;

        let mut simctrl_buf = write_f("simctrl.inp");
        self.yearly
            .save_simulation_config(&mut simctrl_buf)
            .wrap_err("simctrl save failed")?;
        Ok(())
    }

    pub fn run(&self, cli_path: impl AsRef<Path>, dir: impl AsRef<Path>) -> eyre::Result<()> {
        let cli_path = cli_path
            .as_ref()
            .canonicalize()
            .map_err(|e| eyre::eyre!(e.to_string()))?;
        self.save(&dir);
        create_dir_all(&dir.as_ref().join("output"));
        let _r = Command::new(cli_path).current_dir(&dir).spawn()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::model::{DailyData, PlantDataSetBuilder, SoilDataSetBuilder, YearlyData};
    use std::fs::read_to_string;
    use std::io::Cursor;
    use std::str;

    #[test]
    fn write_yearly_data() {
        let config = YearlyData::default();
        let mut cur = Cursor::new(Vec::new());
        config.save_plant_config(&mut cur).unwrap();
        let plant_ref_data = read_to_string("simplecrop/data/plant.inp").unwrap();
        assert_eq!(str::from_utf8(cur.get_ref()).unwrap(), plant_ref_data);

        let mut cur = Cursor::new(Vec::new());
        config.save_simulation_config(&mut cur).unwrap();
        let simctnl_ref_data = read_to_string("simplecrop/data/simctrl.inp").unwrap();
        assert_eq!(str::from_utf8(cur.get_ref()).unwrap(), simctnl_ref_data);

        let mut cur = Cursor::new(Vec::new());
        config.save_soil_config(&mut cur);
        let soil_ref_data = read_to_string("simplecrop/data/soil.inp").unwrap();
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
        w.save_weather(&mut cur);
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
        let data = PlantDataSetBuilder::load("simplecrop/output/plant.out").unwrap();
        assert_eq!(data.plant_leaf_count[0], 2.0);
        assert_eq!(data.air_accumulated_temp[0], 0.0);
        assert_eq!(data.plant_matter[0], 0.3);
        assert_eq!(data.plant_matter_canopy[0], 0.25);
        assert_eq!(data.plant_matter_fruit[0], 0.0);
        assert_eq!(data.plant_leaf_area_index[0], 0.01);
    }

    #[test]
    fn read_soil_t() {
        let data = SoilDataSetBuilder::load("simplecrop/output/soil.out").unwrap();
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
