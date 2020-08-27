#![cfg_attr(not(debug_assertions), deny(warnings))]

use std::fs::{File, create_dir_all};
use std::path::Path;
use std::io::{Write, BufReader, BufRead, BufWriter};
use std::io;
use std::process::{Command, Child};
use ndarray::Array1;
use std::ops::Index;
use std::slice::SliceIndex;
use meillionen_mt::{IntoPandas, FromPandas};
use meillionen_mt_derive::{IntoPandas, FromPandas};

#[derive(Debug, Default, PartialEq, FromPandas)]
pub struct DailyData {
    // irrigation related
    pub irrigation: Array1<f32>,

    // weather related
    pub temp_max: Array1<f32>, // tmax
    pub temp_min: Array1<f32>, // tmin
    pub rainfall: Array1<f32>, // rain
    pub photosynthetic_energy_flux: Array1<f32>, // par
    pub energy_flux: Array1<f32> // srad
}

impl DailyData {
    pub fn save_irrigation<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        let mut i = 1;
        for obs in self.irrigation.iter() {
            let row = format!("{:5}  {:1.1}\n", i, obs);
            buf.write(row.as_bytes())?;
            i += 1;
        }
        Ok(())
    }

    pub fn save_weather<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        let i = 0;
        for obs in 0..self.temp_max.len() {
            let row = format!(
                "{:5}  {:>4.1}  {:>4.1}  {:>4.1}{:>6.1}              {:>4.1}\n",
                i+1, self.energy_flux[i], self.temp_max[i], self.temp_min[i],
                self.rainfall[i], self.photosynthetic_energy_flux[i]);
            buf.write(row.as_bytes())?;
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
    pub plant_density: f32, // pd
    pub plant_nb: f32, // nb
    pub plant_leaf_max_appearance_rate: f32, // rm
    pub plant_growth_canopy_fraction: f32,
    pub plant_min_repro_growth_temp: f32,
    pub plant_repro_phase_duration: f32,
    pub plant_leaves_number_of: f32,
    pub plant_leaf_area_index: f32,
    pub plant_matter: f32, // w
    pub plant_matter_root: f32, // wr
    pub plant_matter_canopy: f32, // wc
    pub plant_matter_leaves_removed: f32, // p1
    pub plant_development_phase: f32, // fl
    pub plant_leaf_specific_area: f32, // sla

    // soil config
    pub soil_water_content_wilting_point: f32, // wpp
    pub soil_water_content_field_capacity: f32, // fcp
    pub soil_water_content_saturation: f32, // stp
    pub soil_profile_depth: f32, // dp
    pub soil_drainage_daily_percent: f32, // drnp
    pub soil_runoff_curve_number: f32, // cn
    pub soil_water_storage: f32, // swc

    // simulation config
    pub day_of_planting: i32, //doyp
    pub printout_freq: i32 // frop
}

impl YearlyData {
    pub fn save_plant_config<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        let data = format!(
            " {:>7.4} {:>7.4} {:>7.4} {:>7.4} {:>7.4} {:>7.4} \
            {:>7.4} {:>7.4} {:>7.4} {:>7.4} {:>7.4} {:>7.4} \
            {:>7.4} {:>7.4} {:>7.4} {:>7.4} {:>7.4}\n",
            self.plant_leaves_max_number, self.plant_emp2, self.plant_emp1, self.plant_density, self.plant_nb, self.plant_leaf_max_appearance_rate,
            self.plant_growth_canopy_fraction, self.plant_min_repro_growth_temp, self.plant_repro_phase_duration, self.plant_leaves_number_of, self.plant_leaf_area_index, self.plant_matter,
            self.plant_matter_root, self.plant_matter_canopy, self.plant_matter_leaves_removed, self.plant_development_phase, self.plant_leaf_specific_area);
        buf.write(data.as_bytes())?;
        let footer: &'static str = "   Lfmax    EMP2    EMP1      PD      nb      rm      fc      tb   intot       n     lai       w      wr      wc      p1      f1    sla\n";
        buf.write(footer.as_bytes())?;
        Ok(())
    }

    pub fn save_simulation_config<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        let data = format!("{:>6} {:>5}\n", self.day_of_planting, self.printout_freq);
        buf.write(data.as_bytes())?;
        let footer: &'static str = "  DOYP  FROP\n";
        buf.write(footer.as_bytes())?;
        Ok(())
    }

    pub fn save_soil_config<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        let data = format!(
            "     {:>5.2}     {:>5.2}     {:>5.2}     {:>7.2}     {:>5.2}     {:>5.2}     {:>5.2}\n",
            self.soil_water_content_wilting_point, self.soil_water_content_field_capacity, self.soil_water_content_saturation, self.soil_profile_depth, self.soil_drainage_daily_percent, self.soil_runoff_curve_number, self.soil_water_storage);
        buf.write(data.as_bytes())?;
        let footer: &'static str =
            "       WPp       FCp       STp          DP      DRNp        CN        SWC\n";
        buf.write(footer.as_bytes())?;
        let units: &'static str =
            "  (cm3/cm3) (cm3/cm3) (cm3/cm3)        (cm)  (frac/d)        -       (mm)\n";
        buf.write(units.as_bytes())?;
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
    pub soil_daily_runoff: Vec<f32>, // rof
    pub soil_daily_infiltration: Vec<f32>, // int
    pub soil_daily_drainage: Vec<f32>, // drn
    pub soil_evapotranspiration: Vec<f32>, // etp
    pub soil_evaporation: Vec<f32>, // esa
    pub plant_potential_transpiration: Vec<f32>, // epa
    pub soil_water_storage_depth: Vec<f32>, // swc
    pub soil_water_profile_ratio: Vec<f32>, // swc / dp
    pub soil_water_deficit_stress: Vec<f32>, // swfac1
    pub soil_water_excess_stress: Vec<f32> // swfac2
}

impl SoilDataSetBuilder {
    fn deserialize(&mut self, vs: &Vec<&str>) -> Option<()> {
        let (sdoy, srest) = vs.split_first().unwrap();
        let doy = sdoy.parse::<i32>().ok()?;
        let fs = srest.iter().map(|f| f.parse::<f32>().ok()).collect::<Option<Vec<f32>>>()?;
        if let [srad, tmax, tmin, rain, irr, rof, inf, drn, etp, esa, epa, swc, swc_dp, swfac1, swfac2] = fs[..]
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

    fn load<P: AsRef<Path>>(p: P) -> io::Result<Self> {
        let f = File::open(p)?;
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

#[derive(Debug, IntoPandas)]
pub struct SoilDataSet {
    pub day_of_year: Array1<i32>,
    pub soil_daily_runoff: Array1<f32>, // rof
    pub soil_daily_infiltration: Array1<f32>, // int
    pub soil_daily_drainage: Array1<f32>, // drn
    pub soil_evapotranspiration: Array1<f32>, // etp
    pub soil_evaporation: Array1<f32>, // esa
    pub plant_potential_transpiration: Array1<f32>, // epa
    pub soil_water_storage_depth: Array1<f32>, // swc
    pub soil_water_profile_ratio: Array1<f32>, // swc / dp
    pub soil_water_deficit_stress: Array1<f32>, // swfac1
    pub soil_water_excess_stress: Array1<f32> // swfac2
}

#[derive(Debug, Default)]
pub struct PlantDataSetBuilder {
    day_of_year: Vec<i32>,
    plant_leaf_count: Vec<f32>,
    air_accumulated_temp: Vec<f32>,
    plant_matter: Vec<f32>,
    plant_matter_canopy: Vec<f32>,
    plant_matter_fruit:  Vec<f32>,
    plant_matter_root: Vec<f32>,
    plant_leaf_area_index: Vec<f32>,
}

impl PlantDataSetBuilder {
    fn deserialize(&mut self, vs: &Vec<&str>) -> Option<()> {
        let (sdoy, srest) = vs.split_first().unwrap();
        let doy = sdoy.parse::<i32>().ok()?;
        let fs = srest.iter().map(|f| f.parse::<f32>().ok()).collect::<Option<Vec<f32>>>()?;
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

    fn load<P: AsRef<Path>>(p: P) -> io::Result<Self> {
        let f = File::open(p)?;
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

#[derive(Debug, IntoPandas)]
pub struct PlantDataSet {
    day_of_year: Array1<i32>,
    plant_leaf_count: Array1<f32>,
    air_accumulated_temp: Array1<f32>,
    plant_matter: Array1<f32>,
    plant_matter_canopy: Array1<f32>,
    plant_matter_fruit:  Array1<f32>,
    plant_matter_root: Array1<f32>,
    plant_leaf_area_index: Array1<f32>,
}

#[derive(Debug)]
pub struct SimpleCropDataSet {
    pub plant: PlantDataSet,
    pub soil: SoilDataSet
}

impl SimpleCropDataSet {
    pub fn load<P: AsRef<Path>>(p: P) -> io::Result<Self> {
        let op = p.as_ref().join("output");
        create_dir_all(&op)?;
        let plant = PlantDataSetBuilder::load(&op.join("plant.out"))?;
        let plant = PlantDataSet {
            day_of_year: From::from(plant.day_of_year),
            plant_leaf_count: From::from(plant.plant_leaf_count),
            air_accumulated_temp: From::from(plant.air_accumulated_temp),
            plant_matter: From::from(plant.plant_matter),
            plant_matter_canopy: From::from(plant.plant_matter_canopy),
            plant_matter_fruit: From::from(plant.plant_matter_fruit),
            plant_matter_root: From::from(plant.plant_matter_root),
            plant_leaf_area_index: From::from(plant.plant_leaf_area_index),
        };
        let soil = SoilDataSetBuilder::load(&op.join("soil.out"))?;
        let soil = SoilDataSet {
            day_of_year: From::from(soil.day_of_year),
            soil_daily_runoff: From::from(soil.soil_daily_runoff),
            soil_daily_infiltration: From::from(soil.soil_daily_infiltration),
            soil_daily_drainage: From::from(soil.soil_daily_drainage),
            soil_evapotranspiration: From::from(soil.soil_evapotranspiration),
            soil_evaporation: From::from(soil.soil_evaporation),
            plant_potential_transpiration: From::from(soil.plant_potential_transpiration),
            soil_water_storage_depth: From::from(soil.soil_water_storage_depth),
            soil_water_profile_ratio: From::from(soil.soil_water_profile_ratio),
            soil_water_deficit_stress: From::from(soil.soil_water_deficit_stress),
            soil_water_excess_stress: From::from(soil.soil_water_excess_stress)
        };
        Ok(Self {
            plant,
            soil
        })
    }

    pub fn into_python(self, py: pyo3::Python) -> pyo3::PyResult<&pyo3::types::PyAny> {
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("plant", self.plant.into_pandas(py)?)?;
        dict.set_item("soil", self.soil.into_pandas(py)?)?;
        Ok(dict)
    }
}

const SET_VARS: [&str; 6] = [
    // weather
    "air__max_temperature",
    "air__min_temperature",
    "rainfall~daily__height",
    "soil_top_level_solar_radiation~net~photosynthetic__energy_flux",
    "soil_top_level_solar_radiation~net__energy_flux",

    // irrigation
    "irrigation~daily__height",
];

const GET_VARS: [&str; 15] = [
    "plant_matter~dry__mass-per-area", // w
    "plant_matter_canopy~dry__mass-per-area", // wc
    "plant_matter_fruit~dry__mass-per-area", // wf
    "plant_matter_root~dry__mass-per-area", // wr
    "air__daily_accumulated_temperature", // int
    "plant__leaf_area_index", // lai

    "soil__one-day_runoff_depth", // rof
    "soil__one-day_infiltration_depth", // inf
    "soil_subsurface__one-day_drainage_depth", // drn
    "plant__one-day_potential_transpiration_depth", // ept
    "soil__one-day_evaporation_depth", // esa
    "plant__one-day_transpiration_depth", // epa
    "soil__water_storage_depth", // swc
    "soil__water_deficit_stress_factor", // swfac1
    "soil__water_excess_stress_factor" // swfac2
];


pub struct SimpleCropConfig {
    pub daily: DailyData,
    pub yearly: YearlyData
}

impl SimpleCropConfig {
    fn save<P: AsRef<Path>>(&self, dir: P) -> io::Result<()> {
        let dp = dir.as_ref().join("data");
        create_dir_all(&dp)?;
        let write_f = |path: &str| File::create(&dp.join(path)).map(|f| BufWriter::new(f)).unwrap();

        let mut weather_buf = write_f("weather.inp");
        self.daily.save_weather(&mut weather_buf)?;

        let mut irrigation_buf = write_f("irrig.inp");
        self.daily.save_irrigation(&mut irrigation_buf)?;

        let mut plant_buf = write_f("plant.inp");
        self.yearly.save_plant_config(&mut plant_buf)?;

        let mut soil_buf = write_f("soil.inp");
        self.yearly.save_soil_config(&mut soil_buf)?;

        let mut simctrl_buf = write_f("simctrl.inp");
        self.yearly.save_simulation_config(&mut simctrl_buf)?;
        Ok(())
    }

    pub fn run(&self, cli_path: impl AsRef<Path>, dir: impl AsRef<Path>) -> io::Result<SimpleCropDataSet> {
        self.save(&dir);
        let r = Command::new(cli_path.as_ref()).current_dir(&dir).spawn()?;
        let data = SimpleCropDataSet::load(&dir)?;
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use crate::model::{SimpleCropConfig, YearlyData, DailyData, PlantDataSetBuilder, SoilDataSetBuilder};

    use chrono::{DateTime, NaiveDateTime, Utc};
    use std::fs::{read_to_string, File};
    use std::io::{Cursor, BufWriter, BufReader, BufRead};
    use std::str;
    use ndarray::Array1;

    #[test]
    fn write_yearly_data() {
        let config = YearlyData::default();
        let mut cur = Cursor::new(Vec::new());
        config.save_plant_config(&mut cur).unwrap();
        let plant_ref_data = read_to_string("../simplecrop/data/plant.inp").unwrap();
        assert_eq!(str::from_utf8(cur.get_ref()).unwrap(), plant_ref_data);

        let mut cur = Cursor::new(Vec::new());
        config.save_simulation_config(&mut cur).unwrap();
        let simctnl_ref_data = read_to_string("../simplecrop/data/simctrl.inp").unwrap();
        assert_eq!(str::from_utf8(cur.get_ref()).unwrap(), simctnl_ref_data);

        let mut cur = Cursor::new(Vec::new());
        config.save_soil_config(&mut cur);
        let soil_ref_data = read_to_string("../simplecrop/data/soil.inp").unwrap();
        assert_eq!(str::from_utf8(cur.get_ref()).unwrap(), soil_ref_data);
    }

    #[test]
    fn write_daily_data() {
        let w = DailyData {
            irrigation: Array1::from(vec![0f32, 1f32]),
            energy_flux: Array1::from(vec![5.1]),
            temp_max: Array1::from(vec![20.0f32]),
            temp_min: Array1::from(vec![4.4f32]),
            rainfall: Array1::from(vec![23.9]),
            photosynthetic_energy_flux: Array1::from(vec![10.7f32])
        };

        let mut cur = Cursor::new(Vec::new());
        w.save_weather(&mut cur);
        assert_eq!(
            str::from_utf8(cur.get_ref()).unwrap(),
            "    1   5.1  20.0   4.4  23.9              10.7\n");

        let mut cur = Cursor::new(Vec::new());
        w.save_irrigation(&mut cur).unwrap();
        assert_eq!(
            "    1  0.0\n    2  1.0\n", str::from_utf8(cur.get_ref()).unwrap());
    }

    #[test]
    fn read_plant_t() {
        let data = PlantDataSetBuilder::load("../simplecrop/output/plant.out").unwrap();
        assert_eq!(data.plant_leaf_count[0], 2.0);
        assert_eq!(data.air_accumulated_temp[0], 0.0);
        assert_eq!(data.plant_matter[0], 0.3);
        assert_eq!(data.plant_matter_canopy[0], 0.25);
        assert_eq!(data.plant_matter_fruit[0], 0.0);
        assert_eq!(data.plant_leaf_area_index[0], 0.01);
    }

    #[test]
    fn read_soil_t() {
        let data = SoilDataSetBuilder::load("../simplecrop/output/soil.out").unwrap();
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
