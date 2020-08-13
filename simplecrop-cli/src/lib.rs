#![cfg_attr(not(debug_assertions), deny(warnings))]

extern crate chrono;
extern crate simplecrop_core;

pub use simplecrop_core::{IrrigationDataset, PlantConfig, SimCtnlConfig, WeatherDataset, SoilConfig, SoilDataSet, SoilResult, PlantResult, PlantDataSet, SimpleCropConfig, SimpleCropDataSet, Weather, Irrigation};
use std::fs::{File, create_dir_all};
use std::path::Path;
use std::io::{Write, BufReader, BufRead, BufWriter};
use std::io;
use std::process::{Command, Child};

pub trait ConfigWriter {
    fn write_all<W: Write>(&self, buf: &mut W) -> io::Result<()>;
}

impl ConfigWriter for IrrigationDataset {
    fn write_all<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        for obs in self.0.iter() {
            let row = format!("{:5}  {:1.1}\n", obs.date.timestamp(), obs.amount);
            buf.write(row.as_bytes())?;
        }
        Ok(())
    }
}

impl ConfigWriter for WeatherDataset {
    fn write_all<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        for obs in self.0.iter() {
            let row = format!(
                "{:5}  {:>4.1}  {:>4.1}  {:>4.1}{:>6.1}              {:>4.1}\n",
                obs.date.timestamp(), obs.srad, obs.tmax, obs.tmin, obs.rain, obs.par);
            buf.write(row.as_bytes())?;
        }
        Ok(())
    }
}

impl ConfigWriter for PlantConfig {
    fn write_all<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        let data = format!(
            " {:>7.4} {:>7.4} {:>7.4} {:>7.4} {:>7.4} {:>7.4} \
            {:>7.4} {:>7.4} {:>7.4} {:>7.4} {:>7.4} {:>7.4} \
            {:>7.4} {:>7.4} {:>7.4} {:>7.4} {:>7.4}\n",
            self.lfmax, self.emp2, self.emp1, self.pd, self.nb, self.rm,
            self.fc, self.tb, self.intot, self.n, self.lai, self.w,
            self.wr, self.wc, self.p1, self.f1, self.sla);
        buf.write(data.as_bytes())?;
        let footer: &'static str = "   Lfmax    EMP2    EMP1      PD      nb      rm      fc      tb   intot       n     lai       w      wr      wc      p1      f1    sla\n";
        buf.write(footer.as_bytes())?;
        Ok(())
    }
}

impl ConfigWriter for SoilConfig {
    fn write_all<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        let data = format!(
            "     {:>5.2}     {:>5.2}     {:>5.2}     {:>7.2}     {:>5.2}     {:>5.2}     {:>5.2}\n",
            self.wpp, self.fcp, self.stp, self.dp, self.drnp, self.cn, self.swc);
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

impl ConfigWriter for SimCtnlConfig {
    fn write_all<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        let data = format!("{:>6} {:>5}\n", self.doyp, self.frop);
        buf.write(data.as_bytes())?;
        let footer: &'static str = "  DOYP  FROP\n";
        buf.write(footer.as_bytes())?;
        Ok(())
    }
}

pub trait ResultLoader {
    fn load<P: AsRef<Path>>(p: P) -> io::Result<Self> where Self: std::marker::Sized;
}

fn deserialize_soil_result(vs: &Vec<&str>) -> Option<SoilResult> {
    let (sdoy, srest) = vs.split_first().unwrap();
    let doy = sdoy.parse::<i32>().ok()?;
    let fs = srest.iter().map(|f| f.parse::<f32>().ok()).collect::<Option<Vec<f32>>>()?;
    if let [srad, tmax, tmin, rain, irr, rof, inf, drn, etp, esa, epa, swc, swc_dp, swfac1, swfac2] = fs[..]
    {
        let sr = SoilResult {
            doy,
            srad,
            tmax,
            tmin,
            rain,
            irr,
            rof,
            inf,
            drn,
            etp,
            esa,
            epa,
            swc,
            swc_dp,
            swfac1,
            swfac2,
        };
        return Some(sr);
    }
    None
}

impl ResultLoader for SoilDataSet {
    fn load<P: AsRef<Path>>(p: P) -> io::Result<Self> {
        let f = File::open(p)?;
        let rdr = BufReader::new(f);
        let mut results = Vec::new();
        for line in rdr.lines().skip(6) {
            let record = line?;
            let data: Vec<&str> = record.split_whitespace().collect();
            if let Some(result) = deserialize_soil_result(&data) {
                results.push(result);
            } else {
                eprintln!("skipping {:?}", data)
            }
        }
        Ok(Self(results))
    }
}

fn deserialize_plant_result(vs: &Vec<&str>) -> Option<PlantResult> {
    let (sdoy, srest) = vs.split_first().unwrap();
    let doy = sdoy.parse::<i32>().ok()?;
    let fs = srest.iter().map(|f| f.parse::<f32>().ok()).collect::<Option<Vec<f32>>>()?;
    if let [n, intc, w, wc, wr, wf, lai] = fs[..] {
        let pr = PlantResult {
            doy,
            n,
            intc,
            w,
            wc,
            wr,
            wf,
            lai,
        };
        return Some(pr);
    }
    None
}

impl ResultLoader for PlantDataSet {
    fn load<P: AsRef<Path>>(p: P) -> io::Result<Self> {
        let f = File::open(p)?;
        let rdr = BufReader::new(f);
        let mut results = Vec::new();
        for line in rdr.lines().skip(9) {
            let record = line.unwrap();
            let data: Vec<&str> = record.split_whitespace().collect();
            if let Some(result) = deserialize_plant_result(&data) {
                results.push(result);
            } else {
                eprintln!("skipping {:?}", data)
            }
        }
        Ok(Self(results))
    }
}

pub trait ConfigSaver {
    fn save<P: AsRef<Path>>(&self, p: P) -> io::Result<()>;
}

impl ConfigSaver for SimpleCropConfig {
    fn save<P: AsRef<Path>>(&self, p: P) -> io::Result<()> {
        let dp = p.as_ref().join("data");
        create_dir_all(&dp)?;
        let write_f = |path: &str| File::create(&dp.join(path)).map(|f| BufWriter::new(f)).unwrap();

        let mut weather_buf = write_f("weather.inp");
        self.weather_dataset.write_all(&mut weather_buf)?;

        let mut irrigation_buf = write_f("irrig.inp");
        self.irrigation_dataset.write_all(&mut irrigation_buf)?;

        let mut plant_buf = write_f("plant.inp");
        self.plant.write_all(&mut plant_buf)?;

        let mut soil_buf = write_f("soil.inp");
        self.soil.write_all(&mut soil_buf)?;

        let mut simctrl_buf = write_f("simctrl.inp");
        self.simctrl.write_all(&mut simctrl_buf)?;
        Ok(())
    }
}

impl ResultLoader for SimpleCropDataSet {
    fn load<P: AsRef<Path>>(p: P) -> io::Result<Self> {
        let op = p.as_ref().join("output");
        create_dir_all(&op)?;
        let plant = PlantDataSet::load(&op.join("plant.out"))?;
        let soil = SoilDataSet::load(&op.join("soil.out"))?;
        Ok(Self {
            plant,
            soil,
        })
    }
}

pub fn execute<P: AsRef<Path>, Q: AsRef<Path>>(cli_path: P, p: Q, cfg: &SimpleCropConfig) -> io::Result<(SimpleCropDataSet, Child)> {
    cfg.save(&p)?;
    let r = Command::new(cli_path.as_ref()).current_dir(&p).spawn()?;
    let data = SimpleCropDataSet::load(&p)?;
    Ok((data, r))
}

pub fn execute_in_tempdir<P: AsRef<Path>>(cli_path: P, cfg: &SimpleCropConfig) -> io::Result<(SimpleCropDataSet, Child)> {
    let dir = tempfile::tempdir()?;
    execute(cli_path, dir, cfg)
}

#[cfg(test)]
mod tests {
    use crate::{ConfigWriter, ResultLoader};
    use simplecrop_core::{Irrigation, IrrigationDataset, PlantConfig, SimCtnlConfig, Weather, WeatherDataset, SoilConfig, SoilResult, PlantResult, SoilDataSet, PlantDataSet};

    use chrono::{DateTime, NaiveDateTime, Utc};
    use std::fs::{read_to_string, File};
    use std::io::{Cursor, BufWriter, BufReader, BufRead};
    use std::str;

    #[test]
    fn write_irrigation() {
        let data = IrrigationDataset(
            vec![
                Irrigation {
                    date: DateTime::from_utc(NaiveDateTime::from_timestamp(87001, 0), Utc),
                    amount: 0f32,
                },
                Irrigation {
                    date: DateTime::from_utc(NaiveDateTime::from_timestamp(87002, 0), Utc),
                    amount: 1f32,
                }
            ]
        );
        let mut cur = Cursor::new(Vec::new());
        data.write_all(&mut cur).unwrap();
        assert_eq!(
            "87001  0.0\n\
            87002  1.0\n", str::from_utf8(cur.get_ref()).unwrap());
    }

    #[test]
    fn write_plantconfig() {
        let config = PlantConfig {
            lfmax: 12.0,
            emp2: 0.64,
            emp1: 0.104,
            pd: 5.0,
            nb: 5.3,
            rm: 0.100,
            fc: 0.85,
            tb: 10.0,
            intot: 300.0,
            n: 2.0,
            lai: 0.013,
            w: 0.3,
            wr: 0.045,
            wc: 0.255,
            p1: 0.03,
            f1: 0.028,
            sla: 0.035,
        };
        let mut cur = Cursor::new(Vec::new());
        config.write_all(&mut cur).unwrap();
        let plant_ref_data = read_to_string("data/plant.inp").unwrap();
        assert_eq!(str::from_utf8(cur.get_ref()).unwrap(), plant_ref_data);
    }

    #[test]
    fn write_simctnl() {
        let simctnl = SimCtnlConfig {
            doyp: 121,
            frop: 3,
        };
        let mut cur = Cursor::new(Vec::new());
        simctnl.write_all(&mut cur).unwrap();
        let simctnl_ref_data = read_to_string("data/simctrl.inp").unwrap();
        assert_eq!(str::from_utf8(cur.get_ref()).unwrap(), simctnl_ref_data);
    }

    #[test]
    fn write_soil() {
        let soil = SoilConfig {
            wpp: 0.06,
            fcp: 0.17,
            stp: 0.28,
            dp: 145.00,
            drnp: 0.10,
            cn: 55.00,
            swc: 246.50,
        };
        let mut cur = Cursor::new(Vec::new());
        soil.write_all(&mut cur);

        let soil_ref_data = read_to_string("data/soil.inp").unwrap();
        assert_eq!(str::from_utf8(cur.get_ref()).unwrap(), soil_ref_data);
    }

    #[test]
    fn write_weather() {
        let w = Weather {
            date: DateTime::from_utc(NaiveDateTime::from_timestamp(87001, 0), Utc),
            srad: 5.1,
            tmax: 20.0,
            tmin: 4.4,
            rain: 23.9,
            par: 10.7,
        };
        let wd = WeatherDataset(vec![w]);

        let mut cur = Cursor::new(Vec::new());
        wd.write_all(&mut cur);

        assert_eq!(
            str::from_utf8(cur.get_ref()).unwrap(),
            "87001   5.1  20.0   4.4  23.9              10.7\n")
    }

    #[test]
    fn read_plant_t() {
        let data = PlantDataSet::load("../simplecrop/output/plant.out").unwrap();
        let comparison = PlantResult {
            doy: 121,
            n: 2.0,
            intc: 0.0,
            w: 0.3,
            wc: 0.25,
            wr: 0.05,
            wf: 0.0,
            lai: 0.01,
        };
        assert_eq!(data.0[0], comparison);
    }

    #[test]
    fn read_soil_t() {
        let data = SoilDataSet::load("../simplecrop/output/soil.out").unwrap();
        let comparison = SoilResult {
            doy: 3,
            srad: 12.1,
            tmax: 14.4,
            tmin: 1.1,
            rain: 0.0,
            irr: 0.0,
            rof: 0.0,
            inf: 0.0,
            drn: 1.86,
            etp: 2.25,
            esa: 2.23,
            epa: 0.02,
            swc: 260.97,
            swc_dp: 1.8,
            swfac1: 1.0,
            swfac2: 1.0,
        };
        assert_eq!(data.0[0], comparison);
    }
}
