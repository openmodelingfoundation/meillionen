#![cfg_attr(not(debug_assertions), deny(warnings))]

extern crate chrono;
extern crate simplecrop_core;

use simplecrop_core::{IrrigationDataset, PlantConfig, SimCtnlConfig, WeatherDataset, SoilConfig, SoilDataSet, SoilResult, PlantResult};
use std::fs::File;
use std::path::PathBuf;
use std::io::{Write, BufReader, BufRead};
use std::io;
use std::ops::Range;

trait ConfigWriter {
    fn write_all<W: Write>(&self, buf: &mut W) -> io::Result<()>;
}

impl ConfigWriter for IrrigationDataset {
    fn write_all<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        for obs in self.data.iter() {
            let row = format!("{:5}  {:1.1}\n", obs.date.timestamp(), obs.amount);
            buf.write(row.as_bytes())?;
        }
        Ok(())
    }
}

impl ConfigWriter for WeatherDataset {
    fn write_all<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        for obs in self.data.iter() {
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

fn read_soil() {
    let f = File::open("../simplecrop/output/soil.out").unwrap();
    let mut rdr = BufReader::new(f);
    let mut results = Vec::new();
    for line in rdr.lines().skip(6) {
        let record = line.unwrap();
        let data: Vec<&str> = record.split_whitespace().collect();
        if let Some(result) = deserialize_soil_result(&data) {
            results.push(result);
        } else {
            println!("skipping {:?}", data)
        }
    }
    println!("{:?}", results);
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
            lai
        };
        return Some(pr)
    }
    None
}

fn read_plant() {
    let f = File::open("../simplecrop/output/plant.out").unwrap();
    let mut rdr = BufReader::new(f);
    let mut results = Vec::new();
    for line in rdr.lines().skip(9) {
        let record = line.unwrap();
        let data: Vec<&str> = record.split_whitespace().collect();
        if let Some(result) = deserialize_plant_result(&data) {
            results.push(result);
        } else {
            println!("skipping {:?}", data)
        }
    }
    println!("{:?}", results);
}

#[cfg(test)]
mod tests {
    use crate::{ConfigWriter, read_soil, read_plant};
    use simplecrop_core::{Irrigation, IrrigationDataset, PlantConfig, SimCtnlConfig, Weather, WeatherDataset, SoilConfig};

    use chrono::{DateTime, NaiveDateTime, Utc};
    use std::fs::{read_to_string, File};
    use std::io::{Cursor, BufWriter, BufReader, BufRead};
    use std::str;

    #[test]
    fn write_irrigation() {
        let data = IrrigationDataset {
            data: vec![
                Irrigation {
                    date: DateTime::from_utc(NaiveDateTime::from_timestamp(87001, 0), Utc),
                    amount: 0f32,
                },
                Irrigation {
                    date: DateTime::from_utc(NaiveDateTime::from_timestamp(87002, 0), Utc),
                    amount: 1f32,
                }
            ]
        };
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
            wpp:   0.06,
            fcp:   0.17,
            stp:   0.28,
            dp:  145.00,
            drnp:  0.10,
            cn:   55.00,
            swc: 246.50
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
            srad:  5.1,
            tmax: 20.0,
            tmin:  4.4,
            rain: 23.9,
            par: 10.7,
        };
        let wd = WeatherDataset { data: vec![w] };

        let mut cur = Cursor::new(Vec::new());
        wd.write_all(&mut cur);

        assert_eq!(
            str::from_utf8(cur.get_ref()).unwrap(),
            "87001   5.1  20.0   4.4  23.9              10.7\n")
    }

    #[test]
    fn read_plant_t() {
        read_plant();
    }

    #[test]
    fn read_soil_t() {
        read_soil();
    }
}
