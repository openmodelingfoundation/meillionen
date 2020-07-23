#![cfg_attr(not(debug_assertions), deny(warnings))]

extern crate chrono;

use std::path::PathBuf;
use std::fs::File;
use chrono::prelude::{DateTime, Utc};
use std::io::{Write};
use std::io;

#[derive(Debug, PartialEq)]
struct Irrigation {
    date: DateTime<Utc>,
    amount: f32,
}

pub struct IrrigationDataset {
    data: Vec<Irrigation>
}

impl IrrigationDataset {
    fn write_all<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        for obs in self.data.iter() {
            let row = format!("{:5}  {:1.1}\n", obs.date.timestamp(), obs.amount);
            buf.write(row.as_bytes())?;
        }
        Ok(())
    }

    pub fn to_file(&self, base: &PathBuf) -> io::Result<()> {
        let p = base.join("data/irrig.inp");
        let mut file = File::create(p)?;
        self.write_all(&mut file)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub struct PlantConfig {
    lfmax: f32,
    emp2: f32,
    emp1: f32,
    pd: f32,
    nb: f32,
    rm: f32,
    fc: f32,
    tb: f32,
    intot: f32,
    n: f32,
    lai: f32,
    w: f32,
    wr: f32,
    wc: f32,
    p1: f32,
    f1: f32,
    sla: f32,
}

impl PlantConfig {
    fn write_all<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        let data = format!(
            " {:>7.1} {:>7.2} {:>7.3} {:>7.1} {:>7.1} {:>7.3} \
            {:>7.2} {:>7.1} {:>7.1} {:>7.1} {:>7.3} {:>7.1} \
            {:>7.3} {:>7.3} {:>7.2} {:>7.3} {:>6.3}\n",
            self.lfmax, self.emp2, self.emp1, self.pd, self.nb, self.rm, self.fc, self.tb, self.intot, self.n, self.lai, self.w, self.wr, self.wc, self.p1, self.f1, self.sla);
        buf.write(data.as_bytes())?;
        let footer: &'static str = "   Lfmax    EMP2    EMP1      PD      nb      rm      fc      tb   intot       n     lai       w      wr      wc      p1      f1    sla\n";
        buf.write(footer.as_bytes())?;
        Ok(())
    }

    pub fn to_file(&self, base: &PathBuf) -> io::Result<()> {
        let p = base.join("data/plant.inp");
        let mut file = File::create(p)?;
        self.write_all(&mut file)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub struct SimCtnlConfig {
    doyp: i32,
    frop: i32
}

impl SimCtnlConfig {
    fn write_all<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        let data = format!("{:>6} {:>5}\n", self.doyp, self.frop);
        buf.write(data.as_bytes())?;
        let footer: &'static str = "  DOYP  FROP\n";
        buf.write(footer.as_bytes())?;
        Ok(())
    }

    pub fn to_file(&self, base: &PathBuf) -> io::Result<()> {
        let p = base.join("data/simctrl.inp");
        let mut file = File::create(p)?;
        self.write_all(&mut file)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{IrrigationDataset, Irrigation, PlantConfig, SimCtnlConfig};
    use chrono::{DateTime, NaiveDateTime, Utc};
    use std::fs::read_to_string;
    use std::io::Cursor;
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
            sla: 0.035
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
            frop: 3
        };
        let mut cur = Cursor::new(Vec::new());
        simctnl.write_all(&mut cur).unwrap();
        let simctnl_ref_data = read_to_string("data/simctrl.inp").unwrap();
        assert_eq!(str::from_utf8(cur.get_ref()).unwrap(), simctnl_ref_data);
    }
}
