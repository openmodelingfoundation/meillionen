use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::DataType;
use arrow::datatypes::DateUnit;
use lazy_static::lazy_static;
use std::sync::Arc;
use std::path::Path;
use std::io::Write;
use std::io;
use arrow::array::{Float32Array, Date32Array};
use chrono::{DateTime, Utc};

pub trait ConfigWriter {
    fn write_all<W: Write>(&self, buf: &mut W) -> io::Result<()>;
}

lazy_static! {
    static ref IRRIGATION_FIELDS: Vec<Field> =
        vec![
            Field::new("date", DataType::Date32(DateUnit::Day), false),
            Field::new("amount", DataType::Float32, false)
        ];
}

pub struct IrrigationDataFrame(RecordBatch);

impl IrrigationDataFrame {
    fn try_new(cols: Vec<arrow::array::ArrayRef>) -> Result<Self, arrow::error::ArrowError> {
        Ok(IrrigationDataFrame(RecordBatch::try_new(
            Arc::new(Schema::new(IRRIGATION_FIELDS.to_vec())),
            cols
        )?))
    }
}

impl ConfigWriter for IrrigationDataFrame {
    fn write_all<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        let date: &[i32] = self.0.column(0).as_any().downcast_ref::<Date32Array>()
            .map(|da| da.value_slice(0, da.len())).unwrap();
        let amount = self.0.column(1).as_any().downcast_ref::<Float32Array>()
            .map(|fa| fa.value_slice(0, fa.len())).unwrap();
        let nrows = self.0.num_rows();

        for row in 0..nrows {
            let row = format!("{:5}  {:1.1}\n", date[row], amount[row]);
            buf.write(row.as_bytes())?;
        }
        Ok(())
    }
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

pub struct WeatherDataFrame(RecordBatch);

impl WeatherDataFrame {
    fn try_new(cols: Vec<arrow::array::ArrayRef>) -> Result<WeatherDataFrame, arrow::error::ArrowError> {
        Ok(WeatherDataFrame(RecordBatch::try_new(
            Arc::new(Schema::new(WEATHER_FIELDS.to_vec())),
        cols)?))
    }
}

impl ConfigWriter for WeatherDataFrame {
    fn write_all<W: Write>(&self, buf: &mut W) -> io::Result<()> {
        let (date, cols) = &self.0.columns().split_first().unwrap();
        let date = date.as_any().downcast_ref::<Date32Array>()
            .map(|da| da.value_slice(0, da.len())).unwrap();
        let cols = cols.iter()
            .map(|c| c.as_any().downcast_ref::<Float32Array>()
                .map(|fa| fa.value_slice(0, fa.len())))
            .collect::<Option<Vec<&[f32]>>>().unwrap();
        let nrows = self.0.num_rows();
        for row in 0..nrows {
            let text = format!(
                "{:5}  {:>4.1}  {:>4.1}  {:>4.1}{:>6.1}              {:>4.1}\n",
                date[row],
                cols[0][row],
                cols[1][row],
                cols[2][row],
                cols[3][row],
                cols[4][row]);
            buf.write(text.as_bytes())?;
        }
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use arrow::record_batch::RecordBatch;
    use arrow::array::{Date32Array, Float32Array, PrimitiveBuilder, PrimitiveArray};
    use std::sync::Arc;
    use std::io::Cursor;
    use std::str;
    use super::*;
    use arrow::datatypes::ArrowPrimitiveType;
    use std::borrow::BorrowMut;

    fn arr_new<T: ArrowPrimitiveType>(mut b: PrimitiveBuilder<T>, values: &[T::Native]) -> PrimitiveArray<T> {
        b.append_slice(values).unwrap();
        b.finish()
    }

    #[test]
    fn write_irrigation() {
        let amount = arr_new(Float32Array::builder(2), &[0f32, 1f32]);
        let date = arr_new(Date32Array::builder(2), &[87001, 87002]);
        let data = IrrigationDataFrame::try_new(vec![
            Arc::new(date),
            Arc::new(amount)
        ]).unwrap();
        let mut cur = Cursor::new(Vec::new());
        data.write_all(&mut cur).unwrap();
        assert_eq!(
            "87001  0.0\n\
            87002  1.0\n", str::from_utf8(cur.get_ref()).unwrap());
    }

    #[test]
    fn write_weather() {
        let date = arr_new(Date32Array::builder(1), &[87001]);
        let solar_radiation = arr_new(Float32Array::builder(1), &[5.1f32]);
        let temp_max = arr_new(Float32Array::builder(1), &[20.0f32]);
        let temp_min = arr_new(Float32Array::builder(1), &[4.4f32]);
        let rain = arr_new(Float32Array::builder(1), &[23.9f32]);
        let par = arr_new(Float32Array::builder(1), &[10.7f32]);

        let data = WeatherDataFrame::try_new(vec![
            Arc::new(date),
            Arc::new(solar_radiation),
            Arc::new(temp_max),
            Arc::new(temp_min),
            Arc::new(rain),
            Arc::new(par)
        ]).unwrap();
        let mut cur = Cursor::new(Vec::new());
        data.write_all(&mut cur).unwrap();

        assert_eq!(
            str::from_utf8(cur.get_ref()).unwrap(),
            "87001   5.1  20.0   4.4  23.9              10.7\n")
    }
}