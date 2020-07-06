#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

pub fn initialize(input: &mut PlantInput) {
    unsafe {
        plant_initialize(input);
    }
}

pub fn rate(input: &mut PlantInput) {
    unsafe {
        plant_rate(input);
    }
}

pub fn integ(input: &mut PlantInput) {
    unsafe {
        plant_integ(input);
    }
}

pub fn output(input: &mut PlantInput) {
    unsafe {
        plant_output(input);
    }
}

pub fn close(input: &mut PlantInput) {
    unsafe {
        plant_close(input);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open() {
        let mut input = PlantInput {
            doy: 0,
            endsim: 0,
            tmax: 0f32,
            tmin: 0f32,
            par: 0f32,
            swfac1: 0f32,
            swfac2: 0f32,
            lai: 0f32
        };
        unsafe {
            plant_initialize(&mut input);
        }
        assert_eq!(input.lai, 0.013f32);
        unsafe {
            plant_close(&mut input);
        }
    }
}
