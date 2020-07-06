#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

pub fn initialize(input: &mut SoilInput) {
    unsafe {
        soil_initialize(input);
    }
}

pub fn rate(input: &mut SoilInput) {
    unsafe {
        soil_rate(input);
    }
}

pub fn integ(input: &mut SoilInput) {
    unsafe {
        soil_integ(input);
    }
}

pub fn output(input: &mut SoilInput) {
    unsafe {
        soil_output(input);
    }
}

pub fn close(input: &mut SoilInput) {
    unsafe {
        soil_close(input);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open() {
        let mut input = SoilInput {
            doy: 0,
            lai: 0f32,
            rain: 0f32,
            srad: 0f32,
            tmax: 1f32,
            tmin: 0f32,
            swfac1: 0f32,
            swfac2: 0f32
        };
        unsafe {
            soil_initialize(&mut input);
        }
        unsafe {
            soil_close(&mut input);
        }
    }
}
