#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

pub fn initialize(input: &mut WeatherInput) {
    unsafe {
        weather_initialize(input);
    }
}

pub fn rate(input: &mut WeatherInput) {
    unsafe {
        weather_rate(input);
    }
}

pub fn close(input: &mut WeatherInput) {
    unsafe {
        weather_close(input);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open() {
        let mut input = WeatherInput {
            srad: 0.,
            tmax: 1.,
            tmin: 0.,
            rain: 1.5,
            par: 0.
        };
        unsafe {
            weather_initialize(&mut input);
        }
        assert_eq!(input.srad,  0f32);
        assert_eq!(input.tmax, 1f32);
        assert_eq!(input.tmin,  0f32);
        assert_eq!(input.rain, 1.5f32);
        assert_eq!(input.par,  0f32);
        unsafe {
            weather_rate(&mut input);
        }
        assert_eq!(input.srad,  5.1f32);
        assert_eq!(input.tmax, 20.0f32);
        assert_eq!(input.tmin,  4.4f32);
        assert_eq!(input.rain, 23.9f32);
        assert_eq!(input.par,  input.srad * 0.5);
        unsafe {
            weather_close(&mut input);
        }
    }
}
