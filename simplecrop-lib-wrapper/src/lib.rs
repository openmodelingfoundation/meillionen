#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

trait DTSSMealy {
    type Input;
    type Output;

    fn update(self: &mut Self, input: &Self::Input) -> Self::Output;
}

trait DTSSMoore {
    type Input;
    type Output;

    fn update(self: &mut Self, input: &Self::Input);
    fn output(self: &Self) -> Self::Output;
}

impl DTSSMealy for PlantModel {
    type Input = PlantInput;
    type Output = f32;

    fn update(&mut self, input: &Self::Input) -> Self::Output {
        unsafe {
            pm_update(&mut *self, input);
            pm_rate(&mut *self);
            pm_integ(&mut *self);
        }
        self.lai
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open() {
        let mut model = PlantModel::default();
        model.lfmax = 12.0f32;
        model.emp2 = 0.64f32;
        model.emp1 = 0.104f32;
        model.pd = 5.0f32;
        model.nb = 5.3f32;
        model.rm = 0.1f32;
        model.fc = 0.85f32;
        model.tb = 10.0f32;
        model.intot = 300.0f32;
        model.n = 2f32;
        model.lai = 0.013f32;
        model.w = 0.3f32;
        model.wr = 0.045f32;
        model.wc = 0.255f32;
        model.p1 = 0.03f32;
        model.fc = 0.028f32;
        model.sla = 0.035;
        let mut input = PlantInput::default();
        input.tmax = 20.0;
        input.tmin = 4.4;
        let lai = model.update(&input);
        assert_eq!(lai, 0.013f32);
    }
}
