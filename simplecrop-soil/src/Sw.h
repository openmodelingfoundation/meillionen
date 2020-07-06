struct SoilInput {
    int doy;
    float lai;
    float rain;
    float srad;
    float tmax;
    float tmin;
    float swfac1;
    float swfac2;
};

void soil_initialize(struct SoilInput* input);
void soil_rate(struct SoilInput* input);
void soil_integ(struct SoilInput* input);
void soil_output(struct SoilInput* input);
void soil_close(struct SoilInput* input);