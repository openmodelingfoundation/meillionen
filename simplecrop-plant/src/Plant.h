struct PlantInput {
    int doy;
    int endsim;
    float tmax;
    float tmin;
    float par;
    float swfac1;
    float swfac2;
    float lai;
};

void plant_initialize(struct PlantInput* input);
void plant_rate(struct PlantInput* input);
void plant_integ(struct PlantInput* input);
void plant_output(struct PlantInput* input);
void plant_close(struct PlantInput* input);
