struct PlantModel {
    float e;
    float fc;
    float lai;
    float nb;
    float n;
    float pt;
    float pg;
    float di;
    float par;

    float rm;
    float dwf;
    float intc;
    float tmax;
    float tmin;
    float p1;
    float sla;

    float pd;
    float emp1;
    float emp2;
    float lfmax;
    float dwc;
    float tmn;

    float dwr;
    float dw;
    float dn;
    float w;
    float wc;
    float wr;
    float wf;
    float tb;
    float intot;
    float dlai;
    float fl;

    float swfac1;
    float swfac2;

    int doy;
    int endsim;
    int count;
};

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
