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
};

void pm_update(struct PlantModel* model, struct PlantInput const* input);
void pm_rate(struct PlantModel* model);
void pm_integ(struct PlantModel* model);
