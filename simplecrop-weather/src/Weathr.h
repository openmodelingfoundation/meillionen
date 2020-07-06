struct WeatherInput {
    float srad;
    float tmax;
    float tmin;
    float rain;
    float par;
};

void weather_initialize(struct WeatherInput* input);

void weather_rate(struct WeatherInput* input);

void weather_close(struct WeatherInput* input);
