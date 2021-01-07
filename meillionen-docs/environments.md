Interface
---------

### List

#### Python

```python
import model_library

model_library.list()
```

#### Web API

```bash
http https://localhost:3000
```

#### CLI

```bash
model-library list
```

Returns a list of model component constructors (as json) contained in the model library.

```json
{
  "models": [
    {
      "name": "Crop",
      "constructor": {
        "args": [
          {
            "name": "plant_leaves_max_number",
            "type": "f32"
          },
          {
            "name": "plant_emp2",
            "type": "f32"
          }
        ]
      }
    }
  ]
}
```

### Construct


#### Python

```python
import model_library

crop = model_library.Crop("yearly.parquet")
```

#### Web API

```bash
http https://localhost:3000/ config@yearly.parquet
```

#### CLI

```bash
model-library construct crop --config yearly.parquet
```

Returns a reference to the serialized model state and the interface

*Model State Reference*

```json
{
  "path": "output/crop/state/1.pickle"
}
```

*Interface*

```json
{
  "input": {
    "type": "dataframe",
    "cols": [
      {
        "name": "irrigation",
        "type": "f32",
        "units": "mm/day"
      },
      {
        "name": "temp_max",
        "type": "f32",
        "units": "celsius"
      },
      {
        "name": "temp_min",
        "type": "f32",
        "units": "celsius"
      },
      {
        "name": "rainfall",
        "type": "f32",
        "units": "mm/day"
      },
      {
        "name": "photosynthetic_energy_flux",
        "type": "f32"
      },
      {
        "name": "energy_flux",
        "type": "f32"
      }
    ]
  },
  "output": {
    "type": "dataframe",
    "cols": [
      {
        "name": "day_of_year", 
        "type": "i32"
      },
      {
        "name": "soil_daily_runoff",
        "type": "f32"
      },
      {
        "name": "plant_matter",
        "type": "f32"
      },
      {
        "name": "plant_matter_canopy",
        "type": "f32"
      },
      {
        "name": "plant_matter_fruit",
        "type": "f32"
      },
      {
        "name": "plant_matter_root",
        "type": "f32"
      },
      {
        "name": "plant_leaf_area_index",
        "type": "f32"
      }
    ]
  }
}
```

### Step

#### Python

```python
import model_library

crop = model_library.Crop("yearly.parquet")
crop.initialize()
```

#### Web API

```bash
http https://localhost:3000/step state=output/crop/state/1.pickle input@daily.parquet
```

#### CLI

```bash
model-library step crop --state output/crop/state/1.pickle --input daily.parquet
```

Returns a reference to the new serialized model state

```json
{
  "path": "output/crop/state/2.pickle"
}
```