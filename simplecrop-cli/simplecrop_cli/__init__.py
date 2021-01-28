from meillionen import PyFuncInterface
from .simplecrop_cli import run


interface = PyFuncInterface.from_json('''{
    "name": "simplecrop",
    "sources": {
        "daily": {
            "description": "daily data",
            "datatype": {
                "Table": {
                     "irrigation": "F64",
                     "temp_max": "F64",
                     "temp_min": "F64",
                     "rainfall": "F64",
                     "photosynthetic_energy_flux": "F64",
                     "energy_flux": "F64"
                }
            }
        }
    },
    "sinks": {
        "crop_yields": {
            "description": "crop yields",
            "datatype": {
                "Table": {
                    "day_of_year": "I64",
                    "yield": "F64"
                }
            }
        }
    }
}''')
