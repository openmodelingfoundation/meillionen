#!/usr/bin/env python

from meillionen import PyFuncInterface


func = PyFuncInterface.from_json('''{
    "name": "simplecrop",
    "sources": {
        "daily": {
            "description": "daily data",
            "datatype": {
                "Table": {
                    "day_of_year": "I64",
                    "temp_min": "F64",
                    "temp_max": "F64",
                    "rainfall": "F64"
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


if __name__ == '__main__':
	args = func.to_cli()
	print("Args\n")
	print(args)

