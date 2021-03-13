import unittest
from meillionen import FuncInterface


class TestPyFuncInterface(unittest.TestCase):
    def setUp(self) -> None:
        func = FuncInterface.from_json('''{
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