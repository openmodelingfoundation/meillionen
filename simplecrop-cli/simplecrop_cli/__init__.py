from .simplecrop_cli import CDFStore, F64CDFVariableRef, run, run_cli, to_dataframe, SimpleCrop

SCHEMA_GET_LOOKUP = {
    'f64': 'get_f64_value',
    'i64': 'get_i64_value',
    'str': 'get_str_value'
}


class CDFStoreConfig:
    def __init__(self, path):
        self.path = path

    @property
    def storetype(self):
        return "netcdf"


class Store:
    def __init__(self, config):
        assert config.storetype == "netcdf"
        self.store = CDFStore(config.path)

    def __getitem__(self, item):
        return VariableRef(self.store.get_f64_value(item))

    def get_value(self, variable_name):
        # when more than one datatype is supported this function
        # will get the datatype of the variable name from the store
        # and then choose the appropriate getter for the type
        return VariableRef(self.store.get_f64_value(variable_name))


class VariableRef:
    def __init__(self, variable_ref):
        self.variable_ref = variable_ref

if __name__ == '__main__':
    run_cli()