from meillionen import PyFuncInterface, PyFuncRequest, PyStoreRef

import sh


def get_interface(cli_path) -> PyFuncInterface:
    cli = sh.Command(cli_path)
    return PyFuncInterface.from_json(cli('interface'))


def run_shell():
    interface = get_interface('../simplecrop/target/cli/simplecrop')
    req = PyFuncRequest()
    req.set_source('daily', PyStoreRef.)
    interface.call_cli()