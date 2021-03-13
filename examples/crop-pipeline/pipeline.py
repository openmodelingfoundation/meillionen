from meillionen import FuncInterface, FuncRequest, StoreRef
import numpy as np
import pandas as pd

import sh


def get_interface(cli_path) -> FuncInterface:
    cli = sh.Command(cli_path)
    return FuncInterface.from_json(cli('interface'))


def run_shell():
    daily = pd.DataFrame({
        'temp_max': np.array([]),
        'temp_min': np.array([]),
        'photosynthetic_eneergy_flux': np.array([]),
        'energy_flux': np.array([])
    })
    overlandflow = get_interface('overlandflow/model.py')
    ov_res = overlandflow(run_id='1')

    daily = daily.join(ov_res.get_sink('irrigation'))

    simplecrop = get_interface('simplecrop/cli.py')
    sc_res = simplecrop(run_id='1', sources={'daily': daily})

    print(sc_res.get_sink('crop_yields'))