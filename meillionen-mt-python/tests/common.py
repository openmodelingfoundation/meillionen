import pyarrow as pa
from meillionen.interface.class_interface import ClassInterface
from meillionen.interface.method_interface import MethodInterface
from meillionen.interface.module_interface import ModuleInterface
from meillionen.interface.method_request import MethodRequest
from meillionen.interface.resource import Feather
from meillionen.interface.mutability import Mutability
from meillionen.interface.schema import PandasHandler
from meillionen.server import Server

mi = MethodInterface(
    'run',
    args=[
        PandasHandler(
            name='daily',
            s=pa.schema([
                ('irrigation', pa.float32()),
                ('temp_max', pa.float32()),
                ('temp_min', pa.float32())
            ]),
            mutability=Mutability.read
        ),
        PandasHandler(
            name='yearly',
            s=pa.schema([
                ('plant_leaves_max_number', pa.float32()),
                ('plant_nb', pa.float32())
            ]),
            mutability=Mutability.read
        ),
        PandasHandler(
            name='soil',
            s=pa.schema([
                ('day_of_year', pa.float32()),
                ('soil_daily_runoff', pa.float32())
            ]),
            mutability=Mutability.write
        )
    ])
ci = ClassInterface(
    'simplecrop',
    methods={
        m.name: m for m in [mi]
    }
)
mod_int = ModuleInterface([
    ci
])


mr = MethodRequest(
    class_name='simplecrop',
    method_name='run',
    kwargs={
        'daily': Feather('daily.feather'),
        'yearly': Feather('yearly.feather'),
        'soil': Feather('soil.feather')
    }
)