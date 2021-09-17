# mi = MethodInterface(
#     'run',
#     sources=[
#
#     ],
#     sinks=[
#
#     ],
#     handler=lambda x: x
# )
# mi(sinks={'daily': Feather(path='foo.feather')}, sources={'soil': Parquet(path='bar.parquet')})