# Building a Wrapper for Overland Flow

Here is a wrapper for the overland flow model

```{eval-rst}
.. literalinclude:: ../../examples/crop-pipeline/overlandflow/model.py
```

There are few things to take note of:

1. The interface definition. It describes the schemas that sinks and sources must conform to. For the overland flow this means access to a rainfall dataframe with a column called `rainfall__depth` that is the miilimeters of daily precipitation
