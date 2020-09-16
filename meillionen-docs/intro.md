Open Modeling Foundation
========================

The Open Modeling Foundation (OMF) is an alliance of modeling organizations that coordinates and administers a common, community developed body of standards and best practices among diverse communities of modeling scientists.

The OMF aims to provide standards and guidelines for

- [Accessibility](https://openmodelingfoundation.github.io/standards/) - metadata, data and source code storage and retrieval standards
- [Documentation](https://openmodelingfoundation.github.io/standards/documentation/) - what needs to included in the description of a model (and how to extend the documentation standards)
- [Interoperability](https://openmodelingfoundation.github.io/standards/interoperability/) - how to develop model components that allow for easy integration with other model components 
- [Reproducibility](https://openmodelingfoundation.github.io/standards/reproducibility/) - how to make a model analysis repeatable given the same code, dependencies and data inputs

It also works to achieve to make adhering to those standards easier by developing

- teaching materials
- libraries for model interoperability and reproducibility

Modeling Data Interoperability Toolkit
--------------------------------------

Computational models for hydrology, ecology, social systems and others areas are written in a variety of frameworks and languages. The OMF intends to make the use of model written in other frameworks and languages easier by providing a cross-language data communication library. The data communication library aims to

1. Reduce the burden of coupling computational models written in different frameworks and languages for model consumers
2. Have a low barrier to entry for model developers to make their models couple easily with other standards adopting models
3. Support for basic metadata standards so that modellers can find models they're interested in.
4. Agnostic to execution strategy - allow for use in workflow manager for DAG jobs, support models with two-way coupling as well
5. Only deal with data access to make it possible to integrate with multiple frameworks. 

The pre-alpha [meillionen](https://github.com/openmodelingfoundation/meillionen) library is in the early stages of attempting to address those goals when used alongside an existing modelling toolkit like [PyMT](https://pymt.readthedocs.io/en/latest/). A more complete version of `meillionen` would have

- A data access library that can be embedded in Python (and eventually other languages like R and Julia) to facilitate passing data between model components. At minimum it would need to support passing tabular and tensor data by file, network memory
- Support reading and writing data
- Support units of measure and storage type conversion
- Case Studies to show how to couple models and how to adapt existing models into the framework
- Documentation for how to use with existing and new models
- Documentation for how to use with different compute environments
  - high performance / high throughput computing (using schedulers like Slurm, Condor etc)
  - Desktop
  - Docker and Singularity containers

### Example: Coupling SimpleCrop with OverlandFlow

A weather dataset that has daily min and max temperatures as well as daily rain
fall.

[SimpleCrop](https://github.com/openmodelingfoundation/SimpleCrop) is a
simplified version of [DSSAT](https://dssat.net). It takes in weather and soil
time series as well as a plant characteristic configuration and returns a time
series of plant characteristic data. It is not spatial.

[OverlandFlow](https://landlab.readthedocs.io/en/latest/reference/components/overland_flow.html)
is an approximation of shallow water flow. It routes water from a precipitation
model over a topographic landscape. It has a very fine timescale (the second
level) compared to SimpleCrop and is spatially explicit.

In order to couple `OverlandFlow` with `SimpleCrop` we need to have figure out what `OverlandFlow` outputs to connect with `SimpleCrop` inputs. From inspecting the overland flow model it is apparent that we can feed simplecrops `surface_water__depth` output to Simple Crop's `rainfall__depth` input. However, there are still some dimension mismatches in connecting the models. The overland flow model has an `x` dimension and a `y` dimension but the `time` dimension is missing because water the overland flow model models individual rainfall events which need to be aggregated up to daily value to get a daily amount of water fed to the crops occupying a particular cell. The Simple Crop `rainfall__depth` input has a `time` dimension but no `x` and `y` dimensions . In order to reconcile these dimension mismatches we have to run the overland flow model once for each day to feed into Simple Crop. For Simple Crop to be able to be compatible we need to run the model at `x`, `y` coordinate pair.

The overall code need to couple the overland flow model with the SimpleCrop model is shown below (you can try out the [notebook iteractively]( http://13.56.188.168/hub/user-redirect/git-pull?repo=https%3A%2F%2Fgithub.com%2Fopenmodelingfoundation%2Fmeillionen&urlpath=tree%2Fmeillionen%2Fnotebooks%2FModel.ipynb&branch=master) for iEMSs). Components match the interfaces used by the PyMT library. Each component has getters and setters to allow the passing of data from one model component to another. Component setter names are supposed to follow [CSDMS standard name](https://csdms.colorado.edu/wiki/CSDMS_Standard_Names) practices.

```python
from landlab.components.overland_flow import Overland
import simplecrop_cli

days_with_rain = get_days_with_rain_data()

of = Overland()
of.set_value('rainfall__depth', days_with_rain)
of.initialize()
of.update()
of.finalize()

sc = simplecrop_cli.SimpleCrop()
sc.initialize()
sc.set_value('infiltration_water__depth', of.get_value('surface_water__depth'))
sc.update()
sc.finalize()
```

Data passing here follows PyMT conventions. Data flows from the overland flow model to the SimpleCrop model by getting the `surface_water__depth` from the overland flow model and setting it to the SimpleCrop model's `infiltration_water__depth`  variable. The SimpleCrop model performs automatic broadcasting on th dimensions of the `surface_water__depth` variable passed in when running SimpleCrop. Since the output `surface_water__depth` variable from overland flow has `x`, `y`, and `time` dimensions and the SimpleCrop model input expects data with only a `time` dimension the SimpleCrop class must broadcast over the `x`, `y` dimensions and run the SimpleCrop model for each `x` and `y`. 

The current structure of SimpleCrop requires that a year's worth of data. If SimpleCrop could take a day's worth of data at a time then it should be possible to have an example of using SimpleCrop in a double coupling context where evaporation from the plants has an impact on the weather. This would benefit from a different store class that allowed passing data between components over the network. Apache Arrow supports passing tensors and tabular data so it seems like it would be a good choice making it possible to pass the same data the surface water depth by day vectors that are currently needed by the SimpleCrop model.

Suppose that you wanted to use a different water accumulation method for taking a topography and precipitation data to return a the amount of water that infiltrated the soil for all cells in the study area. If GRASS's `r.watershed` model had a PyMT wrapper it could look something like

```python
from grass import Watershed
import simplecrop_cli

days_with_rain = get_days_with_rain_data()

ws = Watershed()
ws.set_value('rainfall__depth', days_with_rain)
ws.initialize()
ws.update()
ws.finalize()

sc = simplecrop_cli.SimpleCrop()
sc.initialize()
sc.set_value('infiltration_water__depth', ws.get_value('surface_water__depth'))
sc.update()
sc.finalize()
```  

Note that the only thing you would have to do is import a different model
and run the model.

Currently the framework does not have any write support for data stores. With write support it would look something like

```python
from grass import Watershed
import simplecrop_cli
import meillionen_mt as mt

days_with_rain = get_days_with_rain_data()

ws = Watershed(mt.NetCDFStoreWriter('overland.nc'))
ws.set_value('rainfall__depth', days_with_rain)
ws.initialize()
ws.update()
ws.finalize()

sc = simplecrop_cli.SimpleCrop(mt.NetCDFStoreWriter('simplecrop.nc'))
sc.initialize()
sc.set_value('infiltration_water__depth', ws.get_value('surface_water__depth'))
sc.update()
sc.finalize()
``` 

Notice that this example imports the meillionen_mt python library to build a store interface to access the results of the overland flow model using the same PyMT compatible getter interface.

There is no general support for checking unit compatibility and storage compatibility yet but work will be done to ensure that units are checked and converted and the right storage format is used.

In the near future work will focus on the development of improving Python interfaces and broadcasting. Right now you can ask for the name and size of a NetCDF store variable but indexing and slicing the variable are unavailable. Support for slices returning n-dimensional arrays with labels and dropped singleton dimensions as well as dataframes is being worked on.

Contribute
----------

### Open Modeling Foundation Standards

The Open Modeling Foundation is using GitHub as a platform for members to contribute and modify content. With this platform, we hope to encourage collaboration and accessibility.

You do not need a lot of experience (or any) with Git or GitHub to use this system but this step by step description should help if you find yourself confused. First, you must have a GitHub account to comment or propose changes. If you don’t have an account, you can create one for free at https://github.com/join and additional benefits are available to users affiliated with an academic institution.

There are two ways to work with any given page in the site. You can Create an issue to make a comment or suggestion, note a problem, or start a discussion or you can Suggest Changes to propose specific textual changes to a document. For substantive changes, we recommend that you first create an issue to discuss any proposed changes to the site with the community, and then Suggest Changes to submit your proposed changes for review. For minor edits like typos, feel free to simply Suggest Changes to fix them.

If you would like to be notified about other people’s comments or suggestions, you can Watch this repository by clicking on the Watch button at the upper right corner of the main repository page: https://github.com/openmodelingfoundation/openmodelingfoundation.github.io
How to Make a Comment or Suggestion

1. If you want to make a comment but do not have specific textual changes that you would like to propose, choose the Create Issue menu item at the upper right corner.
2. This will open up a new tab and take you to our GitHub repository’s issue tracker. If you are not signed into GitHub, you will be prompted to login to GitHub. Once you are signed in, you’ll have a form available for you to submit a brief title and description for your comment. If you would like to get the attention of a team member click the @ symbol in the editor for a list of team members. The comment will be automatically labeled based on which section of the site you were viewing.
3. Once you have finished your comment, click the green Submit new issue button.
4. You can track the status of your issue here: https://github.com/openmodelingfoundation/openmodelingfoundation.github.io/issues. You will also be notified via email if someone responds to your comment on github.

How to Edit the Text

1. In your browser navigate to the page that has text you would like to change.
2. On this page, choose Suggest Edits menu item (upper right corner). This will open a new page in a different tab.
3. The first time you attempt to edit a page you will be asked to create a forked repository – do so. (This is your own personal copy of the repository where copies of your proposed edits will be recorded)
4. You will then be taken to a text editor on the GitHub site where you can make changes. (Note the Preview Changes tab if you want to see the result of your edits)
5. Once you have made all of your changes, scroll down to the “Propose file change” section. In the empty boxes you must include a title and provide a short text description. If the changes you have made are related to an issue that has already been submitted, typing the # will bring up a list of available issues that you can associate with your edits.
6. After those boxes are filled in – click the green Propose file change button.
7. Following this, a comparing changes page will load that shows the original text and your new text changes. To make these changes visible to the site moderators, you must then click the green “Create pull request” button. Please choose Create Draft Pull for the time being.
8. Representatives from the Open Modeling Foundation will review your changes for inclusion in the public version of the document.
9. You can track the status of your proposed changes here: https://github.com/openmodelingfoundation/openmodelingfoundation.github.io/pulls. You should receive email notifications when the proposed change has received additional comments or questions, been reviewed, or been merged into the main document.

Be sure to follow our [contributor code of conduct](https://openmodelingfoundation.github.io/contribute/code-of-conduct/)

### Model Interoperability Toolkit (meillionen)

The data interoperability project project is at https://github.com/openmodelingfoundation/meillionen. Clone it, make changes and contribute! Suggestions on APIs for how to query a data stores (in Rust and Python) or example models to add to the documentation are also greatly appreciated. The project is still in the very early stages so don't expect any backwards compatibility until a basic data store query interface has been finalized.