## Installation

## Setting up CADES

Getting the code running on the CADES cluster takes a little bit of work, but it's not too terrible.
I can be broken down into a couple of steps.

1. Connect to the cluster
1. Setup the python environment
1. Run the code

### Connect to the cluster

All commands need to be run in the `slurm-login` node rather than the entry CADES login node.
These steps will need to be repeated each time you want to execute an HPC job.

1. `ssh <username>@cades-extlogin01.ornl.gov`
1. `ssh <username>@or-slurm-login.ornl.gov`

 - the `or-slurm-login` hostname is actually load balanced across a number of nodes.
   Take note of which one you actually connect to (e.g. or-slurm-login01) as you'll want to directly connect to that one in the future.

### Setup python

CADES already has built-in support for anaconda, which makes python package management incredibly easy.

You'll first need to load the anaconda module:

```bash
module load anaconda3
```

From there, you can either create a new environemnt, or connect to an existing one.

```bash
conda env create <environment name>
conda env activate <environment name>
```

Finally, you'll need to install the python dependencies (this will also bring along any system libraries that are needed as well.)

```bash
conda install jupyter dask geopandas rtree arrow shapely scipy
```

### Run the code

Most HPC work is framed around `jobs` which are an abstraction across a number of nodes dedicated to executing a parallel piece of work.
These jobs are submitted to the cluster, from the login nodes, via the `SLURM` job scheduler.

Dask natively supports SLURM and will automatically launch a series of interactive jobs to perform a single piece of work.

Dask code can be run either directly via a python script, or interactively via an `ipython` shell (I tend to use the later).

Most scripts start with the following prologue, which initializes and spins up the Dask backend:

```python
import dask.dataframe as dd
import pandas as pd
import numpy as np
import dask

from dask.distributed import Client
from datetime import datetime

# CADES configuration
from dask_jobqueue import SLURMCluster
# Be careful with using too many process, or you'll run out of file descriptors
cluster = SLURMCluster(project='covid19', queue='covid19_burst', cores=12, memory='350 GB', processes=3, walltime="4:00:00", job_extra=["-N 1"], interface="ib0")
cluster.scale(jobs=20)
client = Client(cluster)
```

It will take some type for the jobs are start and become ready.
You can check the status either via the `squeue` command:

```python
!squeue -u <username>
```

or by calling the `cluster` object directly.

The code can then be run as normal and when the session exits, it will automatically release the HPC nodes.

In order to share code and data between the various compute nodes you'll need to make sure information is stored on the high performance `Lustre` filesystem.
The base path is: `/lustre/or-hydra/cades-birthright/<username>`.

The easiest way to transfer data to the cluster is via the [Globus](https://globus.org) application, which has an easy web interface for moving data between the cluster and your local machine.
