# PyGMQL

API for calling interactively the GMQL Engine from Python


[![Build status](https://travis-ci.org/DEIB-GECO/PyGMQL.svg?branch=master)](https://travis-ci.org/DEIB-GECO)
[![PyPI version](https://badge.fury.io/py/gmql.svg)](https://badge.fury.io/py/gmql)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/Django.svg)](https://github.com/DEIB-GECO/PyGMQL)
[![codecov](https://codecov.io/gh/DEIB-GECO/PyGMQL/branch/master/graph/badge.svg)](https://codecov.io/gh/DEIB-GECO/PyGMQL)

## Documentation
The doucumentation can be found at the following link: http://pygmql.readthedocs.io

[![Documentation Status](https://readthedocs.org/projects/pygmql/badge/?version=latest)](http://pygmql.readthedocs.io/en/latest/?badge=latest)

## Try the library
You can try the library without any previous setup using Binder.

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/DEIB-GECO/PyGMQL/master?filepath=examples%2F00_Introduction.ipynb)


## Get in touch
You can ask questions or provide some feedback through our Gitter channel.

[![Join the chat at https://gitter.im/DEIB-GECO/PyGMQL](https://badges.gitter.im/DEIB-GECO/PyGMQL.svg)](https://gitter.im/DEIB-GECO/PyGMQL?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


## Requirements
The library requires the following:
* Python 3.4+
* The latest version of JAVA installed
* The JAVA_HOME variable set to the Java installation folder (example: `C:\Program Files\Java\jdk1.8.0_161` or `~/jdk1.8.0_161`)


## Installation
### From github
First of all download this repository in a choosen location:
```
git clone https://github.com/DEIB-GECO/PyGMQL.git
```
Than go inside the library folder and install the package as follows:
```
cd PyGMQL
pip install -e .
```

### From PyPi
```
pip install gmql
```

## Setup

### Use Anaconda
We suggest to manage your python distribution through Anaconda. 
The latest version of can be downloaded from https://www.continuum.io/downloads.

Once your Anaconda distribution is installed, let's create a brand new environment:
```
conda create --name pygmql python=3
```
### Check if JAVA is installed 
Check that the `JAVA_HOME` enviroment variable is correctly set to 
the latest JAVA distribution.
```
echo $JAVA_HOME
```
If the variable is not set (the previous command does not show nothing), you may need
to install JAVA (https://www.java.com/it/download/) and then set `JAVA_HOME` like the following:

On linux:
```
echo export "JAVA_HOME=/path/to/java" >> ~/.bash_profile
source ~/.bash_profile
```

On Mac:
```
echo export "JAVA_HOME=\$(/usr/libexec/java_home)" >> ~/.bash_profile
source ~/.bash_profile
```

On Windows:

1. Right click My Computer and select Properties.
2. On the Advanced tab, select Environment Variables, and then 
edit JAVA_HOME to point to where the JDK software is located, 
for example, C:\Program Files\Java\jdk1.6.0_02.


### Use it in Jupyter Notebooks
We strongly suggest to use the library with the support of a **Jupyter Notebook**
for the best graphical rendering of the data structures.
It may be necessary to manually install the Jupyter kernel:
```
source activate pygmql
python -m ipykernel install --user --name pygmql --display-name "Python (pygmql)"
```

### Keep the code updated
This is a constantly evolving project. Therefore the library will be
constantly added with new features. Therefore we suggest to update your
local copy periodically:
```
cd PyGMQL
git pull
```