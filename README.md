# PyGMQL

[![Join the chat at https://gitter.im/DEIB-GECO/PyGMQL](https://badges.gitter.im/DEIB-GECO/PyGMQL.svg)](https://gitter.im/DEIB-GECO/PyGMQL?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
API for calling interactively the GMQL Engine from Python

[![Documentation Status](https://readthedocs.org/projects/pygmql/badge/?version=latest)](http://pygmql.readthedocs.io/en/latest/?badge=latest)
[![Build status](https://travis-ci.org/DEIB-GECO/PyGMQL.svg?branch=master)](https://travis-ci.org/DEIB-GECO)
[![PyPI version](https://badge.fury.io/py/gmql.svg)](https://badge.fury.io/py/gmql)

## Documentation
The doucumentation can be found at the following link: http://pygmql.readthedocs.io

## Setup

#### Use Anaconda
We suggest to manage your python distribution through Anaconda. 
The latest version of can be downloaded from https://www.continuum.io/downloads.

Once your Anaconda distribution is installed, let's create a brand new environment:
```
conda create --name pygmql python=3.6
```

#### Download this repository
First of all download this repository in a choosen location:
```
git clone https://github.com/DEIB-GECO/PyGMQL.git
```
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

#### Install with pip
Than go inside the library folder and install the package as follows:
```
cd PyGMQL
pip install -e .
```

We strongly suggest to use the library with the support of a **Jupyter Notebook**
for the best graphical rendering of the data structures.
It may be necessary to manually install the Jupyter kernel:
```
source activate pygmql
python -m ipykernel install --user --name pygmql --display-name "Python (pygmql)"
```

#### Run the library
Firstly activate the `pygmql` conda environment and enter in the interactive 
Python shell
```
source activate pygmql
python
```

Then simply import the library
```python
import gmql as gl
```
Now you are ready to use PyGMQL! Congratulations!

### Keep the code updated
This is a constantly evolving project. Therefore the library will be
constantly added with new features. Therefore we suggest to update your
local copy periodically:
```
cd PyGMQL
git pull
```

## Tutorial
You can download a tutorial for beginners at this [link](https://www.dropbox.com/s/48cr8hvuytcufgj/Tutorial_PyGMQL.zip?dl=0) 
