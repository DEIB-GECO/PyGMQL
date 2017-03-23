# GMQL-Python
Python-Spark implementation of the GMQL system

## Requirements
- A python environment
- Apache Spark
- The following GMQL modules exported as a jar file
    - GMQL-Core
    - GMQL-Server
    - GMQL-Spark
- The following python libraries:
    - pandas
    - tqdm
    - spylon


## Set up of the project
The following procedure is very stupid and not user friendly and it will be changed ASAP:
1. Download this repository
2. In the file [__init__.py](gmql/__init__.py) set the `c.jars` attribute with the paths of the GMQL jar files listed above
3. In the file [__init__.py](gmql/__init__.py) set the `c._spark_home` attribute with the path of your spark installation
4. Install the code with `pip install .`

## Example of usage
Your spark engine is instantiated when you call the following
```python
import gmql as gl
```

## Tests
In the folder [tests](./tests) you can find some examples of using the library
