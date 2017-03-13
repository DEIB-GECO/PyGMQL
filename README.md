# GMQL-Python
Python-Spark implementation of the GMQL system

## Requirements
- A python environment
- Apache Spark

##Set up of the project
1. Download this repository
2. In your IDE add to the paths of the project the ones relative to your Spark installation

## Example of usage
Your spark engine is instantiated when you call the following
```python
import gmql as gl
```
after this you can access the spark context by `gl.sc`

## Tests
In the folder [tests](./tests) you can find some examples of using the library
