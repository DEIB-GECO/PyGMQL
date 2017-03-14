"""
Setting up the pyspark environment
"""

spark_home = '/home/luca/spark-2.1.0-bin-hadoop2.7'

import findspark
findspark.init(spark_home=spark_home)
import pyspark

app_name = 'gmql_spark'

conf = pyspark.SparkConf() \
    .setMaster('local[*]') \
    .setAppName(app_name)

# # getting the Spark context
# sc = pyspark.SparkContext(conf=conf)
#
# # setting the logger level
# sc.setLogLevel("ERROR")


