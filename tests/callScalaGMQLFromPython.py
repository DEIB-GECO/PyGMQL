"""
    Main of example that demonstrate:
    1) the usage of the spylon package
    2) how to call GMQL scala functions from Python
"""

import spylon.spark as sc

c = sc.SparkConfiguration()
c._spark_home = "/home/luca/spark-2.1.0-bin-hadoop2.7"

# I add the GMQL jar files for accessing them from pyspark
c.jars = ["/home/luca/Scrivania/GMQL/GMQL-Core/target/GMQL-Core-2.0.jar",
          "/home/luca/Scrivania/GMQL/GMQL-Server/target/GMQL-Server-2.0.jar",
          "/home/luca/Scrivania/GMQL/GMQL-Spark/target/GMQL-Spark-4.0.jar"]


path_file = "/home/luca/Scrivania/GMQL-Python/resources/ENCSR373BIX_rep2_1_pe_bwa_biorep_filtered_hotspots.bed"
path_output = "/home/luca/Scrivania/GMQL-Python/resources/"

# I get the pyspark context from Spylon
sc = c.spark_context("prova_spylon")

# Instantiation of a GMQLSparkExecutor
binsize = sc._jvm.it.polimi.genomics.core.BinSize(5000, 5000, 1000)
maxBinDistance = 100000
REF_PARALLILISM = 20
testingIOFormats = False
sparkContext = sc._jvm.org.apache.spark.SparkContext.getOrCreate()
outputFormat = sc._jvm.it.polimi.genomics.core.GMQLOutputFormat.TAB()

gmql_spark_executor = sc._jvm.it.polimi.genomics.\
    spark.implementation.GMQLSparkExecutor(binsize, maxBinDistance, REF_PARALLILISM,
                                           testingIOFormats, sparkContext, outputFormat)

# Creation of a GMQLServer given the spark executor previously defined
server = sc._jvm.it.polimi.genomics.GMQLServer.GmqlServer(gmql_spark_executor, None)

# Getting a BedParser object from GMQL
bedParser = sc._jvm.it.polimi.genomics.spark.implementation.loaders.BedParser

# Executing the query
DS1 = server.READ(path_file).USING(bedParser)



print("DONE")