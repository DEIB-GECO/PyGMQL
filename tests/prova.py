import spylon.spark as ss

c = ss.SparkConfiguration()
c.master = 'local[*]'
c._spark_home = "/home/luca/spark-2.1.0-bin-hadoop2.7"

# I add the GMQL jar files for accessing them from pyspark
c.jars = ["/home/luca/Scrivania/GMQL/GMQL-Core/target/GMQL-Core-2.0.jar",
          "/home/luca/Scrivania/GMQL/GMQL-Server/target/GMQL-Server-2.0.jar",
          "/home/luca/Scrivania/GMQL/GMQL-Spark/target/GMQL-Spark-4.0.jar"]

app_name = 'gmql_spark'

sc = c.spark_context(app_name)

path = "/home/luca/Scrivania/GMQL-Python/resources/hg_narrowPeaks"

t = sc.textFile(path)

print(t.take(10))
