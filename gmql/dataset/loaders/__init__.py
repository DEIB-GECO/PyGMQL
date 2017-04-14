"""
Loader settings:
we use the GMQL scala class CombineTextFileWithPathInputFormat in order to load the region and metadata files with the
same id_sample based on the hash of the file name
"""

inputFormatClass = 'it.polimi.genomics.spark.implementation.loaders.Loaders$CombineTextFileWithPathInputFormat'
keyFormatClass = 'org.apache.hadoop.io.LongWritable'
valueFormatClass = 'org.apache.hadoop.io.Text'
defaultCombineSize = 64

# configuration of the Hadoop loader used by spark
conf = {"textinputformat.record.delimiter": "\n",
        "mapreduce.input.fileinputformat.input.dir.recursive": "true",
        "mapred.max.split.size": str(defaultCombineSize*1024*1024)
        }


def generateKey(filename):
    return hash(filename)