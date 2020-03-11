import sys
import pyspark
from operator import add
from pyspark import SparkContext
sc = SparkContext.getOrCreate("")
from csv import reader
AllTrips = sc.textFile(sys.argv[1],1)
AllTrips = AllTrips.mapPartitions(lambda x:reader(x))

numtaxis = AllTrips.map(lambda x: (x[1],x[0])).distinct()
result = numtaxis.map(lambda x: (x[0],1)).reduceByKey(add)
result = result.map(lambda x: (x[0],str(x[1]))).sortByKey()

output = result.map(lambda r: ', '.join([KVPair for KVPair in r]))
output.saveAsTextFile('task3d.out')

