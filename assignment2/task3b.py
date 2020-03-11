import sys
import pyspark
from operator import add
from pyspark import SparkContext
sc = SparkContext.getOrCreate("")
from csv import reader
AllTrips = sc.textFile(sys.argv[1],1)
AllTrips = AllTrips.mapPartitions(lambda x:reader(x))

result = AllTrips.map(lambda x: ((x[0],x[3]),1)).reduceByKey(add).filter(lambda x: x[1] > 1)
result = result.map(lambda x: (x[0][0],x[0][1])).sortBy(lambda x: (x[0],x[1]))

output = result.map(lambda r: ', '.join([KVPair for KVPair in r]))
output.saveAsTextFile('task3b.out')
