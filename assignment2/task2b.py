import sys
import pyspark
from operator import add
from pyspark import SparkContext
sc = SparkContext.getOrCreate("")
from csv import reader
AllTrips = sc.textFile("/user/ama1219/task1a.out")
AllTrips = AllTrips.mapPartitions(lambda x:reader(x))

result = AllTrips.map(lambda x : (x[7],1)).reduceByKey(add)
result = result.sortByKey()
output = result.map(lambda r: ', '.join([str(KVPair) for KVPair in r]))
output.saveAsTextFile('task2b.out')
