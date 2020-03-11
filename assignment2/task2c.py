import sys
import pyspark
from operator import add
from pyspark import SparkContext
sc = SparkContext.getOrCreate("")
from csv import reader
AllTrips = sc.textFile("/user/ama1219/task1a.out")
AllTrips = AllTrips.mapPartitions(lambda x:reader(x))

result= AllTrips.map(lambda x: (x[3],(str(round(float(x[15])+float(x[16])+float(x[18]),2)),str(round(float(x[19]),2)))))
result = result.sortByKey()
result = result.map(lambda x: (x[0],x[1][0],x[1][1]))
output = result.map(lambda r: ', '.join([KVPair for KVPair in r]))
output.saveAsTextFile('task2c.out')
