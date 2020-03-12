import sys
import pyspark
from operator import add
from pyspark import SparkContext
sc = SparkContext.getOrCreate("")
from csv import reader
AllTrips = sc.textFile("/user/ama1219/task1a.out")
AllTrips = AllTrips.mapPartitions(lambda x:reader(x))

result= AllTrips.map(lambda x: (x[3].split()[0],(float(x[15])+float(x[16])+float(x[18]),float(x[19]))))
result = result.reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1]))
result = result.sortByKey()
result = result.map(lambda x: (x[0],str(round(x[1][0],2)),str(round(x[1][1],2))))
output = result.map(lambda r: ', '.join([KVPair for KVPair in r]))
output.saveAsTextFile('task2c.out')
