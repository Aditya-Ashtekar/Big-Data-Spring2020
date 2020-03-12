import sys
import pyspark
from operator import add
from pyspark import SparkContext
sc = SparkContext.getOrCreate("")
from csv import reader
AllTrips = sc.textFile(sys.argv[1],1)
AllTrips = AllTrips.mapPartitions(lambda x:reader(x))

Taxi = AllTrips.map(lambda x: (x[0],x[3]))
TotalTrips= Taxi.groupByKey().map(lambda x : (x[0],len(x[1])))

Days = Taxi.map(lambda x: (x[0],x[1].split(" ")[1])).distinct()
DaysDriven = Days.map(lambda x: (x[0],1)).reduceByKey(add)

result = TotalTrips.join(DaysDriven)
result = result.sortByKey()
result = result.map(lambda x: (x[0],x[1][0],x[1][1],round(x[1][0]/x[1][1],2)))

output = result.map(lambda r: ', '.join([str(KVPair) for KVPair in r]))
output.saveAsTextFile('task2d.out')
