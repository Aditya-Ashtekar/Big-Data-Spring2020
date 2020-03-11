import sys
import pyspark
from operator import add
from pyspark import SparkContext
sc = SparkContext.getOrCreate("")
from csv import reader
AllTrips = sc.textFile(sys.argv[1],1)
AllTrips = AllTrips.mapPartitions(lambda x:reader(x))

trips = AllTrips.map(lambda x: (x[0],1)).reduceByKey(add)
gps = AllTrips.filter(lambda x: float(x[10])==0.0 and float(x[11])==0.0 and float(x[12])==0.0 and float(x[13])==0.0)
gpstrips = gps.map(lambda x: (x[0],1)).reduceByKey(add)
result = trips.leftOuterJoin(gpstrips)

result = result.map(lambda x: (x[0],str(round((x[1][1]/x[1][0])*100,2))) if x[1][1] != None else (x[0],'0.0')).sortByKey()
output = result.map(lambda r: ', '.join([KVPair for KVPair in r]))
output.saveAsTextFile('task3c.out')
