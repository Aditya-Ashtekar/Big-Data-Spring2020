import sys
import pyspark
from pyspark import SparkContext
sc = SparkContext.getOrCreate("")
from csv import reader
Trips = sc.textFile(sys.argv[1], 1)
Trips = Trips.mapPartitions(lambda x: reader(x))
Trips = Trips.filter(lambda x: x[0] != "medallion")
Fares = sc.textFile(sys.argv[2], 1)
Fares = Fares.mapPartitions(lambda x: reader(x))
Fares = Fares.filter(lambda x: x[0] != "medallion")

T = Trips.map(lambda x: ((x[0],x[1],x[2],x[5]),(x[3],x[4],x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13])))
F = Fares.map(lambda x: ((x[0],x[1],x[2],x[3]),(x[4],x[5],x[6],x[7],x[8],x[9],x[10])))
joined = T.join(F)
AllTrips= joined.map(lambda x: (x[0][0],x[0][1],x[0][2],x[0][3],x[1][0][0],x[1][0][1],x[1][0][2],x[1][0][3],x[1][0][4],x[1][0][5],x[1][0][6],x[1][0][7],x[1][0][8],x[1][0][9],x[1][1][0],x[1][1][1],x[1][1][2],x[1][1][3],x[1][1][4],x[1][1][5],x[1][1][6]))
AllTrips = AllTrips.sortBy(lambda x: (x[0],x[1],x[2],x[3]))
output=AllTrips.map(lambda r: ', '.join([KVPair for KVPair in r]))
output.saveAsTextFile('task1a.out')
