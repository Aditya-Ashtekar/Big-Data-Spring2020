import sys
import pyspark
from pyspark import SparkContext
sc = SparkContext.getOrCreate("")
from csv import reader
AllTrips = sc.textFile(sys.argv[1],1)
AllTrips = AllTrips.mapPartitions(lambda x:reader(x))

mapped = AllTrips.map(lambda x: ((0,5),1) if (float(x[15])>0 and float(x[15])<=5) else (((5,15),1) if float(x[15])>5 and float(x[15])<=15 else(((15,30),1) if float(x[15])>15 and float(x[15])<=30 else (((30,50),1)if float(x[15])>30 and float(x[15])<=50 else (((50,100),1)if float(x[15])>50 and float(x[15])<=100 else (((100,sys.maxsize),1) if float(x[15])>100 else((-1,-1),1) ))))))
res = mapped.countByKey()
res = sc.parallelize(list(res.items()))
res = res.filter(lambda x: x[0][0] != -1)
result = res.map(lambda x : (x[0][0],x[0][1],x[1]))
result = result.sortBy(lambda x: float(x[0]))
output = result.map(lambda r: ', '.join([str(KVPair) for KVPair in r]))
output.saveAsTextFile('task2a.out')
