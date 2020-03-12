import sys
import pyspark
from pyspark import SparkContext
sc = SparkContext.getOrCreate("")
from csv import reader

Licenses = sc.textFile(sys.argv[1],1)
Licenses = Licenses.mapPartitions(lambda x:reader(x))
Vehicle = Licenses.map(lambda x: (x[16],(x[3],x[5],x[8])))

r = Vehicle.map(lambda x: (x[0],(1,x[1][1],x[1][2])))
res = r.reduceByKey(lambda x,y : (x[0]+y[0],float(x[1])+float(y[1]),float(x[2])+float(y[2]))).sortByKey()
result = res.map(lambda x: (x[0],str(x[1][0]),str(round(x[1][1],2)),str(round((x[1][2]/x[1][1])*100,2))))

output = result.map(lambda r: ', '.join([KVPair for KVPair in r]))
output.saveAsTextFile('task4a.out')
