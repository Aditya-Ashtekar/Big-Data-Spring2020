import sys
import pyspark
from operator import add
from pyspark import SparkContext
sc = SparkContext.getOrCreate("")
from csv import reader
AllTrips = sc.textFile(sys.argv[1],1)
AllTrips = AllTrips.mapPartitions(lambda x:reader(x))

res = AllTrips.filter(lambda x: float(x[15])<0.0).count()
result = [[str(res)]]
result = sc.parallelize(result)

output = result.map(lambda r: ', '.join([KVPair for KVPair in r]))
output.saveAsTextFile('task3a.out')
