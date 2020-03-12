import sys
import pyspark
from pyspark import SparkContext
sc = SparkContext.getOrCreate("")
from csv import reader
Licenses = sc.textFile(sys.argv[1],1)
Licenses = Licenses.mapPartitions(lambda x:reader(x))

Agent = Licenses.map(lambda x: (x[20],x[5])).reduceByKey(lambda x,y: str(round(float(x)+float(y),2))).sortBy(lambda x: (x[1],x[0]),ascending=False)
result = sc.parallelize(Agent.take(10))

output = result.map(lambda r: ', '.join([KVPair for KVPair in r]))
output.saveAsTextFile('task4c.out')
