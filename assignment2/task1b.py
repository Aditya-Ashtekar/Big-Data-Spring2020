import sys
import pyspark
from pyspark import SparkContext
sc = SparkContext.getOrCreate("")
from csv import reader
from csv import writer
import json
import io
def list_to_csv_str(x):
    output = io.StringIO("")
    writer(output).writerow(x)
    return output.getvalue().strip()
Fares = sc.textFile(sys.argv[1], 1)
Fares = Fares.mapPartitions(lambda x: reader(x))
fares_header = Fares.first()
Fares = Fares.filter(lambda x: x != fares_header)
Licenses = sc.textFile(sys.argv[2], 1)
Licenses = Licenses.mapPartitions(lambda x: reader(x))
licenses_header = Licenses.first()
Licenses = Licenses.filter(lambda x: x != licenses_header)
F = Fares.map(lambda x: (x[0],(x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10])))
L = Licenses.map(lambda x: (x[0],(x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15])))
joined = F.join(L)
result = joined.map(lambda x: (x[0],x[1][0][0],x[1][0][1],x[1][0][2],x[1][0][3],x[1][0][4],x[1][0][5],x[1][0][6],x[1][0][7],x[1][0][8],x[1][0][9],x[1][1][0],x[1][1][1],x[1][1][2],x[1][1][3],x[1][1][4],x[1][1][5],x[1][1][6],x[1][1][7],x[1][1][8],x[1][1][9],x[1][1][10],x[1][1][11],x[1][1][12],x[1][1][13],x[1][1][14]))
result = result.sortBy(lambda x: (x[0],x[1],x[3]))
#output = result.map(lambda x: ("%s, %s, %s, %s, %s") %(x[0], x[1], x[2], x[3], x[4]))
#output = result.map(lambda r: ', '.join([json.dumps(KVPair) for KVPair in r]))
output = result.map(lambda r: list_to_csv_str([KVPair for KVPair in r]))
output.saveAsTextFile('task1b.out')
