from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
import csv

def fun(x):
	a=0
	y = x[0].split(".")
	for val in y:
		if val.isdigit():
			a = a+1
	if a >= 4:
		return x
	else:
		return None



sc= SparkContext("local","myApp")
sqlContext = SQLContext(sc)


lines=sc.textFile("../epa-http.txt")

ll = lines.map(lambda x: (x.split(" ")))
ll = ll.map(fun)
ll = ll.filter(lambda x: x)
ll2 = ll.map(lambda x: (x[0],x[-1]))
ll3 = ll2.filter(lambda x: "-"in x)
ll4 = ll2.subtract(ll3)
ll4 = ll4.map(lambda x: (x[0],int(x[-1])))
final = ll4.reduceByKey(lambda x,y:x+y)
final = final.sortBy(lambda x: x[1], ascending=False) #using sortBy to sort the Bytes data in descending order
f2 = final.take(10) #getting the top 10 values
df = sqlContext.createDataFrame(f2, ['IP', 'bytes'])
df.write.format('com.databricks.spark.csv').options(header='true').save('IP_bytes_10')#csv for top 10 IPs
f2 = final.take(100)#getting the top 100 values
df = sqlContext.createDataFrame(f2, ['IP', 'bytes'])
df.write.format('com.databricks.spark.csv').options(header='true').save('IP_bytes_100')#csv for top 100 IPs

