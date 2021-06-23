from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc

def filter_non_IP(x):
	j = "."
	a=0
	y = x[0].split(".")
	for val in y:
		if val.isdigit():
			a = a+1
	if a >= 4:
		x[0] = y[0:3]#to get the same subnet, return only the first 3 bytes of valid IPs
		x[0] = j.join(x[0])
		return x
	else:
		return None

#Rest of the code is similar to part 1

sc= SparkContext("local","myApp")
sqlContext = SQLContext(sc)


lines=sc.textFile("../epa-http.txt")

ll = lines.map(lambda x: (x.split(" ")))
ll = ll.map(filter_non_IP)
ll = ll.filter(lambda x: x)
ll2 = ll.map(lambda x: (x[0],x[-1]))
ll3 = ll2.filter(lambda x: "-"in x)
ll4 = ll2.subtract(ll3)
ll4 = ll4.map(lambda x: (x[0],int(x[-1])))
final = ll4.reduceByKey(lambda x,y:x+y)
final = final.map(lambda x: (x[0] + '.*',x[1]))
df = sqlContext.createDataFrame(final, ['IP', 'bytes'])
df = df.sort("IP")
df.show()
df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save('IP_bytes_subnet')
