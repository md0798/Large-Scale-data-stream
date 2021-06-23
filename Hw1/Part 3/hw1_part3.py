from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc

def filter_non_IP(x):
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
ll = ll.map(filter_non_IP)
ll = ll.filter(lambda x: x)
ll2 = ll.map(lambda x: (x[0],x[1],x[-1]))#Isolating the data as (IP,time,bytes) time is as follows - Date:Hour:minute:second
ll3 = ll2.filter(lambda x: "-"in x)
ll4 = ll2.subtract(ll3)
ll4 = ll4.map(lambda x: (x[1].split(":")[1],x))# map lines such that it give (hour, x)
ll4 = ll4.groupBy(lambda x: x[0])#group data using hour data
ll4 = ll4.sortByKey()#sort the data using hour data
ll4 = ll4.map(lambda x: [x for x in x[1]])#get the data in the all different groups
ll5 = ll4.collect()[0]#isolate the '00' hour data  
ll5 = sc.parallelize(ll5)#convert list to RDD
ll5 = ll5.map(lambda x: x[1])# Get the x data only, removing hour from (hour, x) 
ll5 = ll5.map(lambda x: (x[0],int(x[-1])))#get only the IP and bytes, also convert bytes to int
final = ll5.reduceByKey(lambda x,y:x+y)#using reduce to calculate bytes for any IP
df = sqlContext.createDataFrame(final, ['IP', 'bytes'])
df = df.sort("IP")
df.show()
df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save('IP_bytes_time')

