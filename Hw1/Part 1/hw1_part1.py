from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc

#function to filter all non - IPs
def filter_non_IP(x):
	a=0
	y = x[0].split(".")
	for val in y:
		if val.isdigit():
			a = a+1
	if a >= 4:#if more than 4 digits then it is valid IP
		return x
	else:
		return None


#setting sparkcontext and sqlcontext
sc= SparkContext("local","myApp")
sqlContext = SQLContext(sc)

#importing textfile as lines
lines=sc.textFile("../epa-http.txt")
#split the lines with the space in between different fields 
ll = lines.map(lambda x: (x.split(" ")))
#dropping non IPs using map and python function 
ll = ll.map(filter_non_IP)
ll = ll.filter(lambda x: x) #removing all None values
ll2 = ll.map(lambda x: (x[0],x[-1])) # getting only IP and bytes data
ll3 = ll2.filter(lambda x: "-"in x)#filtering bytes with "-" value
ll4 = ll2.subtract(ll3) # removing "-" value lines from data
ll4 = ll4.map(lambda x: (x[0],int(x[-1]))) #converting bytes value to int
final = ll4.reduceByKey(lambda x,y:x+y)#using reduce to calculate bytes for any IP
df = sqlContext.createDataFrame(final, ['IP', 'bytes'])#converting result to sqlcontext to get csv file
df = df.sort("IP")#sorting IP addresses
df.show()
df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save('IP_bytes')#getting csv file
