import random
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
import time
import numpy as np
import matplotlib.pyplot as plt


def generate():
	raNu = [random.randint(1,100),random.randint(1,5)] #generate a list of 2 random integers
	return raNu

def oper_A(ll1,ll2):
	start = time.time()
	ll3 = ll1.join(ll2)#use join to join 2 different RDDs
	end = time.time()
	return ll1.count()/(end-start)#return throughput
	
def oper_B(ll1,ll2):
	st1 = time.time()#Use union and then groupByKey to get the same result as join
	ll4 = ll1.union(ll2)
	ll4 = ll4.groupByKey()
	en1 = time.time()
	return ll1.count()/(en1-st1)#return throughput
	

sc= SparkContext("local","myApp")
sqlContext = SQLContext(sc)
#initialise all lists
tp_a = []
tp_b = []
tp_a_1 = []
tp_b_1 = []
line_count = []
li = np.arange(1,10,0.1)
for i in li:#generate 2 different lists of random integers
	l1 = []
	l2 = []
	for m in range(int(i*1000)):
		l2.append(generate())
	for k in range(int(i*1000)):
		l1.append(generate())
	ll1 =sc.parallelize(l1)#convert both lists to RDD
	ll2 = sc.parallelize(l2)
	tp_a.append(oper_A(ll1,ll2))#get throughput for the different operations
	tp_b.append(oper_B(ll1,ll2))
	line_count.append(ll1.count())#get the number of tuples used


tp_b_max = max(tp_b)
for n in range(len(tp_b)):
	tp_a_1.append(tp_a[n]/tp_b_max)
	tp_b_1.append(tp_b[n]/tp_b_max)

plt.plot(line_count,tp_a_1, 'r-', label = 'Join operation')
plt.plot(line_count,tp_b_1, 'b-', label = 'Union and groupByKey')
plt.legend()
plt.xlabel('Size of tuples')
plt.ylabel("Throughput")
plt.title('Algorithm Selection')
plt.savefig("AS.png")


