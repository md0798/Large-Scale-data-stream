import random
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
import time
import numpy as np
import matplotlib.pyplot as plt

def generate():
	raNu = random.randint(1,100) #generate random int between 1 and 100 
	return raNu #return the random int
	
	

def order(lines, s):
	start = time.time()#capture start time
	f_lines_a = lines.filter(lambda x: x < 50) #filter all numbers less than 50
	time.sleep(0.05)# increase fixed cost
	#end_i = time.time()
	f_lines_b = f_lines_a.filter(lambda x: x%s == 0) #filter numbers that are divisble by s 
	time.sleep(0.05)
	end = time.time()# capture process end time
	#print('A ordered --> ' + str(end_i - start))
	#print('B ordered --> '+ str(end - end_i))
	diff = end - start # cost of the whole process
	tp = lines.count()/diff #throughput of the whole process
	sel_b = f_lines_b.count()/f_lines_a.count() # selectivity of operator B
	#print('Selectivity of B --> '+ str(sel_b))
	return tp, sel_b #return throughput, selectivity of B

def reorder(lines,s):
	start = time.time()#capture start time
	f_lines_b = lines.filter(lambda x: x%s == 0)#filter numbers that are divisble by s
	time.sleep(0.05)# increase fixed cost
	#end_i = time.time()
	f_lines_a = f_lines_b.filter(lambda x: x < 50)#filter all numbers less than 50
	time.sleep(0.05)
	end = time.time()# capture process end time
	#print('B reordered --> '+ str(end_i - start))
	#print('A reordered --> '+ str(end - end_i))
	diff = end - start# cost of the whole process
	tp = lines.count()/diff#throughput of the whole process
	sel_b = f_lines_b.count()/lines.count()# selectivity of operator B
	#print('Selectivity of B --> '+ str(sel_b))
	return tp,sel_b#return throughput, selectivity of B
	


sc= SparkContext("local","myApp")
sqlContext = SQLContext(sc)
tp_o = []#intitalise different lists
sel_b_o = []
tp_r = []
sel_b_r = []
l=[]
tp_o_1 = []
tp_r_1 = []
for i in range(100000): #generate 100000 random int and store them in a list
	l.append(generate())

ll = sc.parallelize(l)#convert list to RDD
for i in range(1,10): # run one for loop to initialise all lists
	m,n = order(ll,i)
	tp_o.append(m)# throughput for ordered operators
	sel_b_o.append(n) # selectivity of b for ordered operators
	o,p = reorder(ll,i)
	tp_r.append(o)# throughput for reordered operators
	sel_b_r.append(p)# selectivity of b for reordered operators


for j in range(20):#run the same loop 20 times to smooth the curve
	l=[]

	for i in range(100000):
		l.append(generate())
	
	
	

	ll = sc.parallelize(l)
	for i in range(1,10):
		m,n = order(ll,i)
		tp_o[i-1]= (m+tp_o[i-1])/2
		sel_b_o[i-1] = (n+sel_b_o[i-1])/2 
		o,p = reorder(ll,i)
		tp_r[i-1]= (o+tp_r[i-1])/2
		sel_b_r[i-1] = (p+sel_b_r[i-1])/2

tp_o_max = max(tp_o)#get max throughput of ordered operators
tp_r_max = max(tp_r)
for i in range(len(sel_b_o)):#normalise throughput
	tp_o_1.append((tp_o[i])/(tp_o_max))


for i in range(len(sel_b_r)):#normalise throughput
	tp_r_1.append((tp_r[i])/(tp_r_max))


#plot the graph
plt.plot(sel_b_o,tp_o_1,'b-',label = "ordered")
plt.plot(sel_b_r,tp_r_1,'r-',label = "reordered")
plt.legend()
plt.xlabel('Selectivity of B')
plt.ylabel("Througput")
plt.title('Operator Reordering')
plt.savefig('OR.png')
