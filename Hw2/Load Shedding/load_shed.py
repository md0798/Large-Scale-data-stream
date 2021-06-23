import random
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
import time
import numpy as np
import matplotlib.pyplot as plt

def generate():
	raNu = random.randint(1,100)#generate random int between 1 and 100 
	return raNu#return the random int
	
def operator(lines,start,tot):
	f_lines = lines.map(lambda x: x*0.1)#operator
	time.sleep(0.05)#fixed process time to smooth curve
	end = time.time()#end of the process
	diff = end-start #complete process time
	tp = tot.count()/diff # throughput of the process
	return tp, f_lines # return throughput and mapped lines
	
def load_shedder(lines):
	start = time.time()# start the process time
	sel = 10000/lines.count()# fix selectivity based on the number of input tuples 
	f_lines = lines.filter(lambda x: random.random() < sel)#filter at random and get the desired tuples
	tp, ll_red = operator(f_lines,start,lines) # send the tuples for processing to operator 
	return tp,sel, ll_red#reutrn throughput, selectivity and mapped lines 


def error(l1,l2):# calculate the error, i.e., the euclidean distance between original tuples and reduced tuples
	diff = len(l2) -len(l1)
	for i in range(diff):#append the reduced tuple with zeroes to calculate the euclidean distance
		l1.append(0)
	l1 = np.array(l1)
	l2 = np.array(l2)
	err = np.linalg.norm(l2-l1) #calculate euclidean distance using numpy
	return err#return error

def normalise(l):# normalise list
	ll = []
	l_max = max(l)# get maximum of the list 
	l_min = min(l)#get minimum of the list
	for i in range(len(l)):
		ll.append((l[i] - l_min)/(l_max-l_min))# normalise the list to get it in the range of 0-1
	return ll	

sc= SparkContext("local","myApp")
sqlContext = SQLContext(sc)

#initialise different lists
tp = []
sel = []
lis = np.arange(1,10,0.1)
err = []
accu = []
for j in lis:
	l = []
	for i in range(int(j*10000)):#generate random integer lists
		l.append(generate())
	
	ll = sc.parallelize(l)#convert list to RDD
	a,b,c = load_shedder(ll)# get throughput, selectivity and mapped lines from load shedder
	tp.append(a)# append throughput for different selectivity
	sel.append(b)#append selectivity
	ll_red = c.collect()# convert mapped lines to list 
	err.append(error(ll_red,l))#append euclidean distance between mapped lines and original list 

tp = normalise(tp)# normalise throughput
err = normalise(err)#normalise error
for i in range(len(err)): # since accuracy is 1 - error
	accu.append(1-err[i])

#plot the graph
plt.plot(sel, tp, 'b-',label = "Throughput")
plt.plot(sel,accu, 'r-',label = "Accuracy")
plt.legend()
plt.xlabel('Selectivity')
plt.ylabel("Euclidean distance")
plt.title('Load Shedding')
plt.savefig('ls.png')

	
