from pyspark import SparkConf, SparkContext
import pyspark
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import random
import numpy as np
from scipy.spatial import distance

# to directly solve "Python worker failed to connect back" environment problem
import findspark
findspark.init()

file_path = r"C:\Users\ee527\Desktop\data.txt"

# init spark
sc = SparkContext.getOrCreate()

# read file
data = sc.textFile(file_path)

data = data.map(lambda x: [x.split(' ')[0], x.split(' ')[1], x.split(' ')[2], x.split(' ')[3]])

# unify and count original data number 
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
columns= ["name", "number", "area", "distance"]
original = spark.createDataFrame(data,columns)
original_number = original.count()

# original data has a,b,c three areas, calculate histogram in order to calculate the accuracy in the end 
a_n = original.select('area').where(original.area=="a").count()
b_n = original.select('area').where(original.area=="b").count()
c_n = original.select('area').where(original.area=="c").count()
ans = np.array([a_n,b_n,c_n])
print(original_number)
print(ans)

# start creating examples
sel_lst = []
thr_lst = []
dis_lst = []
n = 100
for i in range(11):

    # first operator: load shedding
    """
    shedding algorithm: shed data based on their distance

    """
    new_data = data.filter(lambda x: int(x[3]) < n)
    dataframe = spark.createDataFrame(new_data,columns)
    output_number = dataframe.count()
    
    # calculate selectivity
    sel = output_number/original_number 
    sel_lst.append(sel)
    
    # calculate throughput 
    # assume C(B) is high, C(A)=shedder is low: C(B) = 10, C(A) = 1
    thr = 1  / (1 + sel*10)
    thr_lst.append(thr)
    
    # second operator: count total number based on area
    a_n = dataframe.select('area').where(dataframe.area=="a").count()
    b_n = dataframe.select('area').where(dataframe.area=="b").count()
    c_n = dataframe.select('area').where(dataframe.area=="c").count()
    
    final = np.array([a_n,b_n,c_n])

    # calculate euclidean distance
    dis = distance.euclidean(ans,final)
    dis_lst.append(dis)
   
    n = n + 100

# normalize distance to 0 and 1
def NormalizeData(data):
    return (data - np.min(data)) / (np.max(data) - np.min(data))

dis_lst = NormalizeData(np.array(dis_lst))

# acc = 1 - distance
acc_lst = 1 - dis_lst
  
print(sel_lst)
print(thr_lst)
print(acc_lst)

# start plotting image
plt.plot(sel_lst, thr_lst, label = "throughput")
plt.plot(sel_lst, acc_lst, label = "accuracy")
plt.xlabel('selectivity')
plt.ylabel('value')
plt.title("load shedding")
plt.legend()
plt.show()