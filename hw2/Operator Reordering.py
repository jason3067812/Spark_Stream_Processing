from pyspark import SparkConf, SparkContext
import pyspark
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

# to directly solve "Python worker failed to connect back" environment problem
import findspark
findspark.init()

file_path = r"C:\Users\ee527\Desktop\data.txt"

# init spark
sc = SparkContext.getOrCreate()

# read file
data = sc.textFile(file_path)

data = data.map(lambda x: [x.split(' ')[0], x.split(' ')[1]])

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
columns= ["name", "number"]
b = spark.createDataFrame(data,columns)
original_number = b.count()

# operator 1: check whether it is number or not
data = data.filter(lambda x: (x[1].isnumeric()))

# count data number 
b = spark.createDataFrame(data)
input_number = b.count()

a = input_number / original_number
throughput_1 = 1 / (1+a)

# start creating ten examples
throughput_no_reorder = []
throughput_with_reorder = []
n = 0
b_lst = []
for n in range(2,12):
    # operator 2: filter by setting a threshold
    new_data = data.filter(lambda x: int(x[1]) < n)

    # count data number 
    dataframe = spark.createDataFrame(new_data,columns)
    output_number = dataframe.count()

    # calculate selectivity
    selectivity = output_number/input_number
    b_lst.append(selectivity)
    
    """

    formula1:
    cost = c(a) + s(a)*c(b)
        = 1 + s(a)*1

    throughput = 1/1+s(a)

    formula2:
    cost = c(b) + s(b)*c(a)
        = 1 + s(b)*1

    throughput = 1/1+s(b)

    """
    
    # calculate throughput
    throughput_no_reorder.append(throughput_1/throughput_1)
    throughput_2 = 1  / (1+selectivity)
    throughput_with_reorder.append(throughput_2/throughput_1)
     
    n+=1
    
print(a)
print(b_lst)
print(throughput_no_reorder)
print(throughput_with_reorder)


# start plotting image
plt.plot(b_lst, throughput_no_reorder, label = "without reorder")
plt.plot(b_lst, throughput_with_reorder, label = "reorder")
plt.xlabel('selectivity of B')
plt.ylabel('normalized throughput')
plt.title("selection reordering")
plt.legend()
plt.show()