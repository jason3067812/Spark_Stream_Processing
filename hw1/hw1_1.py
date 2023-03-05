from pyspark import SparkConf, SparkContext
import pyspark
from pyspark.sql import SparkSession

# to directly solve "Python worker failed to connect back" environment problem
import findspark
findspark.init()


file_path = "C:\\Users\\ee527\\Desktop\\epa-http.txt"

# init spark
sc = SparkContext.getOrCreate()

# read file
data = sc.textFile(file_path)
# to see some data
a = data.take(12)
print(a)
print(" ")

# only get address and bytes
data = data.map(lambda x: [x.split(' ')[0], x.split(' ')[-1]])
# check whether it is ip address or not
data = data.filter(lambda x: (x[0].split('.'))[-1].isnumeric())
# throw the data if it doesn't have bytes information
data = data.filter(lambda x: x[1] != '-')
data = data.map(lambda x: (x[0],int(x[1])))
# calculate the total bytes 
data = data.reduceByKey(lambda x, y: int(x) + int(y))
# sort data
data = data.sortByKey()
# get data
final = data.collect()

# check data type
print(type(final))

# start writing data into csv
import csv
# open the file in the write mode
with open("C:\\Users\\ee527\\Desktop\\hw1.csv", 'w', newline='') as f:
    # create the csv writer
    writer = csv.writer(f)
    
    for row in final:
        
        # write a row to the csv file
        writer.writerow(row)