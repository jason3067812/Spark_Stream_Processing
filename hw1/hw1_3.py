import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import window

from pyspark import SparkConf, SparkContext

# to solve "Python worker failed to connect back" environment problem
import findspark
findspark.init()


# init spark
sc = SparkContext.getOrCreate()

file_path = "C:\\Users\\ee527\\Desktop\\epa-http.txt"
# read file
data = sc.textFile(file_path)
print(type(data))

# only get address and bytes
data = data.map(lambda x: (x.split(' ')[0], x.split(' ')[1].replace("[","").replace("]",""), x.split(' ')[-1]))
# check whether it is ip address or not
data = data.filter(lambda x: (x[0].split('.'))[-1].isnumeric())
# throw the data if it doesn't have bytes information
data = data.filter(lambda x: x[1] != '-')
# revise original date data to the format that spark window can detect: y,m,d,h,m,s
data = data.map(lambda x: (x[0], f"2023-01-{x[1].split(':')[0]} {x[1].split(':')[1]}:{x[1].split(':')[2]}:{x[1].split(':')[3]}", x[2]))

# test = data.take(10)
# print(test)

# change data type
a = tuple(data.collect())

import pyspark
from pyspark.sql import SparkSession

# start making data type to dataframe
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
columns= ["ip", "date", "byte"]
df = spark.createDataFrame(data = a, schema = columns)

# see data type
print(df)
df.printSchema()
df.show(truncate=False)

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import window

# start using tumbling window to select time, then calculate the total byte based on the same IP
w = df.groupBy(window("date", "1 hour"),"ip").agg(func.sum("byte").alias("sum"))
# select the element that we want to print out in the end
w = w.select("ip",w.window.start.cast("string").alias("start"),w.window.end.cast("string").alias("end"), "sum")
# check data type again
print(type(w))
w.printSchema()
w.show(truncate=False)

# filter the time that we want from 2023-01-30 00:00:00 to 2023-01-30 01:00:00
w = w.where(w.start == "2023-01-30 00:00:00")
w = w.where(w.end == "2023-01-30 01:00:00")
final = w.collect()
print(final)

# start writing data into csv
import csv

# open the file in the write mode
with open("C:\\Users\\ee527\\Desktop\\hw1_3.csv", 'w', newline='') as f:
    # create the csv writer
    writer = csv.writer(f)
    
    for row in final:
        ans = [row[0],row[3]]
        # write a row to the csv file
        writer.writerow(ans)