import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark import SparkConf, SparkContext

# to solve "Python worker failed to connect back" environment problem
import findspark
findspark.init()

# init spark
sc = SparkContext.getOrCreate()

# read file
data = sc.textFile("C:\\Users\\ee527\\Desktop\\epa-http.txt")
# take first three subnet ip
data = data.map(lambda x: (x.replace('.',' ').split(' ')[0], x.replace('.',' ').split(' ')[1], x.replace('.',' ').split(' ')[2], x.replace('.',' ').split(' ')[len(x.replace('.',' ').split(' ')) - 1]))
# delete unkown byte data
data = data.filter(lambda x: x[3] != '-')
# filter non-numerical ip address
data = data.filter(lambda x: x[0].isdigit() == True and x[1].isdigit() == True and x[2].isdigit() == True)
# combine subnet
data = data.map(lambda x: (str(x[0]) + '.' + str(x[1]) + '.' + str(x[2]), x[3]))
# calculate total bytes based on the same ip
data = data.reduceByKey(lambda x, y: int(x) + int(y))
final  = data.collect()
# start writing data into csv
import csv

# open the file in the write mode
with open("C:\\Users\\ee527\\Desktop\\hw1_4.csv", 'w', newline='') as f:
    # create the csv writer
    writer = csv.writer(f)
    
    for row in final:
        
        # write a row to the csv file
        writer.writerow(row)