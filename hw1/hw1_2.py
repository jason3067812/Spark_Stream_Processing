from pyspark import SparkConf, SparkContext

# to solve "Python worker failed to connect back" environment problem
import findspark
findspark.init()


file_path = "C:\\Users\\ee527\\Desktop\\epa-http.txt"

# init spark (prevent )
sc = SparkContext.getOrCreate()

# read file
data = sc.textFile(file_path)
a = data.take(12)
print(a)
print(" ")

# only get address and bytes
data = data.map(lambda x: [x.split(' ')[0], x.split(' ')[-1]])
# check whether it is ip address or not
data = data.filter(lambda x: (x[0].split('.'))[-1].isnumeric())
# throw the data if it doesn't have bytes information
data = data.filter(lambda x: x[1] != '-')
# calculate the total bytes based on the same IP
data = data.reduceByKey(lambda x, y: int(x) + int(y))
# sort data from big to small based on byte
data = data.sortBy(lambda x: -int(x[1]))

# take top 10 data
top_10 = data.take(10)
# take top 100 data
top_100 = data.take(100)

# start writing data into csv
import csv

# open the file in the write mode
with open("C:\\Users\\ee527\\Desktop\\hw1_2_top10.csv", 'w', newline='') as f:
    # create the csv writer
    writer = csv.writer(f)
    
    for row in top_10:
        
        # write a row to the csv file
        writer.writerow(row)

# open the file in the write mode
with open("C:\\Users\\ee527\\Desktop\\hw1_2_top100.csv", 'w', newline='') as f:
    # create the csv writer
    writer = csv.writer(f)
    
    for row in top_100:
        
        # write a row to the csv file
        writer.writerow(row)