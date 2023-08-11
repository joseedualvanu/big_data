# Installing required packages
#!pip install pyspark
#!pip install findspark
import pyspark
import findspark
findspark.init()

# PySpark is the Spark API for Python. In this lab, we use PySpark to initialize the spark context. 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Exercise 1 - Spark Context and Spark Session
# Task 1: Creating the spark session and context
# Creating a spark context class
sc = SparkContext()

# Creating a spark session
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Task 2: Initialize Spark session¶
spark

# Exercise 2: RDDs
# Task 1: Create an RDD.
# For demonstration purposes, we create an RDD here by calling sc.parallelize()
# We create an RDD which has integers from 1 to 30

data = range(1,30)
# print first element of iterator
print(data[0])
len(data)
xrangeRDD = sc.parallelize(data, 4)

# this will let us know that we created an RDD
xrangeRDD

# Task 2: Transformations
# A transformation is an operation on an RDD that results in a new RDD. The transformed RDD is generated rapidly because the new RDD is lazily evaluated, 
# which means that the calculation is not carried out when the new RDD is generated. 
# The RDD will contain a series of transformations, or computation instructions, that will only be carried out when an action is called. 
# In this transformation, we reduce each element in the RDD by 1. Note the use of the lambda function. We also then filter the RDD to only contain elements <10.
subRDD = xrangeRDD.map(lambda x: x-1)
filteredRDD = subRDD.filter(lambda x : x<10)

# Task 3: Actions
# A transformation returns a result to the driver. We now apply the collect() action to get the output from the transformation.
print(filteredRDD.collect())
filteredRDD.count()

# Task 4: Caching Data
# This simple example shows how to create an RDD and cache it. Notice the 10x speed improvement! 
# If you wish to see the actual computation time, browse to the Spark UI...it's at host:4040. 
# You'll see that the second calculation took much less time!
import time

test = sc.parallelize(range(1,50000),4)
test.cache()

t1 = time.time()
# first count will trigger evaluation of count *and* cache
count1 = test.count()
dt1 = time.time() - t1
print("dt1: ", dt1)

t2 = time.time()
# second count operates on cached data only
count2 = test.count()
dt2 = time.time() - t2
print("dt2: ", dt2)

test.count()

# Exercise 3: DataFrames and SparkSQL
# In order to work with the extremely powerful SQL engine in Apache Spark, you will need a Spark Session. 
# We have created that in the first Exercise, let us verify that spark session is still active.
spark

# Task 1: Create Your First DataFrame!
# You can create a structured data set (much like a database table) in Spark. 
# Once you have done that, you can then use powerful SQL tools to query and join your dataframes.

# Download the data first into a local `people.json` file
# !curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/people.json >> people.json

# Read the dataset into a spark dataframe using the `read.json()` function
df = spark.read.json("0_private/spark-hadoop/people.json").cache()

# Print the dataframe as well as the data schema
df.show()
df.printSchema()

# Register the DataFrame as a SQL temporary view
df.createTempView("people")

# Task 2: Explore the data using DataFrame functions and SparkSQL
# In this section, we explore the datasets using functions both from dataframes as well as corresponding SQL queries using sparksql. 
# Note the different ways to achieve the same task!

# Select and show basic data columns
df.select("name").show()
df.select(df["name"]).show()
spark.sql("SELECT name FROM people").show()

# Perform basic filtering
df.filter(df["age"] > 21).show()
spark.sql("SELECT age, name FROM people WHERE age > 21").show()

# Perfom basic aggregation of data
df.groupBy("age").count().show()
spark.sql("SELECT age, COUNT(age) as count FROM people GROUP BY age").show()

##################################################################################
######################## QUESTIONS ###############################################
##################################################################################

# Question 1 - RDDs
# Create an RDD with integers from 1-50. Apply a transformation to multiply every number by 2, resulting in an RDD that contains the first 50 even numbers.
data2 = range(1,50)
# print first element of iterator
print(data2[0])
len(data2)
xrangeRDD2 = sc.parallelize(data2, 4)

# this will let us know that we created an RDD
xrangeRDD2
subRDD2 = xrangeRDD2.map(lambda x: x*2)

# Question 2 - DataFrames and SparkSQL
# Similar to the people.json file, now read the people2.json file into the notebook, 
# load it into a dataframe and apply SQL operations to determine the average age in our people2 file.
# Read the dataset into a spark dataframe using the `read.json()` function
df2 = spark.read.json("0_private/spark-hadoop/people2.json").cache()

df2.show()
df2.printSchema()

# Register the DataFrame as a SQL temporary view
df2.createTempView("people2")

# Age average
spark.sql("SELECT AVG(age) FROM people2").show()

subRDD4 = xrangeRDD.map(lambda x: x-1)
filteredRDD = subRDD.filter(lambda x : x<10)

# Question 3 - SparkSession
# Close the SparkSession we created for this notebook

# will stop the spark session
spark.stop() 



