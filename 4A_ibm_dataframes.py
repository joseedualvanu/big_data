# # Installing required packages
# !pip install pyspark
# !pip install findspark
# !pip install pyarrow==0.14.1
# !pip install pandas
# !pip install numpy==1.19.5

import findspark
findspark.init()

import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

print(" Exercise 1 - Spark session ")

# In this exercise, you will create and initialize the Spark session needed to load the dataframes and operate on it
print(" Task 1: Creating the spark session and context ")
# Creating a spark context class
sc = SparkContext()

# Creating a spark session
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

print( "Task 2: Initialize Spark session")
# To work with dataframes we just need to verify that the spark session instance has been created.
spark

print(" Exercise 2 - Load the data and Spark dataframe ")
print(" Task 1: Loading data into a Pandas DataFrame ")

# Read the file using `read_csv` function in pandas
mtcars = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/mtcars.csv')

# Preview a few records
print(mtcars.head())

print("Task 2: Loading data into a Spark DataFrame")
# We use the `createDataFrame` function to load the data into a spark dataframe
sdf = spark.createDataFrame(mtcars) 
# Let us look at the schema of the loaded spark dataframe
# sdf.printSchema()

print(" Exercise 3: Basic data analysis and manipulation" )
# In this section, we perform basic data analysis and manipulation. We start with previewing the data and then applying some filtering and columwise operations.
print ("Task 1: Displays the content of the DataFrame")
# We use the show() method for this. Here we preview the first 5 records. Compare it to a similar head() function in Pandas.
sdf.show(5)

# We use the select() function to select a particular column of data. Here we show the mpg column.
sdf.select('mpg').show(5)

print(" Task 2: Filtering and Columnar operations ")
# Filtering and Column operations are important to select relevant data and apply useful transformations.
# We first filter to only retain rows with mpg > 18. We use the filter() function for this.
sdf.filter(sdf['mpg'] < 18).show(5)

# Operating on Columns
# Spark also provides a number of functions that can be directly applied to columns for data processing and aggregation. 
# The example below shows the use of basic arithmetic functions to convert the weight values from lb to metric ton. 
# We create a new column called wtTon that has the weight from the wt column converted to metric tons.
sdf.withColumn('wtTon', sdf['wt'] * 0.45).show(5)

print(" Exercise 4: Grouping and Aggregation" )
# Spark DataFrames support a number of commonly used functions to aggregate data after grouping. 
# In this example we compute the average weight of cars by their cylinders as shown below.
sdf.groupby(['cyl'])\
.agg({"wt": "AVG"})\
.show(5)
# We can also sort the output from the aggregation to get the most common cars.
car_counts = sdf.groupby(['cyl'])\
.agg({"wt": "count"})\
.sort("count(wt)", ascending=False)\
.show(5)

##################################################################################
##################################################################################
##################################################################################

print("Practice Questions")
# Question 1 - DataFrame basics
# Display the first 5 rows of all cars that have atleast 5 cylinders.
sdf.filter(sdf['cyl'] > 4).show(5)

# Question 2 - DataFrame aggregation
# Using the functions and tables shown above, print out the mean weight of a car in our database in metric tons.

sdf.withColumn('wtTon', sdf['wt'] * 0.45).show(5)

# Question 3 - DataFrame columnar operations
# In the earlier sections of this notebook, we have created a new column called wtTon to indicate the weight in metric tons using a standard conversion formula. 
# In this case we have applied this directly to the dataframe column wt as it is a linear operation (multiply by 0.45). 
# Similarly, as part of this exercise, create a new column for mileage in kmpl (kilometer-per-liter) instead of mpg(miles-per-gallon) by using a conversion factor of 0.425.
# Additionally sort the output in decreasing order of mileage in kmpl.
sdf.withColumn('kmpl', sdf['mpg'] * 0.425).sort('mpg', ascending=False).show()