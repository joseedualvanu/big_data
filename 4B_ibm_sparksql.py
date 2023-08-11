# # Installing required packages
# !pip install pyspark
# !pip install findspark
# !pip install pandas

import findspark
findspark.init()

# PySpark is the Spark API for Python. In this lab, we use PySpark to initialize the spark context. 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

import pandas as pd

# Exercise 1 - Spark session
# In this exercise, you will create and initialize the Spark session needed to load the dataframes and operate on it
# Task 1: Creating the spark session and context

# Creating a spark context class
sc = SparkContext()

# Creating a spark session
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

spark

#### Task 1: Loading data into a Pandas DataFrame
# Read the file using `read_csv` function in pandas
mtcars = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/mtcars.csv')
# Preview a few records
print(mtcars.head())
mtcars.rename( columns={'Unnamed: 0':'name'}, inplace=True )
print(mtcars.head())

#### Task 2: Loading data into a Spark DataFrame
sdf = spark.createDataFrame(mtcars) 
# sdf.printSchema()

sdf.createTempView("cars")

# Exercise 3 - Running SQL queries and aggregating data
# Once we have a table view, we can run queries similar to querying a SQL table. 
# We perform similar operations to the ones in the DataFrames notebook. Note the difference here however is that we use the SQL queries directly.

# Showing the whole table
spark.sql("SELECT * FROM cars").show()

# Showing a specific column
spark.sql("SELECT mpg FROM cars").show(5)

# Basic filtering query to determine cars that have a high mileage and low cylinder count
spark.sql("SELECT * FROM cars where mpg>20 AND cyl < 6").show(5)

# Aggregating data and grouping by cylinders
spark.sql("SELECT count(*), cyl from cars GROUP BY cyl").show()

# Exercise 4 - Create a Pandas UDF to apply a columnar operation
# Apache Spark has become the de-facto standard in processing big data. To enable data scientists to leverage the value of big data, Spark added a Python API in version 0.7, with support for user-defined functions (UDF). 
# These user-defined functions operate one-row-at-a-time, and thus suffer from high serialization and invocation overhead. 
# As a result, many data pipelines define UDFs in Java and Scala and then invoke them from Python.
# Pandas UDFs built on top of Apache Arrow bring you the _best of both worlds_—the ability to define low-overhead, high-performance UDFs entirely in Python. 
# In this simple example, we will build a Scalar Pandas UDF to convert the wT column from imperial units (1000-lbs) to metric units (metric tons).
# In addition, UDFs can be registered and invoked in SQL out of the box by registering a regular python function using the @pandas_udf() decorator. 
# We can then apply this UDF to our wt column.
# Task 1: Importing libraries and registering a UDF¶

# import the Pandas UDF function 
from pyspark.sql.functions import pandas_udf, PandasUDFType

@pandas_udf("float")
def convert_wt(s: pd.Series) -> pd.Series:
    # The formula for converting from imperial to metric tons
    return s * 0.45

spark.udf.register("convert_weight", convert_wt)

# Task 2: Applying the UDF to the tableview
# We can now apply the convert_weight user-defined-function to our wt column from the cars table view. 
# This is done very simply using the SQL query shown below. In this example below we show both the original weight (in ton-lbs) and converted weight (in metric tons).
spark.sql("SELECT *, wt AS weight_imperial, convert_weight(wt) as weight_metric FROM cars").show()

##################################################################################
##################################################################################
##################################################################################

# Practice Questions
# Question 1 - Basic SQL operations
# Display all Mercedez car rows from the cars table view we created earlier. The Mercedez cars have the prefix "Merc" in the car name column.
spark.sql("SELECT * FROM cars where name like 'Merc%'").show()

# Question 2 - User Defined Functions
# In this notebook, we created a UDF to convert weight from imperial to metric units. 
# Now for this exercise, please create a pandas UDF to convert the mpg column to kmpl (kilometers per liter). 
# You can use the conversion factor of 0.425.

@pandas_udf("float")
def convert_mileage(s: pd.Series) -> pd.Series:
    # The formula for converting from imperial to metric tons
    return s * 0.425

spark.udf.register("convert_mileage", convert_mileage)
spark.sql("SELECT *, mpg AS mpg, convert_weight(mpg) as kmpl FROM cars").show()
