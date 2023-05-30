import findspark
findspark.init()

import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Creating a spark context class
sc = SparkContext()

# Creating a spark session
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

spark

# Read the file using `read_csv` function in pandas
mtcars = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/mtcars.csv')

# Preview a few records
mtcars.head()

# We use the `createDataFrame` function to load the data into a spark dataframe
sdf = spark.createDataFrame(mtcars) 

# Let us look at the schema of the loaded spark dataframe
sdf.printSchema()

sdf.show(5)

sdf.select('mpg').show(5)

sdf.filter(sdf['mpg'] < 18).show(5)

sdf.withColumn('wtTon', sdf['wt'] * 0.45).show(5)

sdf.groupby(['cyl'])\
.agg({"wt": "AVG"})\
.show(5)

car_counts = sdf.groupby(['cyl'])\
.agg({"wt": "count"})\
.sort("count(wt)", ascending=False)\
.show(5)    

