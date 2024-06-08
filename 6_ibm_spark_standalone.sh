
# 1) Use the following command to download the data set we will be using in this lab to the container running Spark.
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/cars.csv

# 2) Stop any previously running containers with the command:
for i in `docker ps | awk '{print $1}' | grep -v CONTAINER`; do docker kill $i; done

# 3) Remove any used containers
docker rm spark-master spark-worker-1 spark-worker-2

# 4) start spark master server
docker run \
    --name spark-master \
    -h spark-master \
    -e ENABLE_INIT_DAEMON=false \
    -p 4040:4040 \
    -p 8080:8080 \
    -v `pwd`:/home/root \
    -d bde2020/spark-master:3.1.1-hadoop3.2


# 5) Start a Spark Worker that will connect to the Master:
docker run \
    --name spark-worker-1 \
    --link spark-master:spark-master \
    -e ENABLE_INIT_DAEMON=false \
    -p 8081:8081 \
    -v `pwd`:/home/root \
    -d bde2020/spark-worker:3.1.1-hadoop3.2

# 6) Launch a PySpark shell in the running Spark Master container:
docker exec \
    -it `docker ps | grep spark-master | awk '{print $1}'` \
    /spark/bin/pyspark \
    --master spark://spark-master:7077

# 7) Create a DataFrame in the shell with:
df = spark.read.csv("/home/root/cars.csv", header=True, inferSchema=True) \
    .repartition(32) \
    .cache()
df.show()

# 8) Verify you can see the application jobs page that should look like the following, although not necessarily exactly the same:
localhost:4040

# In this exercise, you will define a user-defined function (UDF) and run a query that results in an
# error. We will locate that error in the application UI and find the root cause. Finally, we will
# correct the error and re-run the query.
# 9) Define a UDF to show engine type. Copy and paste the code and click Enter.
from pyspark.sql.functions import udf
import time

@udf("string")
def engine(cylinders):
    time.sleep(0.2)  # Intentionally delay task
    eng = {6: "V6", 8: "V8"}
    return eng[cylinders]

# Add the UDF as a column in the DataFrame
df = df.withColumn("engine", engine("cylinders"))

# Group the DataFrame by “cylinders” and aggregate other columns
dfg = df.groupby("cylinders")
dfa = dfg.agg({"mpg": "avg", "engine": "first"})
dfa.show()

# The query will have failed and you should see lots of messages and outputs in the console.
# The next task will be to locate the error in the Application UI and determine the root cause.

# Fix it and run again
@udf("string")
def engine(cylinders):
    time.sleep(0.2)  # Intentionally delay task
    eng = {4: "inline-four", 6: "V6", 8: "V8"}
    return eng.get(cylinders, "other")
    
# Add the UDF as a column in the DataFrame
df = df.withColumn("engine", engine("cylinders"))

# Group the DataFrame by “cylinders” and aggregate other columns
dfg = df.groupby("cylinders")
dfa = dfg.agg({"mpg": "avg", "engine": "first"})
dfa.show()