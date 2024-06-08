# preparation

#python3 -m pip install pyspark

# 1) get the lastest code
git clone https://github.com/big-data-europe/docker-spark.git

# 2) Change the directory to the downloaded code:
cd docker-spark

# 3) start the cluster (leave the terminal running)
docker-compose up

# 4) create submit.py file
touch submit.py

# 5) add the next code
    import findspark
    findspark.init()
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructField, StructType, IntegerType, StringType
    sc = SparkContext.getOrCreate(SparkConf().setMaster('spark://localhost:7077'))
    sc.setLogLevel("INFO")
    spark = SparkSession.builder.getOrCreate()
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(
        [
            (1, "foo"),
            (2, "bar"),
        ],
        StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("txt", StringType(), False),
            ]
        ),
    )
    print(df.dtypes)
    df.show()

# 6) In the terminal, run the following commands to 
# upgrade the pip installer to ensure you have the latest version by running the following commands.
rm -r ~/.cache/pip/selfcheck/
pip3 install --upgrade pip
pip install --upgrade distro-info

# 7) Please enter the following commands in the terminal to download the spark environment.
wget https://archive.apache.org/dist/spark/spark-3.3.3/spark-3.3.3-bin-hadoop3.tgz && tar xf spark-3.3.3-bin-hadoop3.tgz && rm -rf spark-3.3.3-bin-hadoop3.tgz

# 8) Run the following commands to set up the JAVA_HOME which is preinstalled in the environment and SPARK_HOME which you just downloaded.
export JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-amd64
export SPARK_HOME=/home/project/spark-3.3.3-bin-hadoop3

# 9) Install the required packages to set up the spark environment.
pip install pyspark
python3 -m pip install findspark

# 10) Type in the following command in the terminal to execute the Python script.
python3 submit.py

#http://localhost:4040/