# 1) Clone the repository to your environment.
git clone https://github.com/ibm-developer-skills-network/ooxwv-docker_hadoop.git

# 2) Navigate to the docker-hadoop directory to build it.
cd ooxwv-docker_hadoop

# 3) Compose the docker application.
# Compose is a tool for defining and running multi-container Docker applications. 
# It uses the YAML file to configure the services and enables us to create and start all the services from just one configurtation file.
docker-compose up -d

# 4) Run the namenode as a mounted drive on bash.
docker exec -it namenode /bin/bash

# 5)  As you have learnt in the videos and reading thus far in the course, a Hadoop environment is configured by editing a set of configuration files:
#     hadoop-env.sh Serves as a master file to configure YARN, HDFS, MapReduce, and Hadoop-related project settings.
#     core-site.xml Defines HDFS and Hadoop core properties
#     hdfs-site.xml Governs the location for storing node metadata, fsimage file and log file.
#     mapred-site-xml Lists the parameters for MapReduce configuration.
#     yarn-site.xml Defines settings relevant to YARN. It contains configurations for the Node Manager, Resource Manager, Containers, and Application Master.
# For the docker image, these xml files have been configured already. You can see these in the directory /opt/hadoop-3.2.1/etc/hadoop/ by running
ls /opt/hadoop-3.2.1/etc/hadoop/*.xml

# 6) In the HDFS, create a directory structure named user/root/input.
hdfs dfs -mkdir -p /user/root/input

# 7) Copy all the hadoop configuration xml files into the input directory.
hdfs dfs -put $HADOOP_HOME/etc/hadoop/*.xml /user/root/input

# 8) Create a data.txt file in the current directory.
curl https://raw.githubusercontent.com/ibm-developer-skills-network/ooxwv-docker_hadoop/master/SampleMapReduce.txt --output data.txt 

# 9) Copy the data.txt file into /user/root.
hdfs dfs -put data.txt /user/root/

# 10) Check if the file has been copied into the HDFS by viewing its content.
hdfs dfs -cat /user/root/data.txt

# You can see the files in the next local host
http://localhost:9870