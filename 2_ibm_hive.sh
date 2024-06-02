
# 1: Create a directory named data under /home/project by running the following command.
mkdir /home/project/data

# 2: Change to the /home/project/data directory.
cd /home/project/data

# 3: Run the following command to get the emp.csv, a data file with Employee data, in a comma-separated file which you will use later to infuse data into the table you create.
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/data/emp.csv

# 4: Open the file in editor and view the file.
cat /home/project/data/emp.csv

# 4: You will use the hive from the docker hub for this lab. Pull the hive image into your system by running the following command.
docker pull apache/hive:4.0.0-alpha-1

# 5: Now, you will run the hive server on port 10002. You will name the server instance myhiveserver. We will mount the local data folder in the hive server as hive_custom_data. 
# This would mean that the whole data folder that you created locally, along with anything you add in the data folder, is copied into the container under the directory hive_custom_data
docker run -d -p 10000:10000 -p 10002:10002 --env SERVICE_NAME=hiveserver2 -v /home/project/data:/hive_custom_data --name myhiveserver apache/hive:4.0.0-alpha-1

# 6: You can open and take a look at the Hive server with the GUI. Click the button to open the HiveServer2 GUI.
localhost:10002

# 7: Now run the following command, which allows you to access beeline. This is a SQL cli where you can create, modify, delete table, and access data in the table.
docker exec -it myhiveserver beeline -u 'jdbc:hive2://localhost:10000/'

# 8: To create a new table Employee with three columns as in the csv you downloaded - em_id, emp_name and salary, run the following command.
create table Employee(emp_id string, emp_name string, salary  int)  row format delimited fields terminated by ',' ;

# 9: Run the following command to check if the table is created.
show tables;

# 10: Now load the data into the table from the csv file by running the following command.
LOAD DATA INPATH '/hive_custom_data/emp.csv' INTO TABLE Employee;

# 11: Run the following command to list all the rows from the table to check if the data has been loaded from the CSV.
SELECT * FROM employee;

# 12: You can view the details of the commands and the outcome in the HiveServer2 GUI.
localhost:10002

# Hive internally uses MapReduce to process and analyze data. When you execute a Hive query, it generates MapReduce jobs that run on the Hadoop cluster.