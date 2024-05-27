
# 3) Navigate to the hadoop-3.2.3 directory.
cd hadoop-3.2.3

# 4) Check the hadoop command to see if it is setup. This will display the usage documentation for the hadoop script.
bin/hadoop

# 5) Run the following command to download data.txt to your current directory.
curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/data.txt --output data.txt

# 6) Run the following command to download data.txt to your current directory.
bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar wordcount data.txt output

# 7) Once the word count runs successfully, you can run the following command to see the output file it has generated.
ls output

# 8) Run the following command to see the word count output.
cat  output/part-r-00000