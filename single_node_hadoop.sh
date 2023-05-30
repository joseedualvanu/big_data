# in the terminal: 

# 1) Download hadoop-3.2.3.tar.gz to your theia environment by running the following command.
curl https://dlcdn.apache.org/hadoop/common/hadoop-3.2.3/hadoop-3.2.3.tar.gz --output hadoop-3.2.3.tar.gz

# 2) Extract the tar file in the currently directory
tar -xvf hadoop-3.2.3.tar.gz

# 3) Navigate to the hadoop-3.2.3 directory.
cd hadoop-3.2.3

# 4) Check the hadoop command to see if it is setup. This will display the usage documentation for the hadoop script.
bin/hadoop

# 5) Run the following command to download data.txt to your current directory.
curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/data.txt --output data.txt

# 6) Once the word count runs successfully, you can run the following command to see the output file it has generated.
ls output

# 7) Run the following command to see the word count output.
cat  output/part-r-00000