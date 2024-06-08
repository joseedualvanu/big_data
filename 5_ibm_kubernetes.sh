# 1) clone the code
git clone https://github.com/ibm-developer-skills-network/fgskh-new_horizons.git

# 2) change directory
cd fgskh-new_horizons

# 3) Add an alias to kubectl. This will help you just type k instead of kubectl.
alias k='kubectl'

# 4) Save the current namespace in an environment variable that will be used later
my_namespace=$(kubectl config view --minify -o jsonpath='{..namespace}')

# 5) Install the Apache Spark POD
k apply -f spark/pod_spark.yaml

# 6) Now it is time to check the status of the Pod by running the following command.
k get po

# 7) delete the pod
k delete po spark

# 8) start over
k apply -f spark/pod_spark.yaml

# 9) check status
k get po

# 10) Now it is time to run a command inside the spark container of this Pod.
k exec spark -c spark  -- echo "Hello from inside the container"
