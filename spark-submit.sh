#!/bin/bash

export PYTHONIOENCODING=utf8

prog=$(basename $0)
pwd=$(dirname $0)
master="local[1]"
deploy_mode="client"
executor_memory=28g
driver_memory=28g
num_executors=5
executor_cores=5
prog=${prog%%.*}
packages="com.databricks:spark-csv_2.11:1.5.0"
pyfiles=""

# if [[ -e "$pwd/src/log4j.properties" ]]; then
#    export SPARK_CONF_DIR="$pwd/src/log4j.properties"
# fi

cd $pwd/src
if [[ -e "job.zip" ]]; then
   if [[ -z "$pyfiles" ]]; then
      pyfiles="--py-files jobs.zip"
   fi
fi

if [[ -e "libs.zip" ]]; then
   if [[ -z "$pyfiles" ]]; then
      pyfiles="--py-files libs.zip"
   else
      pyfiles="$pyfiles,libs.zip"
   fi
fi

#spark-submit --master $master --deploy-mode $deploy_mode wordcount.py ../data/wordcount.txt 2
spark-submit \
	--driver-java-options '-Dlog4j.configuration=file:log4j.properties' \
	--packages $packages \
	--master $master \
	--deploy-mode $deploy_mode \
	--executor-memory $executor_memory \
	--driver-memory $driver_memory \
	--num-executors $num_executors \
	--executor-cores $executor_cores  \
	--name $prog \
	--conf "spark.app.id=$prog" \
	main.py \
	"$@"
