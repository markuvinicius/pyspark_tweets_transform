#!/usr/bin/env bash

echo "SETTING ENVIRONMENT VARIABLES";

export SPARK_HOME=/usr/local/opt/spark-2.2.1-bin-hadoop2.7;
export PYSPARK_CASSANDRA_JAR=/Users/Marku/Documents/WorkSpace/pyspark-cassandra/target/scala-2.11/pyspark-cassandra-assembly-0.9.0.jar;
export CASSANDRA_HOSTS=127.0.0.1;
export SCRIPT_PATH=/Users/Marku/Documents/WorkSpace/pyspark_tweets_transform/src/process_count_by_tag.py;

echo "SUBMITING SPARK JOB"

$SPARK_HOME/bin/spark-submit \
--jars $PYSPARK_CASSANDRA_JAR \
--py-files $PYSPARK_CASSANDRA_JAR \
--conf spark.cassandra.connection.host=$CASSANDRA_HOSTS \
$SCRIPT_PATH;