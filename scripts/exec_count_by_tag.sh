#!/usr/bin/env bash

echo "SETTING ENVIRONMENT VARIABLES";
export APP_DIR=/Users/Marku/Documents/WorkSpace/pyspark_tweets_transform
export SPARK_HOME=/usr/local/opt/spark-2.2.1-bin-hadoop2.7;
export PYSPARK_CASSANDRA_JAR=$APP_DIR/lib/pyspark-cassandra-assembly-0.9.0.jar;
export CASSANDRA_HOSTS=127.0.0.1;
export SCRIPT_PATH=$APP_DIR/src/process_count_by_tag.py;

echo "SUBMITING SPARK JOB"
$SPARK_HOME/bin/spark-submit \
    --executor-memory 4G \
    --num-executors 4 \
    --executor-cores 2 \
    --jars $PYSPARK_CASSANDRA_JAR \
    --py-files $PYSPARK_CASSANDRA_JAR \
    --conf spark.cassandra.connection.host=$CASSANDRA_HOSTS \
    $SCRIPT_PATH;