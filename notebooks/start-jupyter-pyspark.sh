#!/usr/bin/env bash
echo '======================================================================================================='
echo 'THIS SCRIPT ASSUMES THAT YOU ALREADY HAVE JUPYTER NOTEBOOK INSTALLED AND CONFIGURED ON YOUR ENVIRONMENT'
echo '======================================================================================================='

echo '======================================================================================================='
echo "SETTING ENVIRONMENT VARIABLES";
echo '======================================================================================================='
export SPARK_HOME=/usr/local/opt/spark-2.2.1-bin-hadoop2.7;
export APP_DIR=/Users/Marku/Documents/WorkSpace/pyspark_tweets_transform;
export PYSPARK_CASSANDRA_JAR=$APP_DIR/lib/pyspark-cassandra-assembly-0.9.0.jar;
export CASSANDRA_HOSTS=127.0.0.1;
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'

echo '======================================================================================================='
echo "STARTING JUPYTER NOTEBOOK"
echo '======================================================================================================='

$SPARK_HOME/bin/pyspark \
    --executor-memory 4G \
    --num-executors 4 \
    --executor-cores 2 \
    --jars $PYSPARK_CASSANDRA_JAR \
    --py-files $PYSPARK_CASSANDRA_JAR \
    --conf spark.cassandra.connection.host=$CASSANDRA_HOSTS \