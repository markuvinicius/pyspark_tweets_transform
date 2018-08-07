from pyspark.sql import SparkSession

spark = SparkSession \
            .builder \
            .appName("Python Spark SQL Cassandra example") \
            .getOrCreate()



tweets = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="tweets", keyspace="lz") \
        .load()

tweets\
    .filter(tweets['lang']=='pt')\
    .createOrReplaceTempView("tweets")

distinct_ordered = spark.sql("select " \
                              + "tag, count(1) as qtd " \
                            + "from tweets " \
                            + "group by tag")

distinct_ordered.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode('append') \
    .options(table="count_pt_tweets_by_tag", keyspace="cz") \
    .save()




