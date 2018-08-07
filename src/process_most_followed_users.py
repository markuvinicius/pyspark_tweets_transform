from pyspark.sql import SparkSession

spark = SparkSession \
            .builder \
            .appName("Python Spark SQL Cassandra example") \
            .getOrCreate()



tweets = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="tweets", keyspace="lz") \
        .load()

tweets.createOrReplaceTempView("tweets")

distinct_ordered = spark.sql("select " \
                                + "process_date, " \
                                + "author_id, " \
                                + "followers_count, "\
                                + "friends_count, " \
                                + "tag, " \
                                + "username " \
                            + "from tweets " \
                            + "group by process_date, " \
                                + "author_id, followers_count, " \
                                + "friends_count, tag, username ")

distinct_ordered.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode('append') \
    .options(table="most_followed_users_by_process_date", keyspace="cz") \
    .save()




