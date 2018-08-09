from pyspark.sql import SparkSession

# Read the lz.tweets table and return a dataframe
def get_dataframe(spark=None):
    tweets_df = spark.read \
                    .format("org.apache.spark.sql.cassandra") \
                    .options(table="tweets", keyspace="lz") \
                    .load()

    return tweets_df


# Place the transformation - count tweets by language and tag
def transform_dataframe(tweets_df=None, sparkSession=None):
    # create or replace a lazy evaluated view to enable spark sql
    tweets_df \
        .createOrReplaceTempView("tweets")

    # define a spark.sql query to count and group data
    s_query = "select " \
                    + "process_date, " \
                    + "author_id, " \
                    + "followers_count, "\
                    + "friends_count, " \
                    + "tag, " \
                    + "username " \
            + "from tweets " \
            + "group by process_date, " \
                    + "author_id, followers_count, " \
                    + "friends_count, tag, username "

    # apply the query
    distinct_ordered_df = spark.sql(s_query)

    return distinct_ordered_df


# persist the transformed data on the consumption zone table
def persist_transformed_data(transformed_data_frame=None):
    #save the data on the consumption table
    try:
        transformed_data_frame.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode('append') \
            .options(table="most_followed_users_by_process_date", keyspace="cz") \
            .save()

        return True
    except:
        return False


# Main function
if __name__ == '__main__':

    # Declares the SparkSession
    spark = SparkSession \
        .builder \
        .appName("PySpark Most Followed Users Processor") \
        .getOrCreate()

    tweets_dataframe = get_dataframe(spark)

    transformed_dataframe = transform_dataframe(tweets_df=tweets_dataframe,
                                                sparkSession=spark)

    result = persist_transformed_data(transformed_data_frame=transformed_dataframe)



