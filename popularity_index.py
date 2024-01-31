from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as spark_max, min as spark_min

spark = SparkSession.builder.appName("NormalizedPopularityIndex").getOrCreate()

df_ratings = spark.read.option("header", "true").csv("/user/s2324903/imdb-dataset/processed/title.ratings.csv")

df_ratings = df_ratings.withColumn("averageRating", col("averageRating").cast("float"))
df_ratings = df_ratings.withColumn("numVotes", col("numVotes").cast("float"))

min_max_values = df_ratings.select(
    spark_min("averageRating").alias("min_rating"),
    spark_max("averageRating").alias("max_rating"),
    spark_min("numVotes").alias("min_votes"),
    spark_max("numVotes").alias("max_votes")
).collect()[0]

min_rating = min_max_values["min_rating"]
max_rating = min_max_values["max_rating"]
min_votes = min_max_values["min_votes"]
max_votes = min_max_values["max_votes"]

df_ratings= df_ratings.withColumn("normalized_rating", (col("numVotes") - min_votes) / (max_votes-min_votes) * (lit(10.0) - lit(1.0)) + lit(1.0))

result_df = df_ratings.withColumn("normalized_popularity_index", ((df_ratings["averageRating"]*0.75) + (df_ratings["normalized_rating"]*0.25)))
coalesced_data = result_df.coalesce(1)
coalesced_data.write.csv("/user/s3176649/Results/", mode='overwrite', header=True)


# df_ratings = spark.read.option("header", "true").option("delimiter", "\t").csv("/user/s2324903/imdb-dataset/title.ratings.tsv")
