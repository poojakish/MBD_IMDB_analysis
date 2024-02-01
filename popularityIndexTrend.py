from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("popularityIndexTrend").getOrCreate()

# Load data
title_basics = spark.read.csv("/user/s2324903/imdb-dataset/processed/title.basics.csv", header=True).select('tconst', 'startYear')
popIndex = spark.read.csv("/user/s2324903/imdb-dataset/processed/final.popindex.csv", header=True).select("tconst", "normalized_popularity_index")

# Join title_basics and popIndex
joined = title_basics.join(popIndex, "tconst").orderBy("startYear")

# Group by 'startYear' and calculate the avg of 'normalized_popularity_index'
grouped_result = joined.groupBy("startYear").agg({"normalized_popularity_index": "avg"})

# Drop 'tconst'
result = grouped_result.drop("tconst")

# Write to CSV
result.coalesce(1).write.csv("/user/s2345536/ratingTrendPopIndex/", mode="overwrite")
