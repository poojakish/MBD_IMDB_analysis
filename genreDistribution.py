from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split

spark = SparkSession.builder.appName("popIndex").getOrCreate()

# Load data
title_ratings = spark.read.csv("/user/s2324903/imdb-dataset/title.ratings.tsv", sep=r'\t', header=True).select('tconst','startYear','genres')
popIndex = spark.read.csv("/user/s2324903/imdb-dataset/Popularity_Index.csv", header=True).select('tconst', 'normalized_popularity_index')

# Split genres and explode genre
in_array = title_basics.withColumn("genre_array", split(title_basics["genres"], ","))
exploded = in_array.select("tconst", "startYear", explode("genre_array").alias("genre"))

# join genres with popularity on tconst
joined = exploded.join(popIndex, "tconst")

# Group by Decade and Genre, count the number of movies
df_with_decade = result.withColumn("decade", floor(col("startYear") / 10) * 10)
genre_distribution = df_with_decade.groupBy("Decade", "Genre").count()
result = genre_distribution.orderBy("Decade", "Genre")

result.coalesce(1).write.csv("/user/s2345536/genreDistribution/", mode="overwrite")
