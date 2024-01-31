from pyspark.sql import SparkSession
# from pyspark.sql.functions import explode, lower, split, col
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("CountryGenre").getOrCreate()
df_regions = spark.read.option("header", "true").csv("/user/s2324903/imdb-dataset/processed/title.regions.csv").select("titleId", "region").filter("region!='NA'")
df_regions = df_regions.orderBy("region")

df_basics = spark.read.option("header", "true").csv("/user/s2324903/imdb-dataset/processed/title.basics.csv").select("tconst", "genres").filter("genres != 'NA'")

df3 = df_regions.join(df_basics, df_basics['tconst'] == df_regions['titleId'], 'left').orderBy("region")
df_replaced = df3.withColumn('genres', F.regexp_replace('genres', ', ', ','))
df_split = df_replaced.withColumn("split_column", F.split(df_replaced["genres"], ",")) \
    .withColumn("movieGenres", F.explode("split_column")) \
    .drop("split_column")
genres_to_remove = ['Short', 'Sport', 'Adult', 'Reality-TV', 'Game-Show', 'Talk-Show', r'\N', 'News','Music']
df_split = df_split.filter(~df_split['movieGenres'].isin(genres_to_remove))

df_split = df_split.filter(df_split["movieGenres"]!="Short").groupBy("region", "movieGenres").count()
window_spec = Window.partitionBy("region").orderBy(F.desc("count"))
most_preferred_genres = df_split.withColumn("rank", F.row_number().over(window_spec)).filter("rank = 1 OR rank = 2 OR rank = 3" )

most_preferred_genres.show()
coalesced_data=most_preferred_genres.coalesce(1)
coalesced_data.write.csv("/user/s3176649/Results/country_Genre_Results", mode='overwrite', header=True)
