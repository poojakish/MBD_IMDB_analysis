from pyspark.sql import SparkSession
from pyspark.sql.functions import col,countDistinct,avg,collect_set,dense_rank,concat_ws
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("PopularityCast").getOrCreate()

# Reading data.
df_popularityIndex = spark.read.option("header", "true").csv("/user/s2324903/imdb-dataset/processed/final.popindex.csv")
df_title= spark.read.option("header", "true").csv("/user/s2324903/imdb-dataset/processed/title.basics.csv")
df_prin = spark.read.option("header", "true").option("delimiter", "\t").csv("/user/s2324903/imdb-dataset/title.principals.tsv")
df_names = spark.read.option("header", "true").csv("/user/s2324903/imdb-dataset/processed/name.basics.csv")

#filtered only movies and series. (removed "tvSeries")
df_Movies= df_title.filter(df_title['titleType'].isin(['movie','tvMovie'])).select("tconst","startYear","runtimeMinutes","genres","titleType","primaryTitle")
df_Movies= df_Movies.join(df_popularityIndex.alias('df_popularityIndex'),df_Movies['tconst']==df_popularityIndex['tconst'],'inner')
#filtering movie and series started after 1950
df_Movies=df_Movies.withColumn("startYear", col("startYear").cast("int"))
df_movies_1950= df_Movies.filter(df_Movies['startYear']>= 1950)
df_movies_1950=df_movies_1950.drop(df_popularityIndex["tconst"])
#dropping unnecessary columns.
df_movies_final=df_movies_1950.drop('normalized_rating','numVotes','averageRating')

#filtering out the actors alone.
actors = df_prin.filter(col("category") == "actor")
result1 = actors.join(df_movies_final, df_movies_final['tconst'] == actors['tconst'],'inner').drop(df_movies_final['tconst']) # principle joined with popularity index
df_rank = result1.withColumn("rank", dense_rank().over(Window.partitionBy("nconst").orderBy(col("normalized_popularity_index").desc())))

window_spec = Window().partitionBy('nconst')
df_MoreThan10 = df_rank.withColumn('Record_count', F.count('rank').over(window_spec))
df_MoreThan30_filteres = df_MoreThan10.filter(df_MoreThan10['record_count'] > 30)
df_MoreThan30_filteres.show()

#Ranks of the actors to 5 movies
df_rank_10 = df_MoreThan30_filteres.filter(col("rank") <= 10)
#df_rank_5 = df_MoreThan30_filteres.filter(col("rank")<=5)

#avg rating for the actors
result2 = df_rank_10.groupBy(df_rank_10["nconst"]).agg(avg("normalized_popularity_index").alias("average_popularity"))

#agg genres...
result3 = result1.groupBy(result2["nconst"]).agg(collect_set("genres").alias("aggregated_genres"))

actor_final = df_names.join(result2, result2['nconst'] == df_names['nconst'],'inner').drop(result2['nconst'])
actor_final = actor_final.join(result3, result3['nconst'] == actor_final['nconst'],'inner').drop(actor_final['nconst'])

#concatenate the actor genre and find the results.
actor_concat = actor_final.withColumn("aggregated_genres", concat_ws(", ", col("aggregated_genres")))
actor_concat.coalesce(1).write.csv("/user/s3176649/Results/actorPopularity", mode='overwrite', header=True)