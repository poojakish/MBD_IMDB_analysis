from pyspark.sql import SparkSession
from pyspark.sql.functions import col,countDistinct,avg

spark = SparkSession.builder.appName("CalculateDecadeRegion").getOrCreate()

df_popularityIndex = spark.read.option("header", "true").csv("/user/s3176649/Results/PopularityIndexFinal.csv")

df_regions = spark.read.option("header", "true").csv("/user/s2324903/imdb-dataset/processed/title.regions.csv")

df_distinctRelCount = df_regions.groupBy("titleId").agg(countDistinct("region").alias("unique_regions_count"))
# df_results = df_popularityIndex.join( df_distinctRelCount, df_popularityIndex['tconst'] == df_distinctRelCount['titleId'], 'inner')

df_title= spark.read.option("header", "true").csv("//user/s2324903/imdb-dataset/processed/title.basics.csv ")

df_Movies= df_title.filter(df_title['titleType'].isin('movie','tvSeries','tvMovie')).select("tconst","startYear","runtimeMinutes","genres")

df_Movies= df_Movies.join(df_popularityIndex.alias('df_popularityIndex'),df_Movies['tconst']==df_popularityIndex['tconst'],'inner')

df_Movies=df_Movies.withColumn("startYear", col("startYear").cast("int"))
df_movies_1940= df_Movies.filter(df_Movies['startYear']>=1940)

df_movies_1940=df_movies_1940.drop(df_popularityIndex["tconst"])

df_movies_final= df_movies_1940.join(df_distinctRelCount,df_distinctRelCount['titleId']==df_movies_1940['tconst'],'inner')

df_results = df_movies_final.groupBy("unique_regions_count").agg(avg("normalized_popularity_index").alias("avg_popularity_index"))

df_results= df_results.orderBy("unique_regions_count") ##the results don't have much difference so need to think about...

df_results.coalesce(1).write.csv("/user/s3176649/Results/RegionPopularity_New", mode='overwrite', header=True)