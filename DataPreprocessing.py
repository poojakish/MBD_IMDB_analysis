from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when

spark = SparkSession.builder.appName("Data Preprocessing").getOrCreate()

df_nameBasic = spark.read.option("header", "true").option("delimiter", "\t").csv("/user/s2324903/imdb-dataset/name.basics.tsv")

df_nameBasic_filtered = df_nameBasic.filter(df_nameBasic['birthYear'] != '\\N' or df_nameBasic['birthYear'] != '\\\\N')
#gave more than 90% as \n so changing the value to NA instead.

for col_name in df_nameBasic.columns:
    df_nameBasic = df_nameBasic.withColumn(col_name, when(col(col_name) == '\\N', 'NA').otherwise(col(col_name)))

df_nameBasic.coalesce(1).write.csv("/user/s3176649/Results/PreprocessedNameBasic", mode='overwrite', header=True)
