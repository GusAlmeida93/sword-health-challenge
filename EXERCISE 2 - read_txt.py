from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name,explode,split,col, concat_ws

spark = SparkSession.builder \
      .master("local[10]") \
      .appName("sword-health") \
      .getOrCreate() 

path = './files/'
df = spark.read.option("recursiveFileLookup", "true").text(path)
df = df.withColumn("filename", input_file_name())

df_count=(
  df.withColumn('word', explode(split(col('value'), ' ')))
    .groupBy('word')
    .count()
    .sort('count', ascending=False)
    .withColumn('text', concat_ws(' = ', col('word'), col('count')))
)

df_count.select('text').toPandas().to_csv('output.txt', index=False)