import findspark

findspark.init("/opt/manual/spark")





from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[2]").appName("File Source CSV Stateless").config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')






schema= "Fideid string,Name string,Federation string,Gender string,Year_of_birth string,Title string,Standard_Rating string,Rapid_rating string,Blitz_rating string,Inactive_flag string"


df = (spark.readStream.format("csv")
         .schema(schema)
         .option("header", False)
         .option("maxFilesPerTrigger", 1)
         .load("file:///home/train/data-generator/output"))


new_columns = [col_name.replace(" ", "") for col_name in df.columns]
df = df.toDF(*new_columns)

## Calculate AGE per player



df= df.withColumn("Year_of_birth", F.col("Year_of_birth").cast("integer"))


current_date_value = F.current_date()


df= df.withColumn("current_year",F.year(current_date_value))

df= df.withColumn("current_year",F.col("current_year").cast("integer"))


df= df.withColumn("Age", F.col("current_year") -F.col("Year_of_birth"))

## Filter counted 2 name

##df= df.withColumn("Name_New", F.split(F.col("Name"),","))
df= df.withColumn("Name_split", F.split("Name",","))

df= df.withColumn("Name_first", F.col("Name_split")[0])

df= df.withColumn("Name_count",F.size(F.split("Name_first"," ")))

df= df.filter("Name_count = 2")

## Produce VALUE for Kafka

df = df.select(
    F.to_json(F.struct(
        F.col("Fideid"),
        F.col("Name"),
        F.col("Federation"),
        F.col("Gender"),
        F.col("Year_of_birth"),
        F.col("Title"),
        F.col("Standard_Rating"),
        F.col("Rapid_rating"),
        F.col("Blitz_rating"),
        F.col("Inactive_flag"),
        F.col("current_year"),
        F.col("Age")
    )).alias("value")
)







# Start streaming and write output to console for testing , after test have been finieshed, then write to Kafka
"""
query = df \
    .writeStream \
    .outputMode("append") \
    .format("console")\
    .start()"""

checkpoint_dir = "file:///tmp/streaming/write_to_kafka"


query = (df
.writeStream
.format("kafka")
.option("kafka.bootstrap.servers", "localhost:9092")
.option("topic", "top_women_players")
.trigger(processingTime="2 second")
.outputMode("append")
.option("checkpointLocation", checkpoint_dir)
.start())


query.awaitTermination(100)


