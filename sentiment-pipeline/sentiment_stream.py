from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, current_timestamp
from pyspark.sql.types import DoubleType, StringType
from textblob import TextBlob

# -------------------------------------------------
# Spark session
# -------------------------------------------------
spark = (
    SparkSession.builder
    .appName("SentimentStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------
# Kafka source
# -------------------------------------------------
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "social_raw")
    .option("startingOffsets", "latest")
    .load()
)

# -------------------------------------------------
# Parse message
# -------------------------------------------------
text_df = df.selectExpr("CAST(value AS STRING) AS text")

# -------------------------------------------------
# Sentiment UDFs
# -------------------------------------------------
def polarity(text):
    return float(TextBlob(text).sentiment.polarity)

def label(score):
    if score > 0:
        return "positive"
    elif score < 0:
        return "negative"
    else:
        return "neutral"

polarity_udf = udf(polarity, DoubleType())
label_udf = udf(label, StringType())

result = (
    text_df
    .withColumn("polarity", polarity_udf(col("text")))
    .withColumn("sentiment", label_udf(col("polarity")))
    .withColumn("created_at", current_timestamp())
)

# -------------------------------------------------
# Write to Postgres (foreachBatch)
# -------------------------------------------------
def write_to_postgres(batch_df, batch_id):
    print(f">>> WRITE TO POSTGRES | batch_id={batch_id}, rows={batch_df.count()}")

    (
        batch_df
        .select("text", "polarity", "sentiment", "created_at")
        .write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/sentiment")
        .option("dbtable", "sentiment_results")
        .option("user", "admin")
        .option("password", "admin")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

# -------------------------------------------------
# Streaming query
# -------------------------------------------------
query = (
    result.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .trigger(processingTime="60 seconds") # process theo batch xx phút
    .start()
)

query.awaitTermination()