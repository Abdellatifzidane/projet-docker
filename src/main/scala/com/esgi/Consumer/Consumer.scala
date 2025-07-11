package com.esgi

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import java.util.Properties


object ConsumerKafka {
  // Define schema for the complete API response
  val apiResponseSchema = new StructType()
    .add("rows", ArrayType(
      new StructType()
        .add("row", 
          new StructType()
            .add("nutriscore_grade", StringType)
            .add("categories_tags", ArrayType(StringType))
        )
    ))

  def main(args: Array[String]): Unit = {
  val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
  val checkpoint = sys.env.getOrElse("CHECKPOINT_PATH", "checkpoint/generic")

  val spark = SparkSession.builder()
    .appName("ConsumerKafka")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "5")
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  // Read from Kafka
  val rawStream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("subscribe", "openfood")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()

  // Transform data
  val transformedStream = rawStream
    .select(from_json(col("value").cast("string"), apiResponseSchema).as("data"))
    .select(explode(col("data.rows")).as("row"))
    .select("row.row.*")
    .transform(applyTransformations)

  // Write to both console and PostgreSQL
  val query = transformedStream.writeStream
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      println(s"=== Batch $batchId ===")
      batchDF.show(1000, truncate = false) // Console output

      // Write to PostgreSQL
      writeToPostgres(batchDF)
    }
    .outputMode("complete")
    .option("checkpointLocation", checkpoint)
    .start()

  query.awaitTermination()
}

  def applyTransformations(df: DataFrame): DataFrame = {
  val spark = SparkSession.getActiveSession.get
  import spark.implicits._

  val transformed = df
    .withColumn("nutriscore", 
      when(lower($"nutriscore_grade").isin("a", "b", "c", "d", "e"), upper($"nutriscore_grade"))
      .otherwise("UNKNOWN"))
    .filter($"nutriscore_grade".isNotNull)
    .select("nutriscore")

  transformed
    .groupBy("nutriscore")
    .agg(count("*").as("product_count"))
    
}

def writeToPostgres(df: DataFrame): Unit = {
  val jdbcUrl = sys.env("PG_URL")
  val dbProps = new Properties()
  dbProps.setProperty("user", sys.env("PG_USER"))
  dbProps.setProperty("password", sys.env("PG_PWD"))
  dbProps.setProperty("driver", "org.postgresql.Driver")

  df.write
    .mode("overwrite") // ou "append" si tu veux cumuler les valeurs
    .jdbc(jdbcUrl, "nutriscore_counts", dbProps)
}
}