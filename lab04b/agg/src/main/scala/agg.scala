import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger


object agg {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("lab04b")
      .getOrCreate()

    def sinkWithCheckpoint(chkName: String, mode: String, df: DataFrame) = {
      df
        .writeStream
        .format("kafka")
        .outputMode(mode)
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .option("topic", "kirill_sitnikov_lab04b_out")
        .option("checkpointLocation", s"/tmp/kirill_sitnikov_lab04b/$chkName")
        .option("truncate", "false")
//        .option("numRows", "20")
    }

    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> "kirill_sitnikov" // kirill_sitnikov
//      "startingOffsets" -> "earliest"
//      "maxOffsetsPerTrigger" -> "1000"
    )

    val schema = new StructType()
      .add("event_type", StringType)
      .add("category", StringType)
      .add("item_id", StringType)
      .add("item_price", IntegerType)
      .add("uid", StringType)
      .add("timestamp", LongType)

    val sdf = spark
      .readStream
      .format("kafka")
      .options(kafkaParams)
      .load
      . selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("data"))
      .select("data.*")

//    val serialSdf = sdf.select(
//      col("value").cast("string"),
//      col("topic"),
//      col("partition"),
//      col("offset"),
//      col("timestamp"),
//      col("timestampType")
//    )

//    val parsedSdf = sdf.withColumn(
//      "value_ext", from_json(col("value"), schema)
//    ).select(
//      col("value_ext.event_type").alias("event_type"),
//      col("value_ext.category").alias("category"),
//      col("value_ext.item_id").alias("item_id"),
//      col("value_ext.item_price").alias("item_price"),
//      col("value_ext.uid").alias("uid"),
//      col("value_ext.timestamp").alias("timestamp").cast("timestamp")
//    )

    val groupedSdf = sdf
      .withWatermark("timestamp", "1 hours")
      .groupBy(window(col("timestamp"), "1 hours"))
      .agg(sum(when(col("event_type") === "buy", col("item_price")).otherwise(0)).alias("revenue"),
        count(col("uid").isNotNull).alias("visitors"),
        count(col("event_type") === "buy").alias("purchases")
      )
      .select(
        (col("window.start").cast("long") / 1000).alias("start_ts"),
        (col("window.end").cast("long") / 1000).alias("end_ts"),
        col("revenue"),
        col("visitors"),
        col("purchases"),
        (col("revenue") / col("purchases")).alias("aov")
      )

    val sink = sinkWithCheckpoint("check", "update", groupedSdf)
    val sq = sink.start

    sq.awaitTermination

  }

}
