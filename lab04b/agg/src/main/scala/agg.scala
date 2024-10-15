import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger


object agg {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.{DataFrame, SparkSession}
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.streaming.Trigger


    val spark = SparkSession.builder()
      .appName("lab04b")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> "kirill_sitnikov",
      "startingOffsets" -> "earliest",
      "maxOffsetsPerTrigger" -> "1000"
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

    val gpSdf = sdf
      .select(
        from_json(col("value").cast(StringType), schema).as("data")
      )
      .select(
        col("data.event_type").alias("event_type"),
        col("data.category").alias("category"),
        col("data.item_id").alias("item_id"),
        col("data.item_price").alias("item_price"),
        col("data.uid").alias("uid"),
        to_timestamp((col("data.timestamp").cast(LongType) / 1000).cast(LongType)).as("timestamp")
      )
      .withWatermark("timestamp", "1 hours")
      .groupBy(
        window(col("timestamp"), "1 hour")
      )
      .agg(
        sum(when(col("event_type") === "buy", col("item_price")).otherwise(0)).alias("revenue"),
        count(when(col("uid").isNotNull, col("uid"))).alias("visitors"),
        count(when(col("event_type") === "buy", col("event_type"))).alias("purchases")
      )
      .select(
        col("window.start").alias("start_ts"),
        col("window.end").alias("end_ts"),
        col("revenue"),
        col("visitors"),
        col("purchases"),
        (col("revenue") / col("purchases")).alias("aov")
      )

    val aggJsonSdf = gpSdf.select(
      to_json(struct(
        col("start_ts").cast(LongType).as("start_ts"),
        col("end_ts").cast(LongType).as("end_ts"),
        col("revenue").cast(LongType).as("revenue"),
        col("visitors").cast(LongType).as("visitors"),
        col("purchases").cast(LongType).as("purchases"),
        col("aov").cast(DoubleType).as("aov")
      )).as("value")
    )

    val resSdf = aggJsonSdf
      .writeStream
      .format("kafka")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("topic", "kirill_sitnikov_lab04b_out")
      .option("checkpointLocation", "/tmp/ks_lab04b/checkpoints")
      .start

    resSdf.awaitTermination

  }

}
