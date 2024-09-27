import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object filter {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("lab04a")
      .getOrCreate()

    val topic_name : String = spark.sparkContext.getConf.get("spark.filter.topic_name")
    val offset : String = spark.sparkContext.getConf.get("spark.filter.offset")
    val output_dir : String = spark.sparkContext.getConf.get("spark.filter.output_dir_prefix")

    val schema = new StructType()
      .add("event_type", StringType)
      .add("category", StringType)
      .add("item_id", StringType)
      .add("item_price", IntegerType)
      .add("uid", StringType)
      .add("timestamp", LongType)

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val rawData = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", topic_name)
      .option("startingOffsets",
        if(offset.contains("earliest"))
          offset
        else {
          "{\"" + topic_name + "\":{\"0\":" + offset + "}}"
        }
      )
      .option("endingOffsets", "latest")
      .load()

    val transformedData = rawData.select(
      col("value").cast("string"),
      col("topic"),
      col("partition"),
      col("offset"),
      col("timestamp"),
      col("timestampType")
    )

    val parsedData = transformedData.withColumn(
      "value_ext", from_json(col("value"), schema)
    ).select(
      col("value_ext.event_type").alias("event_type"),
      col("value_ext.category").alias("category"),
      col("value_ext.item_id").alias("item_id"),
      col("value_ext.item_price").alias("item_price"),
      col("value_ext.uid").alias("uid"),
      col("value_ext.timestamp").alias("timestamp")
    )

    val logsWithDate = parsedData
      .withColumn("date", date_format(from_unixtime(col("timestamp") / 1000), "yyyMMdd"))
      .withColumn("p_date", date_format(from_unixtime(col("timestamp") / 1000), "yyyMMdd"))

    val viewDF = logsWithDate.filter(col("event_type") === "view")
    val buyDF = logsWithDate.filter(col("event_type") === "buy")

    val viewPath = if (output_dir == "visits") output_dir + "/view" else output_dir
    val buyPath = if (output_dir == "visits") output_dir + "/buy" else output_dir

    viewDF.write
      .partitionBy("p_date")
      .mode("overwrite")
      .json(viewPath)

    buyDF.write
      .partitionBy("p_date")
      .mode("overwrite")
      .json(buyPath)

  }

}