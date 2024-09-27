import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object filter {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("lab04a")
      .getOrCreate()

    val param1 : String = spark.sparkContext.getConf.get("topic_name")
    val param2 : String = spark.sparkContext.getConf.get("offset")
    val param3 : String = spark.sparkContext.getConf.get("output_dir_prefix")

    val schema = new StructType()
      .add("event_type", StringType)
      .add("category", StringType)
      .add("item_id", StringType)
      .add("item_price", IntegerType)
      .add("uid", StringType)
      .add("timestamp", LongType)

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val offset = "earliest"
    val topic_name = "lab04_input_data"
    val output_dir_prefix = "/user/kirill.sitnikov/visits"



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

    viewDF.write
      .partitionBy("p_date")
      .mode("overwrite")
      .json(output_dir_prefix + "/view")

    buyDF.write
      .partitionBy("p_date")
      .mode("overwrite")
      .json(output_dir_prefix + "/buy")

  }

}