import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.apache.spark.ml.{Pipeline, PipelineModel}


object test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("lab07test")
      .getOrCreate()

    val test_model_path : String = spark.sparkContext.getConf.get("spark.train.test_model_path")

    val modelInfer = PipelineModel.load(test_model_path)

    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> "kirill_sitnikov",
      "startingOffsets" -> "earliest",
      "maxOffsetsPerTrigger" -> "1000"
    )

    val schema = StructType(List(
      StructField("uid", StringType, true),
      StructField("visits", ArrayType(StructType(List(
        StructField("url", StringType, true),
        StructField("timestamp", LongType, true)
      ))), true)
    ))

    val sdf = spark
      .readStream
      .format("kafka")
      .options(kafkaParams)
      .load

    val sdfTransformed = sdf
      .select(from_json(col("value").cast(StringType), schema).as("data"))
      .withColumn("visit", explode(col("data.visits")))
      .select(col("data.uid").as("uid"), col("visit.url").as("url"))
      .withColumn("host", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("domains", regexp_replace(col("host"), "www.", ""))
      .select("uid", "domains")
      .groupBy("uid")
      .agg(collect_list("domains").as("domains"))

    val predictDf = modelInfer.transform(sdfTransformed)

    val aggJsonSdf = predictDf
      .select(
        to_json(struct(
          col("uid").cast(StringType).as("uid"),
          col("predictedLabel").cast(StringType).as("gender_age")
        )).as("value")
    )

    val resSdf = aggJsonSdf
      .writeStream
      .format("kafka")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("topic", "kirill_sitnikov_lab07_out")
      .option("checkpointLocation", "/tmp/ks_lab07/checkpoints")
      .start

    resSdf.awaitTermination

  }

}
