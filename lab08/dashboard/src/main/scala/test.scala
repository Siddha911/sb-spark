import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.ml.{Pipeline, PipelineModel}

import java.net.{URL, URLDecoder}
import scala.util.{Failure, Success, Try}


object test {

  def main(args: Array[String]): Unit = {

    val urlDecoderUDF: UserDefinedFunction = udf((urls: Seq[Row]) => {
      Try {
        urls.map(r => new URL(URLDecoder.decode(r.getAs("url"), "UTF-8")).getHost.stripPrefix("www."))
      } match {
        case Success(urls) => urls
        case Failure(_) => Seq("")
      }
    })

    val spark = SparkSession.builder()
      .appName("lab07test")
      .getOrCreate()

    val test_model_path : String = spark.sparkContext.getConf.get("spark.train.test_model_path")
    val test_df_path : String = spark.sparkContext.getConf.get("spark.train.test_df_path")
    val es_index: String = spark.sparkContext.getConf.get("spark.train.es_index")

    val model = PipelineModel.load(test_model_path)

    val testDF = spark
      .read
      .format("json")
      .load(test_df_path)

    val testParsedDF = testDF
      .withColumn("domains", urlDecoderUDF(col("visits")).cast("array<string>"))
      .drop(col("visits"))

    val predictDf = model.transform(testParsedDF).select("uid", "predictedLabel", "date")

    predictDf
      .write
      .format("org.elasticsearch.spark.sql")
      .mode("overwrite")
      .options(
        Map(
          "es.read.metadata" -> "true",
          "es.nodes.wan.only" -> "true",
          "es.port" -> "9200",
          "es.nodes" -> "10.0.0.31",
          "es.net.ssl" -> "false",
          "es.resource" -> es_index
        )
      )
      .save

  }

}