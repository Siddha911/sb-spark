import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.apache.spark.ml.{Pipeline, PipelineModel}


object train {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("lab07train")
      .getOrCreate()

    val train_df : String = spark.sparkContext.getConf.get("spark.train.train_df")
    val model_path : String = spark.sparkContext.getConf.get("spark.train.model_path")

    val trainingDf = spark
      .read
      .json(train_df)
      .withColumn("visit", explode(col("visits")))
      .select("uid", "visit", "gender_age")
      .withColumn("host", lower(callUDF("parse_url", col("visit.url"), lit("HOST"))))
      .withColumn("domains", regexp_replace(col("host"), "www.", ""))
      .select("uid", "domains", "gender_age")
      .groupBy("uid", "gender_age")
      .agg(collect_list("domains").as("domains"))

    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")

    val indexerModel = indexer.fit(trainingDf)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(indexerModel.labels)

    val pipeline = new Pipeline()
      .setStages(Array(cv, indexerModel, lr, labelConverter))

    val model = pipeline.fit(trainingDf)

    model.write.overwrite.save(model_path)

  }

}
