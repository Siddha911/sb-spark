import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction


object features {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("lab06")
      .getOrCreate()

    import spark.implicits._
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val rawWebLogs = spark
      .read
      .json("/labs/laba03/weblogs.json")
      .withColumn("visit", explode(col("visits")))
      .select("uid", "visit")
      .withColumn("date", (col("visit.timestamp") / 1000).cast("timestamp"))

    val usersByWeekdays = rawWebLogs
      .withColumn("weekday", concat(lit("web_day_"), lower(date_format(col("date"), "E"))))
      .groupBy(col("uid"))
      .pivot("weekday")
      .count

    val usersByHours = rawWebLogs
      .withColumn("hour", concat(lit("web_hour_"), lower(date_format(col("date"), "H"))))
      .groupBy(col("uid"))
      .pivot("hour")
      .count

    val totalVisitsByUser = rawWebLogs
      .groupBy(col("uid"))
      .count

    val userByPeriod = rawWebLogs
      .withColumn("period", lower(date_format(col("date"), "H")).cast(IntegerType))
      .withColumn("period", (when(col("period") < 18 && col("period") >= 9, "web_fraction_work_hours")
        .when(col("period") < 24 && col("period") >= 18, "web_fractiob_evening_hours")
        .otherwise(null)))
      .na.drop()
      .groupBy("uid")
      .pivot("period")
      .count

    val userByPeriodRate = totalVisitsByUser
      .join(userByPeriod, "uid")
      .withColumn("web_fraction_work_hours", col("web_fraction_work_hours") / col("count"))
      .withColumn("web_fractiob_evening_hours", col("web_fractiob_evening_hours") / col("count"))
      .select(userByPeriod("uid"), col("web_fraction_work_hours"), col("web_fractiob_evening_hours"))

    val newWebLogs = rawWebLogs
      .withColumn("host", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))

    val topDomains = newWebLogs
      .groupBy(col("domain"))
      .count
      .na.drop()
      .sort(col("count").desc)
      .limit(1000)
      .select("domain")
      .as[String]
      .collect
      .toArray
      .sorted

    val fltrNewWebLogs = newWebLogs
      .filter(col("domain").isin(topDomains: _*))
      // .withColumn("domain", regexp_replace($"domain", "\\.", "_"))
      .groupBy("uid")
      .pivot("domain", topDomains)
      .count
      .na.fill(0)

    val domainFeatWebLogs = fltrNewWebLogs.withColumn(
      "domain_features",
      array(fltrNewWebLogs.columns.filter(_ != "uid")
        .map(colName => col(s"`$colName`")): _*)
    ).select(
      "uid",
      "domain_features"
    )

    val usersItems = spark
      .read
      .parquet("/user/kirill.sitnikov/users-items/20200429")

    val resultDf = domainFeatWebLogs
      .join(usersByWeekdays, "uid")
      .join(usersByHours, "uid")
      .join(userByPeriodRate, "uid")
      .join(usersItems, "uid")

    resultDf.write.mode("overwrite").parquet("/user/kirill.sitnikov/features")

  }

}
