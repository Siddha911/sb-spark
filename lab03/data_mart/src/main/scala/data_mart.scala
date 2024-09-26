import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.net.{URL, URLDecoder}
import scala.util.Try


object data_mart {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("kirill_sitnikov_lab03")
      .config("spark.cassandra.connection.host", "10.0.0.31")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate()

    val clients: DataFrame = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "clients", "keyspace" -> "labdata"))
      .load()

    val clientsWithAgeCat = clients.withColumn("age_cat",
      when(col("age").between(18, 24), "18-24")
        .when(col("age").between(25, 34), "25-34")
        .when(col("age").between(35, 44), "35-44")
        .when(col("age").between(45, 54), "45-54")
        .otherwise(">=55")
    )

    val visits: DataFrame = spark.read
      .format("org.elasticsearch.spark.sql")
      .options(Map("es.read.metadata" -> "true",
        "es.nodes.wan.only" -> "true",
        "es.port" -> "9200",
        "es.nodes" -> "10.0.0.31",
        "es.net.ssl" -> "false"))
      .load("visits")

    val definedVisits = visits.filter(col("uid").isNotNull)

    val categorizedVisits = definedVisits.groupBy(col("uid"))
      .pivot("category")
      .count

    val visitsColumns = categorizedVisits.columns.head +: categorizedVisits.columns.tail
      .map(_.toLowerCase.replace("-", "_"))
      .map("shop_" + _)

    val renamedCatVisits = categorizedVisits.toDF(visitsColumns: _*)

    val clientsWithAgeCatAndShopCat = clientsWithAgeCat.as("c").join(
      renamedCatVisits.as("v"),
      col("c.uid") === col("v.uid"),
      "left"
    ).drop(col("v.uid"))

    val logs: DataFrame = spark.read
      .json("hdfs:///labs/laba03/weblogs.json")

    val modLogs = logs.withColumn("url",
      explode(col("visits")))
      .select("uid", "url.url")

    def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {
      Try {
        val decodedUrl = URLDecoder.decode(url, "UTF-8")
        val host = new URL(decodedUrl).getHost
        host.replaceFirst("^www\\.", "")
      }.getOrElse("")
    })

    val transformedLogs = modLogs.select(
      col("uid"),
      decodeUrlAndGetDomain(col("url")).alias("domain")
    )

    val transformedAndNotNullLogs = transformedLogs.filter(col("domain") =!= "")

    val cats: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
      .option("dbtable", "domain_cats")
      .option("user", "kirill_sitnikov")
      .option("password", "6gM3K6F9")
      .option("driver", "org.postgresql.Driver")
      .load()

    val domainsAndCats = transformedAndNotNullLogs.as("t").join(
      cats.as("c"),
      col("t.domain") === col("c.domain"),
      "left"
    )

    val uidAndDomainCategory = domainsAndCats.groupBy("uid", "category").count

    val clientsDomainCatsNotGrouped = clientsWithAgeCatAndShopCat.join(uidAndDomainCategory, "uid")

    val clientsDomainCatsPivot = clientsDomainCatsNotGrouped.groupBy("uid").pivot("category").sum("count")

    val clientsDomainCatsPivotNotNull = clientsDomainCatsPivot.drop("null")

    val domainCatsColumns = clientsDomainCatsPivotNotNull.columns.head +: clientsDomainCatsPivotNotNull.columns.tail
      .map("web_" + _)

    val renamedClientsDomainCatsPivotNotNull = clientsDomainCatsPivotNotNull.toDF(domainCatsColumns: _*)

    val clientsShopCatAndWebCat = clientsWithAgeCatAndShopCat.as("c").join(
      renamedClientsDomainCatsPivotNotNull.as("s"),
      "uid"
    ).drop("s.uid")

    clientsShopCatAndWebCat.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/kirill_sitnikov") //как в логине к личному кабинету но _ вместо .
      .option("dbtable", "clients")
      .option("user", "kirill_sitnikov")
      .option("password", "6gM3K6F9")
      .option("driver", "org.postgresql.Driver")
      .option("truncate", value = true) //позволит не терять гранты на таблицу
      .mode("overwrite") //очищает данные в таблице перед записью
      .save()

  }

}
