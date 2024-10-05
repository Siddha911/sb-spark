import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object users_items {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("lab05")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.mergeSchema", "true")

    val normalizedItemId = regexp_replace(regexp_replace(lower(col("item_id")), "-", "_"), " ", "_")

    val input_dir: String = spark.sparkContext.getConf.get("spark.users_items.input_dir")
    val update: String = spark.sparkContext.getConf.get("spark.users_items.update", "1")
    val output_dir: String = spark.sparkContext.getConf.get("spark.users_items.output_dir")

    if (update == "0") {
      val usersBuys = spark.read.json(input_dir + "/buy")
      val usersViews = spark.read.json(input_dir + "/view")

      val allUsersEvents = usersBuys.union(usersViews)
      val notNullUsersEvents = allUsersEvents.filter(col("uid").isNotNull)

      val stg_output_dir_subdir = notNullUsersEvents.select(max("p_date").cast("string"))
      val output_dir_subdir = stg_output_dir_subdir.collect()(0).getString(0)

      val allUsersBuys = notNullUsersEvents.filter(
          col("event_type") === "buy")
        .groupBy(
          col("uid"), normalizedItemId.alias("buy_item_id")
        ).agg(
          count("*").alias("buy_count")
        )

      val allUsersViews = notNullUsersEvents.filter(
          col("event_type") === "view")
        .groupBy(
          col("uid"), normalizedItemId.alias("view_item_id")
        ).agg(
          count("*").alias("view_count")
        )

//      val userItemMatrix = allUsersBuys.join(
//        allUsersViews,
//        Seq("uid"),
//        "outer"
//      ).na.fill(0)

      val pivotBuy = allUsersBuys
        .groupBy("uid")
        .pivot("buy_item_id")
        .sum("buy_count")
        .na.fill(0)
        .drop("null")

      val pivotView = allUsersViews
        .groupBy("uid")
        .pivot("view_item_id")
        .sum("view_count")
        .na.fill(0)
        .drop("null")

      val newViewColumns = pivotView.columns.map { colName =>
        if (colName == "uid") colName else s"view_$colName"
      }
      val newBuyColumns = pivotBuy.columns.map { colName =>
        if (colName == "uid") colName else s"buy_$colName"
      }

      val renamedPivotView = pivotView.toDF(newViewColumns: _*)
      val renamedPivotBuy = pivotBuy.toDF(newBuyColumns: _*)

      val finalMatrix = renamedPivotView.join(
        renamedPivotBuy,
        Seq("uid"),
        "outer"
      ).na.fill(0)

       finalMatrix.write
           .mode("overwrite")
           .parquet(output_dir + s"/$output_dir_subdir")

//      renamedPivotView.write
//        .option("mergeSchema", "true")
//        .mode("append")
//        .parquet(output_dir + s"/$output_dir_subdir")
//
//      renamedPivotBuy.write
//        .option("mergeSchema", "true")
//        .mode("append")
//        .parquet(output_dir + s"/$output_dir_subdir")

    } else if (update == "1") {
      val usersBuys = spark.read.json(input_dir + "/buy")
      val usersViews = spark.read.json(input_dir + "/view")

      val allUsersEvents = usersBuys.union(usersViews)
      val notNullUsersEvents = allUsersEvents.filter(col("uid").isNotNull)

      val stg_output_dir_subdir = allUsersEvents.select(max("p_date").cast("string"))
      val output_dir_subdir = stg_output_dir_subdir.collect()(0).getString(0)

      val allUsersBuys = notNullUsersEvents.filter(
          col("event_type") === "buy")
        .groupBy(
          col("uid"), normalizedItemId.alias("buy_item_id")
        ).agg(
          count("*").alias("buy_count")
        )

      val allUsersViews = notNullUsersEvents.filter(
          col("event_type") === "view")
        .groupBy(
          col("uid"), normalizedItemId.alias("view_item_id")
        ).agg(
          count("*").alias("view_count")
        )

//      val userItemMatrix = allUsersBuys.join(
//        allUsersViews,
//        Seq("uid"),
//        "outer"
//      ).na.fill(0)

      val pivotBuy = allUsersBuys
        .groupBy("uid")
        .pivot("buy_item_id")
        .sum("buy_count")
        .na.fill(0)
        .drop("null")

      val pivotView = allUsersViews
        .groupBy("uid")
        .pivot("view_item_id")
        .sum("view_count")
        .na.fill(0)
        .drop("null")

      val newViewColumns = pivotView.columns.map { colName =>
        if (colName == "uid") colName else s"view_$colName"
      }
      val newBuyColumns = pivotBuy.columns.map { colName =>
        if (colName == "uid") colName else s"buy_$colName"
      }

      val renamedPivotView = pivotView.toDF(newViewColumns: _*)
      val renamedPivotBuy = pivotBuy.toDF(newBuyColumns: _*)

      val finalMatrix = renamedPivotView.join(
        renamedPivotBuy,
        Seq("uid"),
        "outer"
      ).na.fill(0)

      val lagMatrix = spark.read.option("mergeSchema", "true").parquet("/user/kirill.sitnikov/users-items/20200429")

      lagMatrix.write
        .option("mergeSchema", "true")
        .mode("overwrite")
        .parquet(output_dir + s"/$output_dir_subdir")

      finalMatrix.write
        .option("mergeSchema", "true")
        .mode("overwrite")
        .parquet(output_dir + s"/$output_dir_subdir")

    }

  }

}
