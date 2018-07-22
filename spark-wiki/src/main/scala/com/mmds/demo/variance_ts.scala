package com.mmds.demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{count, sum, variance}


object variance_ts {
  case class pagecount(project: Option[String], article: Option[String]
                       ,requests: Option[Int], hour_vise: Option[String])
  def main(args: Array[String]): Unit = {

    def pwd = System.getProperty("user.dir")
    println(pwd)

    Logger.getRootLogger().setLevel(Level.ERROR)
    val sess = SparkSession.builder()
      .master("local[*]")
      .appName("DatasetConvertor")
      .getOrCreate()

    import sess.implicits._

    val df = sess.read.parquet("./pageviews.parquet")
    df.show()

    val ds = df.groupBy("page_id", "date").agg(sum("sum_requests") as "requests")
    ds.show()

    //val ds_filtr = ds.filter( $"count" > 4)
    //ds_filtr.show()

    val vari = ds.groupBy("page_id").agg(variance("requests") as "variance", count("date") as "count")
    vari.show()

    //.filter("variance > 0.0")
    val result = vari.filter( $"count" === 14).orderBy($"variance".asc)
    result.show()

    val df_page = sess.read.format("csv").option("header", "true").load("./data/page/ukwiki-20180701-page.csv")
    df_page.printSchema()
    df_page.show()

    val result_with_title = result.join(df_page, "page_id")

    val fnl = result_with_title.select("page_id", "title", "variance").orderBy($"variance".asc)
    fnl.show()

    //fnl.write.format("csv").save("./ranking.csv")
    //fnl.write.format("com.databricks.spark.csv").save("./ranking.csv")
    //fnl.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("./ranking.csv")
    fnl.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").csv("./ranking.csv")

    //val test = ds.filter( $"page_id" === 1463593)
    //test.show()

  }
}
