package com.mmds.demo

import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{lit, sum}

import scala.collection.mutable


object pageviewRequest_0 {
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

    val df_lang = sess.read.format("csv").option("header", "true").load("./data/langlinks/ukwiki-20180701-langlinks.csv")

    val test = df_lang.filter( $"id" === 1273710)
    test.show()

    val dl = df_lang.filter($"lang" === "en")
    dl.printSchema()
    dl.show()

    val df_page = sess.read.format("csv").option("header", "true").load("./data/page/ukwiki-20180701-page.csv")
    df_page.printSchema()
    df_page.show()


    // todo broadcast
    val dl_set = dl.collect.toSet

    //def dayIterator(start: LocalDate, end: LocalDate) = Iterator.iterate(start)(_ plusDays 1) takeWhile (_ isBefore end)
    //dayIterator(new LocalDate("2018-06-01"), new LocalDate("2018-06-02")).foreach(println)

    def genDateRange (startDate: Date, endDate: Date): List[Date] = {
      var dt = startDate
      val res: mutable.MutableList[Date] = new mutable.MutableList[Date]()
      val c = Calendar.getInstance()
      while ( dt.before(endDate) || dt.equals(endDate)) {
        res += dt
        c.setTime(dt)
        c.add(Calendar. DATE, 1)
        dt = c.getTime
      }
      res.toList
    }

    val format = new java.text.SimpleDateFormat("yyyyMMdd")
    val startDate = format.parse("20180601")
    val endDate = format.parse("20180614")

    val dateRange = genDateRange(startDate, endDate)
    for (date <- dateRange) {
      val date_Str = format.format(date)
      println(date_Str)
      val rawData = sess.read
        .option("delimiter", " ")
        .option("inferSchema", true)
        .csv("./data/pageviews/pageviews-" + date_Str + "*").cache

      val cols = Seq("project", "title", "requests", "hour_vise")
      val rawDF = rawData.toDF(cols:_*)
      val rawDF_uk = rawDF.filter($"project" === "uk")
      val rawDF_uk_with_id = rawDF_uk.join(df_page, "title")
      rawDF_uk_with_id.printSchema()
      rawDF_uk_with_id.show()


      val rawDF_uk_notEn = rawDF_uk_with_id.join(dl, rawDF_uk_with_id("page_id") === dl("id"), "leftanti")
      //val ds = rawDF_uk_notEn.withColumn("z", lit("red")).as[pagecount]

      //val ds = rawDF_uk_notEn.groupBy($"page_id").agg(sum($"request")).toDF("page_id", "request")
      val ds = rawDF_uk_notEn.groupBy("page_id").agg(sum("requests") as "sum_requests") //.toDF("page_id", "requests")

      println(s"Num partitions : ${ds.rdd.getNumPartitions}")
      ds.printSchema()
      ds.show()
      val enDS = ds.select("page_id", "sum_requests").withColumn("date", lit(date_Str))

      enDS.write
        .format("parquet")
        .mode(SaveMode.Append)
        .save("./pageviews.parquet")

    }
  }
}
