package org.xpandit


import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, IntegerType, StringType, StructField, StructType}
import org.json4s.scalap.scalasig.ClassFileParser.header

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions.regexp_replace

/**
 * @author ${user.name}
 */
object App {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\winutil\\")
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "C:/Users/Thalisson/Desktop/xpandit2/desafio/src/main/resources/csvfiles/").appName("Desafio").master("local[2]").getOrCreate()

    val simpleSchema2 = StructType(Array(
      StructField("App2", StringType, true),
      StructField("Translated_Review", StringType, true),
      StructField("Sentiment", StringType, true),
      StructField("Sentiment_Polarity", DoubleType, true),
      StructField("Sentiment_Subjectivity", FloatType, true)
    ))


    val simpleSchema = StructType(Array(
      StructField("App", StringType, true),
      StructField("Category", StringType, true),
      StructField("Rating", DoubleType, true),
      StructField("Reviews", DoubleType, true),
      StructField("Size", StringType, true),
      StructField("Installs", StringType, true),
      StructField("Type", StringType, true),
      StructField("Price", DoubleType, true),
      StructField("ContentRating", StringType, true),
      StructField("Genres", StringType, true),
      StructField("LastUpdated", StringType, true),
      StructField("CurrentVer", StringType, true),
      StructField("AndroidVer", StringType, true)

    ))

    var df9 = spark.read.schema(simpleSchema2).options(Map("inferSchema" -> "true", "delimiter" -> ","))
      .csv("C:/Users/Thalisson/Desktop/xpandit2/desafio/src/main/resources/csvfiles/googleplaystore_user_reviews.csv")

    val df13 = df9.na.fill(0).groupBy("App2").avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity")
    val df10 = df13.withColumn("App2", regexp_replace(df13("App2"), "'", ""))


    val df2 = spark.read.option("header", true).schema(simpleSchema).options(Map("inferSchema" -> "true", "delimiter" -> ","))
      .csv("C:/Users/Thalisson/Desktop/xpandit2/desafio/src/main/resources/csvfiles/googleplaystore.csv").orderBy(desc("Reviews"))
    val df3 = df2.withColumn("App", regexp_replace(df2("App"), "'", ""))
    df3.createOrReplaceGlobalTempView("apps")

    val df4 = df3.groupBy("App").max("Reviews").collect()


    val apps: ArrayBuffer[String] = new ArrayBuffer[String]
    df4.foreach(app => {
      apps.append(app.getString(0))

    })
    import org.apache.spark.sql.functions._
    var newDf = spark.sql(
      s"""SELECT first(App) as App, collect_set(Category) as Values,
         | first(Rating), max(Reviews),first(Size),first(Installs),first(Type),first(Price),
         | first(ContentRating),first(Genres),first(LastUpdated),first(CurrentVer),first(AndroidVer)
         |  FROM global_temp.apps where App == '${apps.last}' """.stripMargin).toDF()


    apps.foreach(app => {
      var auxDf = spark.sql(
        s"""SELECT first(App) as App, collect_set(Category) as Values,
           | first(Rating), max(Reviews),first(Size),first(Installs),first(Type),first(Price),
           | first(ContentRating),first(Genres),first(LastUpdated),first(CurrentVer),first(AndroidVer)
           |  FROM global_temp.apps where App == '$app' """.stripMargin).toDF()

      newDf = newDf.union(auxDf)

       df9 = newDf.join(df10, newDf("App") === df10("App2"), "left_outer").toDF()
      df9.show()

    })
    df9.drop("App2").show()
    df9.write.option("compression","gzip").parquet("C:/Users/Thalisson/Desktop/xpandit2/desafio/src/main/resources/csvfiles/googleplaystore_user_reviews.csv")

  }


}
