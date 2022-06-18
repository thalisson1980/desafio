package org.xpandit

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\winutil\\")
    val spark = SparkSession.builder().appName("Desafio").master("local[2]").getOrCreate()

    val simpleSchema = StructType(Array(
      StructField("App",StringType,true),
      StructField("Translated_Review",StringType,true),
      StructField("Sentiment",StringType,true),
      StructField("Sentiment_Polarity", DoubleType, true),
      StructField("Sentiment_Subjectivity", FloatType, true)
    ))
    val df2 = spark.read.schema(simpleSchema).options(Map("inferSchema"->"true","delimiter"->","))
      .csv("C:/Users/Thalisson/Desktop/xpandit2/desafio/src/main/resources/csvfiles/googleplaystore_user_reviews.csv")
    
    df2.na.fill(0).groupBy("app").avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity").show(false)

  }

}
