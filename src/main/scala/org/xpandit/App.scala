package org.xpandit


import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}
import org.json4s.scalap.scalasig.ClassFileParser.header

import scala.collection.mutable.ArrayBuffer

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\winutil\\")
    val spark = SparkSession.builder().appName("Desafio").master("local[2]").getOrCreate()

    val simpleSchema = StructType(Array(
      StructField("App",StringType,true),
      StructField("Category",StringType,true),
      StructField("Rating",DoubleType,true),
      StructField("Reviews", DoubleType, true),
      StructField("Size", StringType, true),
      StructField("Installs", StringType, true),
      StructField("Type", StringType, true),
      StructField("Price", DoubleType, true),
      StructField("Content Rating", StringType, true),
      StructField("Genres", StringType, true),
      StructField("Last Updated", StringType, true),
      StructField("Current Ver", StringType, true),
      StructField("Android Ver", StringType, true)

    ))

    val df2 = spark.read.option("header",true).schema(simpleSchema).options(Map("inferSchema"->"true","delimiter"->","))
      .csv("C:/Users/Thalisson/Desktop/xpandit2/desafio/src/main/resources/csvfiles/googleplaystore.csv")

    val df3 = df2.filter("rating >= 4 and rating <= 10").orderBy(desc("rating"))


  }

}
