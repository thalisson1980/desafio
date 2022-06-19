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

  def main(args : Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\winutil\\")
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "C:/Users/Thalisson/Desktop/xpandit2/desafio/src/main/resources/csvfiles/").appName("Desafio").master("local[2]").getOrCreate()


    val simpleSchema = StructType(Array(
      StructField("App",StringType,true),
      StructField("Category",StringType,true),
      StructField("Rating",DoubleType,true),
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
    

    val df2 = spark.read.option("header",true).schema(simpleSchema).options(Map("inferSchema"->"true","delimiter"->","))
      .csv("C:/Users/Thalisson/Desktop/xpandit2/desafio/src/main/resources/csvfiles/googleplaystore.csv").orderBy(desc("Reviews"))


    val df3 = df2.withColumn("AppNew",regexp_replace(df2("App"),"'",""))
    df3.createOrReplaceGlobalTempView("apps")
    val df4 = df3.groupBy("AppNew").max("Reviews").collect()


        val apps: ArrayBuffer[String] = new ArrayBuffer[String]
        df4.foreach(app =>{
          apps.append(app.getString(0))

        })
        var newDf =  spark.sql(
          s"""SELECT first(App) as App, collect_set(Category) as Categories,
             | first(Rating) as Rating, max(Reviews) as Reviews,first(Size) as Size,first(Installs) as Installs,first(Type) as Type,first(Price) as Price,
             | first(ContentRating) as Content_Rating,collect_set(Genres) as Genres,first(LastUpdated) as Last_Updated ,first(CurrentVer)as Current_Version,first(AndroidVer) as Minimum_Android_Version
             |  FROM global_temp.apps where AppNew == '${apps.last}' """.stripMargin).toDF()


            apps.foreach(app =>{
          var auxDf = spark.sql(
            s"""SELECT first(App) as App, collect_set(Category) as Categories,
               | first(Rating) as Rating, max(Reviews) as Reviews,first(Size),first(Installs) as Installs,first(Type) as Type,first(Price) as Price,
               | first(ContentRating) as Content_Rating,collect_set(Genres) as Genres,first(LastUpdated) as Last_Updated ,first(CurrentVer)as Current_Version,first(AndroidVer) as Minimum_Android_Version
               |  FROM global_temp.apps where AppNew == '${app}' """.stripMargin).toDF()

              newDf = newDf.union(auxDf)
              newDf.show()
        })

    val df = spark.createDataFrame(spark.sparkContext
      .emptyRDD[Row], simpleSchema)
    newDf.foreach(row =>{

    })


  }


}
