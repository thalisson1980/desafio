package org.xpandit


import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}
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
      StructField("Content Rating", StringType, true),
      StructField("Genres", StringType, true),
      StructField("Last Updated", StringType, true),
      StructField("Current Ver", StringType, true),
      StructField("Android Ver", StringType, true)

    ))

    val df2 = spark.read.option("header",true).schema(simpleSchema).options(Map("inferSchema"->"true","delimiter"->","))
      .csv("C:/Users/Thalisson/Desktop/xpandit2/desafio/src/main/resources/csvfiles/googleplaystore.csv").orderBy(desc("Rating"))


    val df3 = df2.withColumn("AppNew",regexp_replace(df2("App"),"'",""))
    df3.createOrReplaceGlobalTempView("apps")
    val df4 = df3.groupBy("AppNew").max("Rating").collect()



    val apps: ArrayBuffer[String] = new ArrayBuffer[String]
    val apps2: ArrayBuffer[Object] = new ArrayBuffer[Object]
    df4.foreach(app =>{
      apps.append(app.getString(0))

    })
    apps.foreach(app =>{
     /*val auxdf =  df3.filter(s"AppNew == '$app'")
     if( auxdf.count() > 1){
       var greaterRating = 0.0
       val categories: ArrayBuffer[String] = new ArrayBuffer[String]
        auxdf.foreach( aux => {
            if(greaterRating <= aux.getAs[Double](2)){

            }
        })
     }
*/
      apps2.append(spark.sql(s"SELECT first(App),(Select distinct(Category) from global_temp.apps where app == '$app' ) as Categories, max(Rating) as maxRating FROM global_temp.apps where AppNew == '$app' ").collect())
    })

  }


}
