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
      .csv("C:/Users/Thalisson/Desktop/xpandit2/desafio/src/main/resources/csvfiles/googleplaystore.csv")

    //df2.createOrReplaceGlobalTempView("apps")

    val df3 = df2.groupBy("App").max("Rating").collect()

    val apps: ArrayBuffer[String] = new ArrayBuffer[String]
    df3.foreach(app =>{
      apps.append(app.getString(0))

    })

    apps.foreach(app =>{
      df2.filter("App" == app)
    })

 /*   val apps: ArrayBuffer[app] = new ArrayBuffer[app]

    df2.foreach(f => {
      var appExists = false
      var ratingValue = 0.0
      var category = ""
     if(apps.size > 0) {
      apps.foreach(app =>{
        if(app.app == f.get(0)){
          appExists = true
          ratingValue = f.getAs[Double](2)
          category = f.getAs[String](1)
        }
        if(appExists){
          var categoryExists = false
          if(app.rating < ratingValue){
            app.rating = ratingValue
          }
          app.categories.foreach( auxCategory =>{
            if(auxCategory == category){
              categoryExists = true
            }
          })
          if(!categoryExists){
            app.categories.append(category)
          }
          println(app.categories)
        }
        if(!appExists){
          var nova = new app()
          nova.app = f.getAs(0)
          nova.categories.append(f.getAs(1))
          nova.rating = f.getAs(2)
          nova.reviews = f.getAs(3)
          nova.Size = f.getAs(4)
          nova.Installs = f.getAs(5)
          nova.Type = f.getAs(6)
          nova.Price = f.getAs(7)
          nova.Content_Rating = f.getAs(8)
          nova.Genres = f.getAs(9)
          nova.Last_Updated = f.getAs(10)
          nova.Current_Ver = f.getAs(11)
          nova.Android_Ver = f.getAs(12)
          apps.append(nova)
        }

      })
     }else{
       var nova = new app()
       nova.app = f.getAs(0)
       nova.categories.append(f.getAs(1))
       nova.rating = f.getAs(2)
       nova.reviews = f.getAs(3)
       nova.Size = f.getAs(4)
       nova.Installs = f.getAs(5)
       nova.Type = f.getAs(6)
       nova.Price = f.getAs(7)
       nova.Content_Rating = f.getAs(8)
       nova.Genres = f.getAs(9)
       nova.Last_Updated = f.getAs(10)
       nova.Current_Ver = f.getAs(11)
       nova.Android_Ver = f.getAs(12)
       apps.append(nova)
     }

    })
*/
  }

  class app  {

    var app = ""
    var categories: ArrayBuffer[String] = new ArrayBuffer[String]
    var rating = 0.0
    var reviews = 0.0
    var Size = ""
    var Installs = ""
    var Type = ""
    var Price = 0.0
    var Content_Rating = ""
    var Genres = ""
    var Last_Updated = ""
    var Current_Ver = ""
    var Android_Ver = ""

  }

}
