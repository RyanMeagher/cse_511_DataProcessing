package cse512

import org.apache.spark.sql.SparkSession
import scala.math
import scala.math

object SpatialQuery extends App{

  println("Starting SpatialQuery ojbect")

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    //println("Running runRangeQuery method")

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")


    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String) => {
      //from the defined rectangle find all the edges
      var rectangle = queryRectangle.split(",");
      var lon_1= rectangle(0).toDouble
      var lat_1= rectangle(1).toDouble
      var lon_2=rectangle(2).toDouble
      var lat_2= rectangle(3).toDouble

      // given a point from a dataset find the lon and lat
      var point = pointString.split(",");
      var lon_point=point(0).toDouble
      var lat_point=point(1).toDouble

      //determine which rectangle coordinates are max and min
      var max_lon=math.max(lon_1,lon_2)
      var min_lon=math.min(lon_1,lon_2)
      var max_lat=math.max(lat_1,lat_2)
      var min_lat=math.min(lat_1,lat_2)

      if (lon_point>max_lon || lon_point < min_lon || lat_point>max_lat || lat_point<min_lat) {
          false; }
      else {
          true;}


    })

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()


    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {
    println("Made it here1 ")


    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>{
      //from the defined rectangle find all the edges
      var rectangle = queryRectangle.split(",");
      var lon_1= rectangle(0).toDouble
      var lat_1= rectangle(1).toDouble
      var lon_2=rectangle(2).toDouble
      var lat_2= rectangle(3).toDouble

      // given a point from a dataset find the lon and lat
      var point = pointString.split(",");
      var lon_point=point(0).toDouble
      var lat_point=point(1).toDouble

      //determine which rectangle coordinates are max and min
      var max_lon=math.max(lon_1,lon_2)
      var min_lon=math.min(lon_1,lon_2)
      var max_lat=math.max(lat_1,lat_2)
      var min_lat=math.min(lat_1,lat_2)

      // compare the points lat and longitude to the min and max lon and lat
      if (lon_point>max_lon || lon_point < min_lon || lat_point>max_lat || lat_point<min_lat) {
        false; }
      else {
        true;}

    })

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()


    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>{
      val p1=pointString1.split(",")
      val p2=pointString2.split(",")

      val p1_lon=p1(0).toDouble
      val p1_lat=p1(1).toDouble
      val p2_lon=p2(0).toDouble
      val p2_lat=p2(1).toDouble


      var euc_dist=Math.sqrt( Math.pow((p1_lon-p2_lon),2) + Math.pow( (p1_lat-p2_lat),2))

      if (euc_dist<=distance){
        true;
      }
      else{
        false;
      }


    })

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()


    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>{
      val p1=pointString1.split(",")
      val p2=pointString2.split(",")

      val p1_lon=p1(0).toDouble
      val p1_lat=p1(1).toDouble
      val p2_lon=p2(0).toDouble
      val p2_lat=p2(1).toDouble

      var euc_dist=Math.sqrt( Math.pow((p1_lon-p2_lon),2) + Math.pow( (p1_lat-p2_lat),2))

      if (euc_dist<=distance){
        true;
      }
      else{
        false;
      }


    })
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  //val spark = SparkSession
  //  .builder()
  //  .appName("CSE512-Project-Phase2-Template-master")
  //  .config("spark.some.config.option", "some-value").master("local[*]")
  //  .getOrCreate()


  //val count=runRangeQuery(spark,"src/resources/arealm10000.csv", "-93.63173,33.0183,-93.359203,33.219456")
  //  println(count)
  //val count1= runRangeJoinQuery(spark,"src/resources/arealm10000.csv","src/resources/zcta10000.csv")
  //  println(count1)
  //val count2=runDistanceQuery(spark, "src/resources/arealm10000.csv","-88.331492,32.324142", "1 ")
  //  println(count2)
  //val count3= runDistanceJoinQuery(spark,"src/resources/arealm10000.csv","src/resources/arealm10000.csv", "0.1" )
  //  println(count3)


}

