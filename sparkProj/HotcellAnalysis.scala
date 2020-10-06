package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{


  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  //pickupInfo.show()




  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)

  //pickupInfo.show()



  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  pickupInfo=pickupInfo.where(s"x>= $minX and x<= $maxX and y>= $minY and y<= $maxY and z>= $minZ and z<= $maxZ")

  pickupInfo=pickupInfo.groupBy("x", "y", "z").count()
  pickupInfo.createOrReplaceTempView("hotcell")


  // finding the mean
  val mean: Double =pickupInfo.agg(sum("count")/numCells).first().getDouble(0)


  // finding the std
  val std: Double=math.sqrt(pickupInfo.agg((sum(pow("count",2))/ numCells) - math.pow(mean,2.0)).first().getDouble(0))

  spark.udf.register("ST_Within", (x1:Double, y1:Double, z1:Double, x2:Double, y2:Double, z2:Double)=> HotcellUtils.ST_Within(x1,y1,z1,x2,y2,z2))
  var joindf=spark.sql("select j1.x as x, j1.y as y, j1.z as z, j2.count as wijxj from hotcell as j1, hotcell as j2 where ST_Within(j1.x,j1.y,j1.z,j2.x,j2.y,j2.z)")
  joindf.createOrReplaceTempView("joindf")
  //joindf.show()

  spark.udf.register("countAdjacent", (x:Double,y:Double,z:Double)=>HotcellUtils.countAdjacent(x,y,z,minX,maxX,minY,maxY,minZ,maxZ))
  var joindf2= spark.sql("select x,y,z, countAdjacent(x,y,z) as wij_sum, sum(wijxj) as sum_wijxj from joindf group by x, y ,z  ")
  joindf2.createOrReplaceTempView("joindf2")
  //joindf2.show()

  spark.udf.register("gScore", (wijxj_sum:Double, wij_sum:Double ) => HotcellUtils.gettisord(mean,std,numCells,wijxj_sum,wij_sum))
  val hotspots= spark.sql(" select x,y,z,wij_sum,gScore(sum_wijxj,wij_sum) as score from joindf2 order by gScore(sum_wijxj,wij_sum) desc ")


  //hotspots.show(50)
  return hotspots.select("x", "y","z")


}


}
