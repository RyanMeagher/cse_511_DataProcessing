package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {

  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }


  def gettisord( mean:Double, std: Double, numCells: Double, wijxj_sum:Double, wij_sum:Double): Double= {
    (wijxj_sum-mean*wij_sum) / (std * math.sqrt((numCells*wij_sum- math.pow(wij_sum, 2)) /(numCells-1)))
  }

  def countAdjacent(x:Double,y:Double,z:Double, minX:Double, maxX:Double, minY:Double, maxY:Double, minZ:Double, maxZ:Double) : Double= {
    var count=0
    if(x> minX && x<maxX){
      count+=1
    }
    if (y>minY && y<maxY){
      count+=1
    }
    if (z>minZ && z<maxZ){
      count+=1
    }

    val result= count match {
      case 0 => 8
      case 1 => 12
      case 2 => 18
      case _ => 27
    }
    result

  }

  def ST_Within(x1:Double, y1:Double, z1:Double, x2:Double, y2:Double, z2:Double):Boolean={
    math.abs(x1-x2) <= 1 && math.abs(y1-y2)<= 1 && math.abs(z1-z2)<=1

  }




  // YOU NEED TO CHANGE THIS PART
}
