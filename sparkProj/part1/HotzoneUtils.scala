package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
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


  }

  // YOU NEED TO CHANGE THIS PART

}
