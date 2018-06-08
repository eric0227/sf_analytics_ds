package model

case class GeoPoint(lon:Double, lat:Double) {
  /**
    * in kilometers
    * @param other
    * @return
    */
  def haversineDistance(other: GeoPoint): Double = {
    val deltaLat = math.toRadians(other.lat - lat)
    val deltaLong = math.toRadians(other.lon - lon)
    val x = math.pow(math.sin(deltaLat / 2), 2) + math.cos(math.toRadians(lat)) * math.cos(math.toRadians(other.lat)) * math.pow(math.sin(deltaLong / 2), 2)
    val greatCircleDistance = 2 * math.atan2(math.sqrt(x), math.sqrt(1 - x))
    6371 * greatCircleDistance
  }

  /**
    * orientation by 0 to 360
    * @param other
    * @return
    */
  def orientation(other: GeoPoint) : Int = {
    val dLon = other.lon - lon
    val y = math.sin(dLon) * math.cos(other.lat)
    val x = math.cos(lat) * math.sin(other.lat) - math.sin(lat) * math.cos(other.lat) * Math.cos(dLon)
    val bearing = math.atan2(y, x) * 180 / math.Pi
    if (bearing < 0) bearing.toInt + 360 else bearing.toInt
  }
}

