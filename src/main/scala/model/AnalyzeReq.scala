package model

case class GisInfoReqClientData( speedMh:Int, ts:Long, id:String)

case class AnalyzeTripReq( interval:Int, vechicleType:String, sensorType:String, locations:Seq[TreLocation])

case class AnalyzeTripRspOverLimit( roadId:String, roadName:String, gps:GeoPoint, maxSpeedMh:Int, roadRank:String, clientData:Option[GisInfoReqClientData]) {

  def info : String = s"""{"id":"${roadId}","maxSpd":${maxSpeedMh},"rank":${roadRank}}"""

}
