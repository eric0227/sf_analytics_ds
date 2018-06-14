package model

import app.udf.SfScoreUDF.TripEventResult
import com.datastax.driver.core.utils.UUIDs
import kafka.TreKafka
import org.joda.time.LocalDateTime
import play.api.libs.json.Json

/**
  * tre_vehicle 테이블 레코드
  * @param id
  * @param companyId
  * @param category
  */
case class TreVehicleRow( id:String, companyId:String, category:Option[String])

/**
  * tre_sensor table
  * @param id
  * @param `type`
  */
case class TreSensorRow( id:String, `type`:Option[String])

/**
  * tre_user table
  * @param id
  * @param name
  * @param companyId
  */
case class TreUserRow( id:String, name:String, companyId:String)

/**
  * tre_company table
  * @param id
  * @param serviceType
  * @param name
  */
case class TreCompanyRow( id:String, serviceType:String, name:String)


/**
  * tre_micro_trip table
  * @param microTripId
  * @param tripId
  * @param sensorId
  * @param vehicleId
  * @param date
  * @param ts
  * @param payload
  * @param deviceType
  */

case class TreMicroTripRow(microTripId:String, tripId:String, sensorId:String, vehicleId:String,
                           date:Option[String], ts:Option[Long], payload:Option[String], deviceType:Option[String],
                           var createdTime:Option[Long] = None)
{
  def toKafkaString( companyId:String) : String = {
    implicit val microTripRowWrites = Json.writes[TreMicroTripRow]
    val json = Json.toJson(this)

    s"""${TreKafka.topicMicrotrip},{
       |"data":${json.toString},
       |"companyId":"$companyId",
       |"sensorId":"$sensorId",
       |"msgType":"microtrip"
       |}
    """.stripMargin.replaceAllLiterally("\n", "").trim()
  }
}

/**
  * tre_gps_event table
  * @param vehicleId
  * @param userId
  * @param tripId
  * @param posLon
  * @param posLat
  * @param eventTs
  * @param eventType
  * @param speedMh
  * @param info
  * @param deviceType
  * @param createdTime
  */
case class TreGpsEventRow( vehicleId:String, userId:String, tripId:String, posLon:Double, posLat:Double,
                           eventTs:Long, eventType:Int, speedMh:Int, info:String, deviceType:String, createdTime:Long, id:String = UUIDs.timeBased.toString) {

  def toPrettyString:String = {
    val dt = new LocalDateTime(eventTs)
    f"${dt.toString("hh:mm:ss")} ${deviceType} ${TreEventEnum(eventType)}%8s   Loc(${posLon}%.4f : ${posLat}%.4f) Speed(${speedMh/1000.0}%.1f) ${info}"
  }
}

/**
  * tre_trip_stat table
  * @param start
  * @param lturn
  * @param rturn
  * @param uturn
  * @param deaccel
  * @param accel
  * @param stop
  * @param lldw
  * @param rldw
  * @param fcw1
  * @param fcw2
  * @param overlimit
  * @param distance
  * @param tripId
  * @param vehicleId
  * @param userId
  * @param date
  * @param deviceType
  */
case class TreTripStatRow( start:Int, lturn:Int, rturn:Int, uturn:Int, deaccel:Int, accel:Int, stop:Int,
                           lldw:Int, rldw:Int, fcw1:Int, fcw2:Int, var overlimit:Int, distance :Int,
                           dtcc:String, cwdrv:String, cwprk:String, bw:String, bbxwn:String,
                           tripId:String, vehicleId:String, userId:String, companyId:String, date:String, deviceType :String) {

  def addStr(a:String, b:String) : String = if ( b == "") a else if ( a == "") b else s"$a,$b"

  def add(other:TreTripStatRow) : TreTripStatRow = {
    TreTripStatRow( this.start + other.start,
      this.lturn + other.lturn,
      this.rturn + other.rturn,
      this.uturn + other.uturn,
      this.deaccel + other.deaccel,
      this.accel + other.accel,
      this.stop + other.stop,
      this.lldw + other.lldw,
      this.rldw + other.rldw,
      this.fcw1 + other.fcw1,
      this.fcw2 + other.fcw2,
      this.overlimit + other.overlimit,
      this.distance + other.distance,
      addStr(dtcc, other.dtcc),
      addStr(cwdrv, other.cwdrv),
      addStr(cwprk, other.cwprk),
      addStr(bw, other.bw),
      addStr(bbxwn, other.bbxwn),
      tripId, vehicleId, userId, companyId, date, deviceType)
  }

  def toPrettyString: String = {
    s"""Device    : ${deviceType}
       |start     : ${start}
       |lturn     : ${lturn}
       |rturn     : ${rturn}
       |uturn     : ${uturn}
       |deaccel   : ${deaccel}
       |accel     : ${accel}
       |stop      : ${stop}
       |lldw      : ${lldw}
       |rldw      : ${rldw}
       |fcw1      : ${fcw1}
       |fcw2      : ${fcw2}
       |overlimit : ${overlimit}
       |distance  : ${distance}
       |dtcc      : ${dtcc}
       |cwdrv     : ${cwdrv}
       |cwprk     : ${cwprk}
       |bw        : ${bw}
       |bbxwn     : ${bbxwn}
     """.stripMargin
  }
}


/**
  * tre_trip table
  * @param tripId
  * @param vehicleId
  * @param userId
  * @param sensorId
  * @param startTs
  * @param startDt
  * @param endTs
  * @param endDt
  * @param deviceType
  * @param createdTime
  * @param payload
  * @param updated
  */

case class TreTripRow(tripId:String, vehicleId:String, userId:String, sensorId:String, var companyId:Option[String],
                      startTs:Option[Long], startDt:Option[String],
                      endTs:Option[Long], endDt:Option[String],
                      deviceType:Option[String], createdTime:Option[Long],
                      payload:Option[String],
                      var updated:Option[Int])
{
  def toKafkaString() : String = {
    implicit val tripRowWrites = Json.writes[TreTripRow]
    val json = Json.toJson(this)

    s"""${TreKafka.topicTrip},{
       |"data":${json.toString},
       |"latestTrip":[{"deviceType":"${deviceType.getOrElse("")}","id":null}],
       |"companyId":"${companyId.getOrElse("")}",
       |"sensorId":"${sensorId}",
       |"msgType":"trip"
       |}
    """.stripMargin.replaceAllLiterally("\n", "").trim()

  }
}


/**
  * tre_event table
  * @param eventId
  * @param vehicleId
  * @param sensorId
  * @param ty
  * @param createdTime
  * @param deviceType
  * @param eventDt
  * @param eventTs
  * @param payload
  * @param tripId
  * @param userId
  */
case class TreEventRow( eventId:String, vehicleId:String, sensorId:String, ty:Int, createdTime:Long,
                        deviceType:String, eventDt:String, eventTs:Long, payload:String, tripId:String, userId:String)
{
  def toPrettyString: String = {
    s"""Device   : ${deviceType}
       |event_dt : ${eventDt}
       |payload  : ${payload}
     """.stripMargin
  }

}

case class TreOverlimitCountRow( vehicleId:String, tripId:String, userId:String, overlimit:Int)

case class TreMoveDF( startPos:GeoPoint, endPos:GeoPoint, ts0:Long, ts1:Long, elapsedSec:Long, distance:Double,
                      speedMh:Int, vehicleType:Option[String], sensorType:Option[String], var event:Option[String] = None)

object TreMoveDF {
  def from( m : TreMove, vehicle:String, sensorType:String) : TreMoveDF = {
    TreMoveDF( GeoPoint(m.loc(0).lon, m.loc(0).lat), GeoPoint(m.loc(1).lon, m.loc(1).lat),
      m.loc(0).ts, m.loc(1).ts, m.milliSec, m.distance, m.speedMh, Some(vehicle), Some(sensorType))
  }
}


case class TripEventResultTuple(check_event: Seq[TreGpsEventRow], check_over_limit: Seq[TreGpsEventRow])


