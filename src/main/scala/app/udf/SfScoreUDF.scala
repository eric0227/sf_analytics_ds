package app.udf

import java.util.UUID

import com.datastax.driver.core.utils.UUIDs
import com.typesafe.scalalogging.LazyLogging
import common.TreanaConfig
import common.TreanaConfig._
import kafka.DeviceTripId
import model._
import org.apache.http.client.HttpResponseException
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}
import org.joda.time.{LocalDate, LocalDateTime}
import play.api.libs.json.{JsArray, JsObject, JsSuccess, Json}

object SfScoreUDF extends LazyLogging {

  val speedCheckInterval = TreanaConfig.config.getOrElse[Int]("treana.speed.interval", 5) // 5 seconds
  val ksLinkUrl = TreanaConfig.config.getOrElse[String]("treana.speed.kslink.url", "")
  val ksLinkEnabled = TreanaConfig.config.getOrElse[Boolean]("treana.speed.kslink.enabled", false)
  val ksLinkTimeout = TreanaConfig.config.getOrElse[Int]("treana.speed.kslink.timeout", 120)
  val batchSize = TreanaConfig.config.getOrElse[Int]("treana.dse.batchSize", 10)
  val maxSparkIdx = TreanaConfig.config.getOrElse[Int]("treana.dse.maxSparkIdx", 60)
  
  val gpsEventType = Map[String,Int](
    "start" -> 1,
    "accel" -> 2,
    "deaccel" -> 3,
    "stop" -> 4,
    "overlimit" -> 5
  )

  object TripMetrics extends Enumeration {
    val gisCheckOverlimitReq
    ,gisCheckOverlimitRsp
    ,gisCheckOverlimitFail = Value
  }

  case class TripEventResult( id:String, ts:Long, pos:GeoPoint, speedMh:Int, event:Int, info:String, deviceType:String) {
    def toTreGpsEventRow( vehicleId:String, userId:String) : TreGpsEventRow = {
      TreGpsEventRow( vehicleId, userId, id, pos.lon, pos.lat, ts, event, speedMh, info, deviceType, 0L)
    }
  }


  def toGpsList( gpsList:Seq[Row]) : Seq[TreLocation] = {
    gpsList.map{
      case Row( devTime, ts:Long, lon:Double, lat:Double, id:String) =>
        TreLocation( if (devTime == null) ts else devTime.asInstanceOf[Long], lon, lat, id)
      case _ => TreLocation( 0, 0, 0)
    }.filter( _.ts > 0).sortWith( _.ts < _.ts)
  }

  /**
    * GPS location 정보에서 이벤트를 추출한다. (급가속, 급감속 등)
    * @param gps
    * @param sensorType
    * @return
    */
  def checkEvent( gps:Seq[TreLocation], sensorType:String, verbose:Boolean = false ) : Seq[TripEventResult] = {

    sensorType match {
      case "GPS" | "BLACKBOX" =>
        // GPS 좌표를 Move()로 변환
        val move = TreMove.from(gps, speedCheckInterval)

        if ( verbose) {
          if (move.isEmpty) logger.info("No GPS events")
          else {
            move.get.foreach( r => logger.info( r.toPrettyString))
          }
        }

        // check location
        TripEvent.updateTrafficEvent( move, None, Some(sensorType), verbose)
          .filter( _.event.isDefined)
          .map{ t =>
            TripEventResult( t.loc.last.id, t.loc.last.ts, GeoPoint(t.loc.last.lon, t.loc.last.lat), t.speedMh,
              t.event.map( x => gpsEventType.getOrElse(x, 0)).getOrElse(0), "", sensorType)}

      case _ => Seq.empty[TripEventResult]
    }
  }

  val mtrip_and_event_stat = udf((trip_id: String, vehicle_id: String, user_id: String, company_id: String, device_type:String, device_dist: Int, eventList: Seq[Row], eventPayloadList: Seq[String]) => {
    def checkNull(a:Any) : Int = if (a == null) 0 else a.asInstanceOf[Int]

    val stat = eventList.map {
      case Row(sp, em, ldw, fcw) => TripStat(checkNull(sp), checkNull(em), checkNull(ldw), checkNull(fcw))
      case _ => TripStat(0,0,0,0)
    }

    val dt = new LocalDate(UUIDs.unixTimestamp(UUID.fromString(trip_id)))

    //stat.reduce( _.add(_)).stat

    val ts = stat.reduce( _.add(_))   // 통계 합산
    ts.add(TreEventEnum.distance.id, device_dist) // distance 값 추가
    val tripStatRow: TreTripStatRow = ts.toTreTripStatRow(trip_id, vehicle_id, user_id, company_id, dt.toString, device_type)

    val x = eventPayloadList.map(payload => parseEventPayload(trip_id, payload)).flatten

    // 이벤트 통계 합산
    if ( x.nonEmpty)
      x.reduce(_.add(_))
        .toTreTripStatRow(tripStatRow.tripId, tripStatRow.vehicleId, tripStatRow.userId, tripStatRow.companyId, tripStatRow.date, tripStatRow.deviceType)
        .add(tripStatRow)
    else tripStatRow
  })

/*
  val addEventStat = udf((trip_stat_row: TreTripStatRow, event_payload: Seq[String]) => {
    val trip_id = trip_stat_row.tripId
    val x = event_payload.map(payload => parseEventPayload(trip_id, payload)).flatten

    // 이벤트 통계 합산
    if ( x.nonEmpty)
      x.reduce(_.add(_))
        .toTreTripStatRow(trip_stat_row.tripId, trip_stat_row.vehicleId, trip_stat_row.userId, trip_stat_row.companyId, trip_stat_row.date, trip_stat_row.deviceType)
        .add(trip_stat_row)
    else trip_stat_row
  })
*/

  val trip_distance = udf((gpsList:Seq[Row]) => {
    // devTime이 있으면 그 값을 사용한다. ts는 DB write 시각이 될 수 있으며, 이때는 시간 간격이 달라지게 된다.
    // 예를 들어 1초 간격으로 데이터가 오지 않고, 한꺼번에 몰릴 수 있다.
    val gps = toGpsList(gpsList)

    // sum distance
    TreMove.from(gps, 1).map( _.map(_.distance).sum).getOrElse(0)
  })


  val event = udf((vehicle_id: String, user_id: String, gpsList: Seq[Row], deviceType:String) => {
    // GPS 정보 변환
    val gps = toGpsList(gpsList)
    val events = checkEvent( gps, deviceType).map(_.toTreGpsEventRow(vehicle_id, user_id))

    // 과속 여부 확인
    val overLimits = checkOverLimit(gps, deviceType).map(_.toTreGpsEventRow(vehicle_id, user_id))
    TripEventResultTuple(events, overLimits)
  })

  val lon_lat_string = udf((lon: Long, lat: Long) => {
    if(lon != null && lat != null)  s"{lon:${lon},lat:${lat}}" else null
  })

  val base_device_type = udf((latestTrip: Seq[Row]) => {
    val list = latestTrip.map {
      case Row(deviceType: String, id: String) => DeviceTripId(deviceType, Some(id))
      case Row(deviceType: String) => DeviceTripId(deviceType, None)
    }

    val deviceType = priorityDeviceTrip(list)
    deviceType
  })

  def checkOverLimit( gps:Seq[TreLocation], deviceType:String) : Seq[TripEventResult] = {

    implicit val treLocationWrites = Json.writes[TreLocation]
    implicit val analyzeTripReqWrites = Json.writes[AnalyzeTripReq]
    implicit val geoPointReads = Json.reads[GeoPoint]
    implicit val gisInfoReqClientData = Json.reads[GisInfoReqClientData]
    implicit val analyzeTripRspOverLimitReads = Json.reads[AnalyzeTripRspOverLimit]

    val dt = LocalDateTime.now
    if ( ksLinkEnabled && gps.nonEmpty) {
      try {
        val json = Json.toJson( AnalyzeTripReq( speedCheckInterval, "", "", gps))
        val client = new org.apache.http.impl.client.DefaultHttpClient()
        val request = new org.apache.http.client.methods.HttpPost( ksLinkUrl)
        val entity = new org.apache.http.entity.StringEntity(json.toString)
        request.setEntity(entity)
        request.setHeader("Accept", "application/json")
        request.setHeader("Content-type", "application/json; charset=UTF-8")

        import org.apache.http.params.HttpConnectionParams
        import org.apache.http.util.EntityUtils

        val timeout = ksLinkTimeout
        // seconds
        val httpParams = client.getParams
        HttpConnectionParams.setConnectionTimeout(httpParams, timeout * 1000) // http.connection.timeout
        HttpConnectionParams.setSoTimeout(httpParams, timeout * 1000) // http.socket.timeout

        logger.debug(s"$dt Check overlimit ${ksLinkUrl}")
        // println(json.toString)
        // println(s"$dt Check overlimit ${ksLinkUrl}")

        // GIS 프로세스 연동해서 map matching 처리
        val response = client.execute(request)
        val statusLine = response.getStatusLine
        val resp = response.getEntity
        if (statusLine.getStatusCode >= 300) {
          EntityUtils.consume(resp)
          throw new HttpResponseException(statusLine.getStatusCode, statusLine.getReasonPhrase)
        }
        else {

          val body = EntityUtils.toString(resp, "UTF-8")
          val overlimitEventCd = gpsEventType.getOrElse("overlimit", 5)

          Json.parse(body).validate[Seq[AnalyzeTripRspOverLimit]] match {
            case s: JsSuccess[Seq[AnalyzeTripRspOverLimit]] =>

              s.get.filter( _.clientData.isDefined).map { a =>
                // logger.info(a)
                println(a)
                TripEventResult( a.clientData.get.id, a.clientData.get.ts, a.gps, a.clientData.get.speedMh,
                  overlimitEventCd, a.info, deviceType)
              }
            case _ => logger.warn(s"$dt Failed to parse '$body'")
              Seq.empty[TripEventResult]
          }
        }
      } catch {
        case e: Exception =>
          logger.error(s"$dt Failed to analyze $e")
          Seq.empty[TripEventResult]
      }
    }
    else {
      logger.info( s"$dt ${ksLinkEnabled} size=${gps.size}")
      Seq.empty[TripEventResult]
    }
  }

  // 201805 : Aggregated Data 처리
  def parseEventPayload( tripId:String, payload:String ): Seq[TripStat] = {
    val json = Json.parse(payload)

    json match {
      case a : JsArray =>
        a.value.filter{
          case a: JsObject => true
          case _ => false
        }.map(_.asInstanceOf[JsObject]).map(o => objToPayloadTuple(tripId, o))
      case o: JsObject =>
        Seq(objToPayloadTuple(tripId, o))
      case _ =>
        logger.warn("Failed to parse payload")
        Seq.empty[TripStat]
    }
  }

  // 201805 : Aggregated Data 처리
  def objToPayloadTuple( tripId:String, o:JsObject ): TripStat = {
    val dtcc = (o \ "dtcc").asOpt[String]
    val wbv = (o \ "wbv").asOpt[Int]
    val wid = (o \ "wid").asOpt[Int]
    val ldw = (o \ "ldw").asOpt[Int]
    val fcw = (o \ "fcw").asOpt[Int]


    val dclon = (o \ "dclon").asOpt[Double]
    val dclat = (o \ "dclat").asOpt[Double]
    val plat = (o \ "plat").asOpt[Double]
    val plon = (o \ "plon").asOpt[Double]
    val cwdrv = if ( dclon.isDefined && dclat.isDefined) Some( s"{lon:${dclon.get},lat:${dclat.get}}") else None
    val cwprk = if ( plon.isDefined && plat.isDefined) Some( s"{lon:${plon.get},lat:${plat.get}}") else None

    val event = Seq( dtcc, cwdrv, cwprk, wbv.map(_.toString), wid.map(_.toString)).map(_.getOrElse(""))

    // LDW, FCW 이벤트와 dtcc 등의 이벤트를 TripStat 객체 형식으로 반환
    TripStat(0, 0, ldw.getOrElse(0), fcw.getOrElse(0), event)
  }

  /**
    * 201805 : 복합 센서, OBD -> ADAS -> GPS -> BBX 순으로 기준 trip을 삼는다.
    */
  def priorityDeviceTrip(deviceTrip: Seq[DeviceTripId]): Option[String] = {
    // deviceTrip.foreach{ r => println(s"${r.deviceType} ${r.id} ${r.id.nonEmpty}") }
    if (deviceTrip.exists(r => r.deviceType == "OBD")) {
      // logger.info("priorityDeviceTrip - OBD")
      Some("OBD")
    } else if (deviceTrip.exists(r => r.deviceType == "ADAS")) {
      // logger.info("priorityDeviceTrip - ADAS")
      Some("ADAS")
    } else if (deviceTrip.exists(r => r.deviceType == "GPS")) {
      // logger.info("priorityDeviceTrip - GPS")
      Some("GPS")
    } else if (deviceTrip.exists(r => r.deviceType == "BLACKBOX")) {
      // logger.info("priorityDeviceTrip - BLACKBOX")
      Some("BLACKBOX")
    } else {
      logger.warn("priorityDeviceTrip - Not found deviceType matched!!")
      None
    }
  }

}
