package app.udf

import com.typesafe.scalalogging.LazyLogging
import common.TreanaConfig
import common.TreanaConfig._
import model._
import org.apache.http.client.HttpResponseException
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}
import org.joda.time.LocalDateTime
import play.api.libs.json.{JsSuccess, Json}

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

  val trip_stat = udf((eventList: Seq[Row]) => {

    def checkNull(a:Any) : Int = if (a == null) 0 else a.asInstanceOf[Int]

    val events = eventList.map {
      case Row(sp, em, ldw, fcw) => TripStat(checkNull(sp), checkNull(em), checkNull(ldw), checkNull(fcw))
      case _ => TripStat(0,0,0,0)
    }

    events.reduce( _.add(_)).stat
  })


  val trip_distance = udf((gpsList:Seq[Row]) => {
    // devTime이 있으면 그 값을 사용한다. ts는 DB write 시각이 될 수 있으며, 이때는 시간 간격이 달라지게 된다.
    // 예를 들어 1초 간격으로 데이터가 오지 않고, 한꺼번에 몰릴 수 있다.
    val gps = toGpsList(gpsList)

    // sum distance
    TreMove.from(gps, 1).map( _.map(_.distance).sum).getOrElse(0)
  })


  val event = udf((gpsList: Seq[Row], deviceType:String) => {
    // GPS 정보 변환
    val gps = toGpsList(gpsList)
    val events = checkEvent( gps, deviceType)

    // 과속 여부 확인
    val overLimits = checkOverLimit(gps, deviceType)
    (events, overLimits)
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
}
