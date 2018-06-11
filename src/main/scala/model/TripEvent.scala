package model

import java.util.UUID

import com.typesafe.config.ConfigObject
import com.typesafe.scalalogging.StrictLogging
import common.TreanaConfig
import org.joda.time.format.DateTimeFormat

object TripEvent extends StrictLogging {

  type SpeedSpec = Map[String, Seq[Int]]
  private var spec_ : Option[Map[String, SpeedSpec]] = None

  /**
    * Some(Map(stop -> List(-1, -14, 5),
    * deaccel -> List(-1, -14, -1),
    * accel -> List(-1, 8, -1),
    * start -> List(5, 8, -1)))
    *
    * @param category
    * @return
    */

  def specOf(category: String = "taxi"): Option[SpeedSpec] = {
    if (spec_.isEmpty) {
      import scala.collection.JavaConverters._

      val list: Iterable[ConfigObject] = TreanaConfig.config.getObjectList("treana.speed.spec").asScala

      try {
        val s = list.map { item =>
          val category = item.get("category").unwrapped.toString
          val values = item.withoutKey("category").withoutKey("type").unwrapped.asScala

          (category, (for ((k, v) <- values)
            yield (k, v.asInstanceOf[String].split(",").map(x => if (x == "-") -1 else (x.toDouble * 1000).toInt).toSeq)).toMap)
        }.toMap
        spec_ = Some(s)
        logger.info(s"spec updated $spec_")
      }
      catch {
        case e: Exception => logger.error(s"Failed to get config treana.speed.spec $e")
      }
    }
    spec_.flatMap(_.get(category))
  }

  def spec(vehicleType: Option[String], sensorType: Option[String]): SpeedSpec = {
    val s = sensorType.flatMap(TripEvent.specOf(_))
    if (s.isDefined) s.get
    else vehicleType.flatMap(TripEvent.specOf(_)).getOrElse(TripEvent.specOf().get)
  }

  /**
    * eTAS 화물차 버스 택시
    * 급가속	초당 11km/h 이상 가속	, 초당 5.0km/h 이상 가속, 초당 6.0km/h 이상 가속, 초당 8.0km/h 이상 가속
    * 급감속	초당 7.5km/h 이상 감속, 초당 8km/h 이상 감속, 초당 9km/h 이상 감속, 초당 14km/h 이상 감속 운행
    * 급정지	초당 7.5km/h 이상 감속하여 속도가 0이 된 경우, 초당 8km/h 이상 감속하여 속도가 5km/h 이내가 된 경우, 초당 9km/h 이상 감속하여 속도가 5km/h 이내 된 경우, 초당 14km/h 이상 감속하여 속도가 5km/h 이내가 된 경우
    * 급출발	정지 상태에서 11km/h 이상 가속, 5km 이하에서 출발하여 초당 5km/h 이상 가속 운행, 5km 이하에서 출발하여 초당 6km/h 이상 가속, 운행	5km 이하에서 출발하여 초당 8km/h 이상 가속 운행
    *
    * @return
    */
  def trafficEvent(move: Seq[TreMove], vehicleType: Option[String], sensorType: Option[String], verbose:Boolean = false): Option[String] = {
    require(move.size >= 2)

    def checkEvent(delta: Int, map: SpeedSpec, list: Seq[String]): Option[String] = {
      list match {
        case k :: tail =>
          val check = map.get(k).map(v =>
            if (v(1) > 0) (delta >= v(1)) && (v.head < 0 || move.head.speedMh <= v.head) // 가속
            else (delta <= v(1)) && (v.head < 0 || move.head.speedMh >= v.head) && (v(2) < 0 || move(1).speedMh <= v(2)) // 감속
          )

          check.flatMap(x => if (x) Some(k) else checkEvent(delta, map, tail))

        case Nil => None
      }
    }

    val s = spec(vehicleType, sensorType)
    val deltaSpeed = move(1).speedMh - move.head.speedMh
    val deltaTs = Math.abs(move(1).loc.head.ts - move.head.loc.head.ts)

    // 가속한 경우는 start, accel 조건중에서 찾고
    // 감속한 경우는 stop, deaccel에서 찾는다.
    // 순차적으로 비교하므로 start/stop 이 acce/deaccel 보다 먼저 나와야 한다.
    val event = if (deltaTs < 900) None // 0.9초 이하면 무시한다.
      else {
        if (deltaSpeed > 0) checkEvent(deltaSpeed, s, Seq("start", "accel"))
          else if (deltaSpeed < 0) checkEvent(deltaSpeed, s, Seq("stop", "deaccel"))
          else None
      }

    if ( verbose && event.isDefined) println( f"${event.get}%8s Delta=${deltaSpeed/1000.0}%.1f Size:${move.size} Move(${move.head.toPrettyString} -> ${move(1).toPrettyString}) ")

    event
  }

  def updateTrafficEvent(move: Option[Seq[TreMove]], vehicleType: Option[String], sensorType: Option[String], verbose:Boolean = false): Seq[TreMove] = {
    // 이벤트 발생 여부 계산
    if (move.isDefined) {
      for (i <- 1 until move.get.size) yield {
        move.get(i).event = trafficEvent(move.get.slice(i - 1, i + 1), vehicleType, sensorType, verbose)
      }
      move.get
    }
    else Seq.empty[TreMove]
  }

  def checkOverLimit(move: Seq[TreMove]): Seq[TreMove] = {
    move
  }

  def from(s: Seq[TripGps], vehicleType: Option[String], sensorType: Option[String]): TripEvent = {
    require(s.size >= 2)
    from(s.head, s(1), vehicleType, sensorType)
  }

  def from(s0: TripGps, s1: TripGps, vehicleType: Option[String], sensorType: Option[String]): TripEvent = {
    val sec: Long = (s1.ts - s0.ts) / 1000
    val dist: Double = s0.pos.haversineDistance(s1.pos)
    val speed: Int = ((dist * 3600) / sec.toDouble).toInt

    new TripEvent(s1.id, s0.tripId, s0.sensorId, s0.vehicleId,
      s0.pos, s1.pos, s0.tm, s1.tm,
      sec, dist, speed, None, vehicleType, sensorType)
  }

}

/**
  *
  * @param id
  * @param vehicleId
  * @param tripId
  * @param startPos
  * @param endPos
  * @param startTm
  * @param endPayloadTs
  * @param elapsedSec
  * @param distance
  * @param speedMh - meter per hour
  * @param event
  */
case class TripEvent(id: UUID, tripId: UUID, vehicleId: UUID, sensorId: UUID,
                     startPos: GeoPoint, endPos: GeoPoint,
                     startTm: org.joda.time.DateTime, endPayloadTs: org.joda.time.DateTime,
                     elapsedSec: Long, distance: Double,
                     speedMh: Int,
                     vehicleType: Option[String] = None,
                     sensorType: Option[String] = None,
                     var event: Option[String] = None)
//  extends TreMove( startPos, endPos, startTm.getMillis/1000, endPayloadTs.getMillis/1000, elapsedSec, distance, speedMh, vehicleType, sensorType)
{
  override def toString: String = {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    s"(id:$id, vh:$vehicleId tr:$tripId, pos:$startPos->$endPos ${formatter.print(startTm)}~${formatter.print(endPayloadTs)} elapsed:${elapsedSec}s dist:$distance m speed:$speedMh km/h event:$event)"
  }

  def toShortDesc: String = s"(id:$id, dist:$distance m speed:$speedMh km/h event:$event)"
}

object MainTripEvent {

  def main(args: Array[String]): Unit = {
    // taxi, bus, truck, etas
    println("taxi:", TripEvent.specOf())
    println("bus:", TripEvent.specOf("bus"))
    println("truck:", TripEvent.specOf("truck"))
    println("etas:", TripEvent.specOf("etas"))

    // println(s"${TripEvent.spec(Option("taxi"), None)}")

    val list: Seq[(String, Double, Double, Option[String])] = Seq(("taxi", 6, 14, Some("accel")) // 8km 이상이면 급가속
      , ("taxi", 50, 36, Some("deaccel")) // -14km 이상이면 급감속
      , ("taxi", 5, 13, Some("start")) // 5km 이하에서 출발하여 8km 이상 가속하면 급출발
      , ("taxi", 19, 5, Some("stop")) // -14km 이상 감속 후 5km 이하면 급정지
    )

    for (t <- list) {
      val m: Seq[TreMove] = Seq(
        TreMove(Seq(TreLocation(0L, 0.0, 0.0), TreLocation(1000L, 0.0, 0.0)), 10L, 10, (t._2 * 1000).toInt, None),
        TreMove(Seq(TreLocation(1000L, 0.0, 0.0), TreLocation(2000L, 0.0, 0.0)), 10L, 10, (t._3 * 1000).toInt, None)
      )
      println(s"$t => ${TripEvent.trafficEvent(m, Some(t._1), Some("GPS"))}") // move: Seq[TreMove], vehicleType: Option[String], sensorType: Option[String]
      // (taxi,6.0,14.0,Some(accel)) => Some(accel)
      // (taxi,50.0,36.0,Some(deaccel)) => Some(deaccel)
      // (taxi,5.0,13.0,Some(start)) => Some(start)
      // (taxi,19.0,5.0,Some(stop)) => Some(stop)
    }
  }

}