package model

import org.joda.time.LocalDateTime
import util.Util._

object TreMove {

  case class Move(var sec: Long, var dist: Double) {
    def add(that: Move) = {
      this.sec = this.sec + that.sec
      this.dist = this.dist + that.dist
      this
    }
  }

  def from(locList: Seq[TreLocation], windowSize: Int): Option[Seq[TreMove]] = {
    if (locList.lengthCompare(3) < 0) return None

    val sorted = locList.sortWith( _.ts < _.ts)

    // 두 지점간의 거리 계산
    val moves = sorted.sliding(2).map { loc =>
      val sec: Long = loc.last.ts - loc.head.ts

      val pos0 = GeoPoint(loc.head.lon, loc.head.lat)
      val pos1 = GeoPoint(loc.last.lon, loc.last.lat)
      val dist = pos0.haversineDistance(pos1) * 1000.0 // unit : Meter

      Move(sec, dist) // 
    }.toSeq

    val sum = Move(0, 0)

    val a = watchTime("for (i <- 1 until Math.min(windowSize, move.size)) yield { ... ") {
      for (i <- 1 until Math.min(windowSize, moves.size)) yield {
        sum.add(moves(i - 1))
        new TreMove(Seq(sorted.head, sorted(i)), sum.sec, sum.dist.toInt)
      }
    }

    // windowSize 만큼의 시간, 거리를 누적    
    val agg = moves.sliding(windowSize).map(_.reduce((a, b) => a.add(b))).toSeq

    val b = watchTime("for (i <- 0 until agg.size) yield { ... ") {
      /*for (i <- 0 until agg.size) yield {
        val maxIdx = Math.min(sorted.size - 1, i + windowSize)
        new TreMove(Seq(sorted(i), sorted(maxIdx)), agg(i).sec, agg(i).dist.toInt)
      }*/

      // 201805: 성능 개선
      // val tt: Seq[((Move, TreLocation), Int)] = agg.zip(sorted).zip(0 until agg.size)
      val ssize = sorted.size
      agg.zip(sorted).zipWithIndex.map { case ((mv, lo), i) =>
        val maxIdx = Math.min(ssize - 1, i + windowSize)
        new TreMove(Seq(lo, sorted(maxIdx)), mv.sec, mv.dist.toInt)
      }
    }

    watchTime("Some(a ++ b)") {
      // Some(a ++ b)
      // Some(a.toSeq ++ b.toSeq)

      // 201805: 성능 개선
      Some(Seq(a, b).flatten)
    }
  }

}

/**
 * 2개 위치 정보로 계산되는 이동 정보
 * @param loc 두 지점간 위치 정보 
 * @param milliSec
 * @param distance
 */
case class TreMove(loc: Seq[TreLocation], milliSec: Long, distance: Int, speedMh: Int, var event: Option[String]) {

  def this(loc: Seq[TreLocation], milliSec: Long, distance: Int) {
    this(loc, milliSec, distance, if (milliSec < 900) 0 else ((distance.toLong * 3600000) / milliSec).toInt, None)
  }

  def toPrettyString:String = {
    val dt = new LocalDateTime(loc.head.ts)
    f"${dt.toString("hh:mm:ss")} Move(${loc.size} ${loc.head.lon}%.5f:${loc.head.lat}%.5f -> ${loc.last.lon}%.5f:${loc.last.lat}%.5f)  " +
      f"Speed(${speedMh/1000.0}%.1f) ($distance meters / $milliSec msec) $event "
  }

}

object MainTreMove {
  def main(args: Array[String]): Unit = {
    val gps: Seq[TreLocation] = List(
        TreLocation(1000L, 126.9670, 37.5671) // ts:Long, lon:Double, lat:Double, id:String=""
      , TreLocation(2000L, 126.9670, 37.5671)
      , TreLocation(3000L, 126.9670, 37.5673)
      , TreLocation(4000L, 126.9670, 37.5678)
      , TreLocation(5000L, 126.9670, 37.5682)
      , TreLocation(6000L, 126.9670, 37.5683)
      , TreLocation(7000L, 126.9670, 37.5684)
      , TreLocation(8000L, 126.9670, 37.5684)
      , TreLocation(9000L, 126.9670, 37.5684)
      , TreLocation(10000L, 126.9670, 37.5684)
      , TreLocation(11000L, 126.9670, 37.5684)
    )

//    gps.foreach(println)

    val tmList: Option[Seq[TreMove]] = TreMove.from(gps, 5)
    tmList.get foreach println

//    val distance = move.map(_.map(_.distance).sum).getOrElse(0)
//    println(s"distance = $distance")
//
//    val vehicleType = "taxi"
//    val sensorType = "taxi"
//
//    val a: Seq[TreMove] = TripEvent.updateTrafficEvent(move, Some(vehicleType), Some(sensorType))
//    a.filter(_.event.isDefined).foreach { t => println(t) }
    
  }  


}