package model

import model.TreEventEnum._
import model.TreEventString._
import play.api.libs.json.Json

case class TripStat(stat: Array[Int], event: Array[String]) {

  def add(other: TripStat): TripStat = {
    for (i <- stat.indices) this.stat(i) += other.stat(i)

    val str = for (i <- event.indices) yield {
      if (event(i).length == 0) other.event(i)
      else if (other.event(i).isEmpty) event(i)
      else if (event(i).contains(other.event(i))) event(i) // 201805 : remove duplication
      else s"${event(i)},${other.event(i)}"
    }

    new TripStat( this.stat, str.toArray)
  }

  def add(idx: Int, value: Int): TripStat = {
    this.stat(idx) += value
    this
  }

  def toJson: String = {
    val map = for (i <- stat.indices if stat(i) > 0) yield s""""${TreEventEnum(i).toString}":${stat(i)}"""
    "{" + map.mkString(",") + "}"
  }

  def toJson2: String = {
    val map = for (i <- event.indices if event(i).length > 0) yield s""""${TreEventString(i).toString}":${event(i)}"""
    "{" + map.mkString(",") + "}"
  }

  def toTreTripStatRow(tripId: String, vehicleId: String, userId: String, companyId:String, date: String, deviceType: String): TreTripStatRow =
    TreTripStatRow(stat(start.id), stat(lturn.id), stat(rturn.id), stat(uturn.id),
      stat(deaccel.id), stat(accel.id), stat(stop.id), stat(lldw.id), stat(rldw.id),
      stat(fcw1.id), stat(fcw2.id), stat(overlimit.id), stat(distance.id),
      event(dtcc.id),
      event(cwdrv.id),
      event(cwprk.id),
      event(bw.id),
      event(bbxwn.id),
      tripId, vehicleId, userId, companyId, date, deviceType)


  override def toString: String = {
    val map = for (i <- stat.indices if stat(i) > 0) yield s"${TreEventEnum(i).toString}:${stat(i)}"
    map.mkString("\n")
  }

}

object TripStat {

  def apply(sp: Int, em: Int, ldw: Int, fcw: Int, event:Seq[String] = Seq.empty[String]): TripStat = {
    val s = Array.fill(TreEventEnum.maxId){0}

    // OBD에서 발생한 비정상 주행 이벤트
    if (em > 0) {
      for (i <- start.id to stop.id) {
        if (((em >> (i - start.id)) & 0x01) == 0x01) s(i) = 1
      }
    }

    ldw match {
      case 31 => s(lldw.id) = 1
      case 32 => s(rldw.id) = 1
      case _ =>
    }

    fcw match {
      case 31 => s(fcw1.id) = 1
      case 32 => s(fcw2.id) = 1
      case _ =>
    }

    val m = if ( event.isEmpty) Array.fill(TreEventString.maxId){""} else event.toArray
    TripStat(s, m)
  }

}


object MainTripStat {

  def main(args: Array[String]): Unit = {
    val x1: TripStat = TripStat(0, 2, 0, 0) // sp:Int, em:Int, ldw:Int, fcw:Int
    val x2: TripStat = TripStat(0, 4, 0, 0)
    val x3: TripStat = TripStat(0, 8, 0, 0)
    val x4: TripStat = TripStat(0, 16, 0, 0)
    val x5: TripStat = TripStat(0, 32, 0, 0)
    val x6: TripStat = TripStat(0, 64, 0, 0)
    val a3: List[TripStat] = List(x1, x2, x3, x4, x5, x6)

    // val none, start, lturn, rturn, uturn, deaccel, accel, stop,
    a3 foreach (x => println(x.toJson))

    val tripStatRow: TreTripStatRow = a3.reduce(_.add(_)).add(TreEventEnum.distance.id, 1000)
      .toTreTripStatRow("0", "0", "0", "0", org.joda.time.DateTime.now().toString, "OBD")

    println(tripStatRow.toPrettyString)

    // val dtcc, cwdrv, cwprk, bw, bbxwn
    val events1: Seq[String] = Seq("P2502", "{lon:37.380156,lat:127.11566}", "{lon:37.379754,lat:127.115185}", "13", "")
    val events2: Seq[String] = Seq("P2503", "{lon:37.380156,lat:127.11666}", "{lon:37.379754,lat:127.115185}", "13", "")

    val y1: TripStat = TripStat(0, 0, 0, 0, events1)
    val y2: TripStat = TripStat(0, 0, 0, 0, events2)

    println(y1.toJson2)
    println(y2.toJson2)

    println(y1.add(y1).toTreTripStatRow("0", "0", "0", "0", org.joda.time.DateTime.now().toString, "OBD").toPrettyString)
    println(y1.add(y2).toTreTripStatRow("0", "0", "0", "0", org.joda.time.DateTime.now().toString, "OBD").toPrettyString)
  }

}