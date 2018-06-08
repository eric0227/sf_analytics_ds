package app.udf

import model.{JsonDFSchema, TreLocation, TreMove}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType}

object SfScoreUDF {

  def toGpsList( gpsList:Seq[Row]) : Seq[TreLocation] = {
    gpsList.map{
      case Row( devTime, ts:Long, lon:Double, lat:Double, id:String) =>
        TreLocation( if (devTime == null) ts else devTime.asInstanceOf[Long], lon, lat, id)
      case _ => TreLocation( 0, 0, 0)
    }.filter( _.ts > 0).sortWith( _.ts < _.ts)
  }


  val trip_distance = udf((gpsList:Seq[Row]) => {
    // devTime이 있으면 그 값을 사용한다. ts는 DB write 시각이 될 수 있으며, 이때는 시간 간격이 달라지게 된다.
    // 예를 들어 1초 간격으로 데이터가 오지 않고, 한꺼번에 몰릴 수 있다.
    val gps = toGpsList(gpsList)

    // sum distance
    TreMove.from(gps, 1).map( _.map(_.distance).sum).getOrElse(0)
  })

/*
  val event = udf((gpsList: Seq[Row], deviceType:String) => {
    // GPS 정보 변환
    val gps = TripDF.toGpsList(gpsList)
    val events = TripDF.checkEvent( gps, deviceType)

    // 과속 여부 확인
    val overLimits = TripDF.checkOverLimit(gps, deviceType)
    (events, overLimits)
  })
*/
}
