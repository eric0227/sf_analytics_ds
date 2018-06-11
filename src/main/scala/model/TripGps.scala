package model

import java.util.UUID

import org.joda.time.DateTime

object TripGps {
  /*
  def fromMicroTrip(m:MicroTrip): TripGps = {
    val json = Json.parse(m.payload)
    new TripGps( m.id, m.tripId, m.sensorId, m.vehicleId, new DateTime(m.ts.toLong), m.ts.toLong, GeoPoint((json \ "lon").as[Double], (json \ "lat").as[Double]))
  }
  */

  def fromRawData( id:UUID, tripId:UUID, sensorId:UUID, vehicleId:UUID, lon:Double, lat:Double, ts:DateTime) : TripGps = {
    new TripGps(id, tripId, sensorId, vehicleId, ts, ts.getMillis/1000, GeoPoint(lon, lat))
  }
}

case class TripGps(id:UUID, tripId:UUID, sensorId:UUID, vehicleId:UUID, tm:DateTime, ts:Long, pos:GeoPoint)
