package app.udf

import app.{SfUtil, SparkSessionTestWrapper}
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import model.{JsonDFSchema, TreLocation}
import org.apache.spark.sql.Row
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._

class SfScoreUDFTest extends FunSuite  with SparkSessionTestWrapper with DatasetComparer {
  import spark.implicits._

  val sf_microtrip_data = Seq(
     """{"data":{"date":"2018-06-05T10:55","deviceType":"BLACKBOX","companyId":"9340ba10-37ae-11e8-ad7d-833dc5b9c077","payload":"[{\"tid\":524,\"try\":1,\"lat\":35.167027,\"lon\":129.060648,\"sp\":31,\"clt\":1528163858000},{\"tid\":524,\"try\":1,\"lat\":35.166963,\"lon\":129.060463,\"sp\":29,\"clt\":1528163860000},{\"tid\":524,\"try\":1,\"lat\":35.166895,\"lon\":129.060317,\"sp\":24,\"clt\":1528163861000},{\"tid\":524,\"try\":1,\"lat\":35.166842,\"lon\":129.060208,\"sp\":16,\"clt\":1528163864000},{\"tid\":524,\"try\":1,\"lat\":35.16682,\"lon\":129.06016,\"sp\":3,\"clt\":1528163866000},{\"tid\":524,\"try\":1,\"lat\":35.166817,\"lon\":129.060147,\"sp\":0,\"clt\":1528163868000},{\"tid\":524,\"try\":1,\"lat\":35.16681,\"lon\":129.060145,\"sp\":0,\"clt\":1528163870000},{\"tid\":524,\"try\":1,\"lat\":35.166807,\"lon\":129.060147,\"sp\":0,\"clt\":1528163872000},{\"tid\":524,\"try\":1,\"lat\":35.166803,\"lon\":129.060147,\"sp\":0,\"clt\":1528163874000},{\"tid\":524,\"try\":1,\"lat\":35.166802,\"lon\":129.06015,\"sp\":0,\"clt\":1528163875000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163878000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163880000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163882000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163884000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163886000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163888000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163890000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163892000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163894000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163896000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163898000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163900000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163902000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163904000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163906000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163908000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163910000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163912000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163914000},{\"tid\":524,\"try\":1,\"lat\":35.166805,\"lon\":129.060145,\"sp\":0,\"clt\":1528163916000}]","createdTime":1528163920635,"tripId":"887cbdc0-6857-11e8-8dc3-833dc5b9c077","microTripId":"f7fc2cb0-6863-11e8-8dc3-833dc5b9c077","vehicleId":"ea9e81a0-3c65-11e8-a83e-69177a4f662b","ts":1528163916000,"sensorId":"ece013c0-37b0-11e8-bc18-956d65b68a0a"},"msgType":"microtrip"}"""
    ,"""{"data":{"date":"2018-06-05T10:55","deviceType":"BLACKBOX","companyId":"9340ba10-37ae-11e8-ad7d-833dc5b9c077","payload":"[{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163860000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163862000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163864000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163866000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163868000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163870000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163872000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163875000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163876000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163878000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163880000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163882000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163884000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163886000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163888000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163890000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163892000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163894000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163896000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163898000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163900000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163903000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163905000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163906000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163909000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163911000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163913000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163914000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163916000},{\"tid\":11,\"try\":1,\"lat\":37.93397,\"lon\":127.734977,\"sp\":0,\"clt\":1528163918000}]","createdTime":1528163922645,"tripId":"0a2d7db0-6861-11e8-8dc3-833dc5b9c077","microTripId":"f92ee050-6863-11e8-8dc3-833dc5b9c077","vehicleId":"a610ebb0-6625-11e8-96b3-bf7af28e956c","ts":1528163918000,"sensorId":"652724f0-5e57-11e8-9995-833dc5b9c077"},"msgType":"microtrip"}"""
  )
  val sf_event_data = Seq(
     """{"data":{"deviceType":"BLACKBOX","eventId":"233c7370-6865-11e8-96b3-bf7af28e956c","companyId":"9340ba10-37ae-11e8-ad7d-833dc5b9c077","ty":108,"payload":"{\"auth\":1,\"fwv\":\"1.0.11\",\"bfwv\":\"1.0.05\",\"bnm\":\"M7\",\"mfwv\":\"AMM592SK-20-00\",\"sfwv\":\"20180514\",\"fqual\":2,\"fsharp\":1,\"fntbr\":1,\"fnv\":1,\"fmode\":0,\"rqual\":2,\"rntbr\":1,\"rprec\":1,\"dcs\":3,\"pcs\":2,\"fms\":3,\"rms\":3,\"spk\":3,\"mic\":0,\"cutps\":2,\"cutpl\":2,\"parke\":1,\"cuv\":4,\"cutm\":4,\"loff\":3,\"lbr\":2,\"boot\":0,\"menu\":0,\"led\":1,\"pet\":2,\"cutt\":4,\"ldws\":2,\"ldsl\":2,\"ldsr\":2,\"ldwb\":0,\"fcws\":2,\"fcwb\":0,\"fvsa\":1,\"fvs\":2,\"fvwb\":0,\"tlds\":1}","createdTime":1528164422693,"tripId":"ae01cd70-6856-11e8-96b3-bf7af28e956c","vehicleId":"2c2df170-6234-11e8-8b0c-833dc5b9c077","eventTs":1528164408000,"userId":"13814000-1dd2-11b2-8080-808080808080","eventDt":"2018-06-05 11:06:48","sensorId":"65263a91-5e57-11e8-9355-bf7af28e956c"},"msgType":"event"}"""
    ,"""{"data":{"deviceType":"BLACKBOX","eventId":"259be740-6865-11e8-96b3-bf7af28e956c","companyId":"9340ba10-37ae-11e8-ad7d-833dc5b9c077","ty":109,"payload":"{\"mod\":0,\"lat\":0.0,\"lon\":0.0}","createdTime":1528164426675,"tripId":"ae01cd70-6856-11e8-96b3-bf7af28e956c","vehicleId":"2c2df170-6234-11e8-8b0c-833dc5b9c077","eventTs":1528164408000,"userId":"13814000-1dd2-11b2-8080-808080808080","eventDt":"2018-06-05 11:06:48","sensorId":"65263a91-5e57-11e8-9355-bf7af28e956c"},"msgType":"event"}"""
  )
  val sf_trip_data = Seq(
     """{"latestTrip":[{"deviceType":"BLACKBOX","id":"65165c10-5e57-11e8-9995-833dc5b9c077"}],"data":{"deviceType":"BLACKBOX","companyId":"9340ba10-37ae-11e8-ad7d-833dc5b9c077","payload":"{\"tid\":29,\"dis\":6415,\"stlat\":35.208382,\"stlon\":126.894345,\"edlat\":35.208337,\"edlon\":126.864835}","endTs":1528163921000,"createdTime":1528161809615,"startDt":"2018-06-05 10:23:25","endDt":"2018-06-05 10:58:41","tripId":"0db815f0-685f-11e8-8dc3-833dc5b9c077","startTs":1528161805000,"vehicleId":"986811e0-5ffa-11e8-8e8e-833dc5b9c077","userId":"13814000-1dd2-11b2-8080-808080808080","sensorId":"65165c10-5e57-11e8-9995-833dc5b9c077"},"msgType":"trip"}"""
    ,"""{"latestTrip":[{"deviceType":"BLACKBOX","id":"ece60730-37b0-11e8-ad7d-833dc5b9c077"}],"data":{"deviceType":"BLACKBOX","companyId":"9340ba10-37ae-11e8-ad7d-833dc5b9c077","payload":"{\"tid\":404,\"dis\":6010,\"stlat\":37.45607,\"stlon\":127.058508,\"edlat\":37.448505,\"edlon\":127.007008}","endTs":1528164386000,"createdTime":1528163330693,"startDt":"2018-06-05 10:48:47","endDt":"2018-06-05 11:06:26","tripId":"985a2b50-6862-11e8-96b3-bf7af28e956c","startTs":1528163327000,"vehicleId":"33baba10-3c6c-11e8-a83e-69177a4f662b","userId":"13814000-1dd2-11b2-8080-808080808080","sensorId":"ece60730-37b0-11e8-ad7d-833dc5b9c077"},"msgType":"trip"}"""
  )

  test("toGpsList") {
    val gpsList = Seq(
      Row(2.toLong, 2.toLong, 2.toDouble, 2.toDouble, "id_2"),
      Row(1.toLong, 1.toLong, 1.toDouble, 1.toDouble, "id_1"),
      Row(3.toLong, 3.toLong, 3.toDouble, 3.toDouble, "id_3"),
      Row(0.toLong, 0.toLong, 0.toDouble, 0.toDouble, "id_4")
    )

    val locationList:Seq[TreLocation] =  SfScoreUDF.toGpsList(gpsList)
    println(locationList)

    assert(locationList.size == 3)
    assertResult(Seq("id_1", "id_2","id_3")) {
      locationList.map(_.id)
    }
  }

  test("sf_microtrip to DataFrame") {
    val microtrip = sf_microtrip_data
      .toDF("value")
      .select(from_json($"value", JsonDFSchema.sf_microtrip).as("value")).select("value.*")

    microtrip.createOrReplaceTempView("sf_microtrip")
    println("#### sf-microtrip ######")
    microtrip.printSchema()

    SfUtil.printConsole(microtrip.select("data.*","msgType"))
  }

  test("trip_distance") {
    val microtrip = sf_microtrip_data
      .toDF("value")
      .select(from_json($"value", JsonDFSchema.sf_microtrip).as("value")).select("value.*")
      .withColumn("mtrip_payload", explode(from_json($"data.payload", JsonDFSchema.microtrip_payload)))
      .select($"data.tripId".as("trip_id"), $"mtrip_payload", $"data.ts".as("mtrip_ts"))

    microtrip.createOrReplaceTempView("sf_microtrip")
    microtrip.printSchema()

    val df = microtrip.groupBy($"trip_id")    // 차량으로 Aggregation
      .agg(
        count($"mtrip_payload").as("mtrip_cnt")
      , collect_list(struct($"mtrip_payload.clt", $"mtrip_ts", $"mtrip_payload.lon", $"mtrip_payload.lat", $"trip_id")).as("gps_list"))
    .withColumn("device_dist", SfScoreUDF.trip_distance( $"gps_list"))

    df.printSchema()
    df.show()
  }
}
