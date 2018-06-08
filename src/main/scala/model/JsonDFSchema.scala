package model

import org.apache.spark.sql.types._

object JsonDFSchema {

  // {"data":{"date":"2018-06-05T10:55","deviceType":"BLACKBOX","companyId":"9340ba10-37ae-11e8-ad7d-833dc5b9c077","payload":"[{\"tid\":16,\"try\":1,\"lat\":37.444277,\"lon\":126.64147,\"sp\":46,\"clt\":1528163846000},{\"tid\":16,\"try\":1,\"lat\":37.44415,\"lon\":126.641707,\"sp\":44,\"clt\":1528163848000},{\"tid\":16,\"try\":1,\"lat\":37.444055,\"lon\":126.641957,\"sp\":42,\"clt\":1528163850000},{\"tid\":16,\"try\":1,\"lat\":37.44397,\"lon\":126.642225,\"sp\":46,\"clt\":1528163852000},{\"tid\":16,\"try\":1,\"lat\":37.443902,\"lon\":126.642518,\"sp\":46,\"clt\":1528163854000},{\"tid\":16,\"try\":1,\"lat\":37.443835,\"lon\":126.642798,\"sp\":46,\"clt\":1528163856000},{\"tid\":16,\"try\":1,\"lat\":37.443825,\"lon\":126.643087,\"sp\":46,\"clt\":1528163858000},{\"tid\":16,\"try\":1,\"lat\":37.443817,\"lon\":126.64338,\"sp\":46,\"clt\":1528163860000},{\"tid\":16,\"try\":1,\"lat\":37.443828,\"lon\":126.643685,\"sp\":48,\"clt\":1528163862000},{\"tid\":16,\"try\":1,\"lat\":37.44386,\"lon\":126.643978,\"sp\":46,\"clt\":1528163864000},{\"tid\":16,\"try\":1,\"lat\":37.443915,\"lon\":126.644248,\"sp\":42,\"clt\":1528163866000},{\"tid\":16,\"try\":1,\"lat\":37.443965,\"lon\":126.644457,\"sp\":31,\"clt\":1528163868000},{\"tid\":16,\"try\":1,\"lat\":37.44399,\"lon\":126.644568,\"sp\":14,\"clt\":1528163870000},{\"tid\":16,\"try\":1,\"lat\":37.443998,\"lon\":126.644593,\"sp\":1,\"clt\":1528163872000},{\"tid\":16,\"try\":1,\"lat\":37.443997,\"lon\":126.644598,\"sp\":0,\"clt\":1528163874000},{\"tid\":16,\"try\":1,\"lat\":37.444008,\"lon\":126.644607,\"sp\":0,\"clt\":1528163876000},{\"tid\":16,\"try\":1,\"lat\":37.44401,\"lon\":126.644603,\"sp\":0,\"clt\":1528163878000},{\"tid\":16,\"try\":1,\"lat\":37.444008,\"lon\":126.644598,\"sp\":0,\"clt\":1528163880000},{\"tid\":16,\"try\":1,\"lat\":37.444008,\"lon\":126.644592,\"sp\":0,\"clt\":1528163882000},{\"tid\":16,\"try\":1,\"lat\":37.444005,\"lon\":126.644588,\"sp\":0,\"clt\":1528163884000},{\"tid\":16,\"try\":1,\"lat\":37.444005,\"lon\":126.644587,\"sp\":0,\"clt\":1528163886000},{\"tid\":16,\"try\":1,\"lat\":37.444005,\"lon\":126.644587,\"sp\":0,\"clt\":1528163889000},{\"tid\":16,\"try\":1,\"lat\":37.444005,\"lon\":126.644587,\"sp\":0,\"clt\":1528163890000},{\"tid\":16,\"try\":1,\"lat\":37.444005,\"lon\":126.644587,\"sp\":0,\"clt\":1528163893000},{\"tid\":16,\"try\":1,\"lat\":37.444005,\"lon\":126.644587,\"sp\":0,\"clt\":1528163894000},{\"tid\":16,\"try\":1,\"lat\":37.44401,\"lon\":126.6446,\"sp\":5,\"clt\":1528163897000},{\"tid\":16,\"try\":1,\"lat\":37.444027,\"lon\":126.644695,\"sp\":18,\"clt\":1528163898000},{\"tid\":16,\"try\":1,\"lat\":37.444058,\"lon\":126.644838,\"sp\":27,\"clt\":1528163900000},{\"tid\":16,\"try\":1,\"lat\":37.444095,\"lon\":126.645042,\"sp\":33,\"clt\":1528163902000},{\"tid\":16,\"try\":1,\"lat\":37.444147,\"lon\":126.64526,\"sp\":37,\"clt\":1528163904000}]","createdTime":1528163907839,"tripId":"a88e7700-6863-11e8-96b3-bf7af28e956c","microTripId":"f05ba8f0-6863-11e8-96b3-bf7af28e956c","vehicleId":"e02a6600-6008-11e8-9b4a-956d65b68a0a","ts":1528163904000,"sensorId":"6525c560-5e57-11e8-9082-956d65b68a0a"},"msgType":"microtrip"}
  val sf_microtrip = new StructType()
    .add("data", new StructType()
      .add("microTripId", StringType)
      .add("tripId", StringType)
      .add("sensorId", StringType)
      .add("vehicleId", StringType)
      .add("date", StringType)
      .add("ts", LongType)
      .add("payload", StringType)
      .add("deviceType", StringType)
      .add("createdTime", StringType)
      .add("companyId", StringType))
    .add("msgType", StringType)

  // "payload":"[{\"tid\":16,\"try\":1,\"lat\":37.444277,\"lon\":126.64147,\"sp\":46,\"clt\":1528163846000},{\"tid\":16,\"try\":1,\"lat\":37.44415,\"lon\":126.641707,\"sp\":44,\"clt\":1528163848000},{\"tid\":16,\"try\":1,\"lat\":37.444055,\"lon\":126.641957,\"sp\":42,\"clt\":1528163850000},{\"tid\":16,\"try\":1,\"lat\":37.44397,\"lon\":126.642225,\"sp\":46,\"clt\":1528163852000},{\"tid\":16,\"try\":1,\"lat\":37.443902,\"lon\":126.642518,\"sp\":46,\"clt\":1528163854000},{\"tid\":16,\"try\":1,\"lat\":37.443835,\"lon\":126.642798,\"sp\":46,\"clt\":1528163856000},{\"tid\":16,\"try\":1,\"lat\":37.443825,\"lon\":126.643087,\"sp\":46,\"clt\":1528163858000},{\"tid\":16,\"try\":1,\"lat\":37.443817,\"lon\":126.64338,\"sp\":46,\"clt\":1528163860000},{\"tid\":16,\"try\":1,\"lat\":37.443828,\"lon\":126.643685,\"sp\":48,\"clt\":1528163862000},{\"tid\":16,\"try\":1,\"lat\":37.44386,\"lon\":126.643978,\"sp\":46,\"clt\":1528163864000},{\"tid\":16,\"try\":1,\"lat\":37.443915,\"lon\":126.644248,\"sp\":42,\"clt\":1528163866000},{\"tid\":16,\"try\":1,\"lat\":37.443965,\"lon\":126.644457,\"sp\":31,\"clt\":1528163868000},{\"tid\":16,\"try\":1,\"lat\":37.44399,\"lon\":126.644568,\"sp\":14,\"clt\":1528163870000},{\"tid\":16,\"try\":1,\"lat\":37.443998,\"lon\":126.644593,\"sp\":1,\"clt\":1528163872000},{\"tid\":16,\"try\":1,\"lat\":37.443997,\"lon\":126.644598,\"sp\":0,\"clt\":1528163874000},{\"tid\":16,\"try\":1,\"lat\":37.444008,\"lon\":126.644607,\"sp\":0,\"clt\":1528163876000},{\"tid\":16,\"try\":1,\"lat\":37.44401,\"lon\":126.644603,\"sp\":0,\"clt\":1528163878000},{\"tid\":16,\"try\":1,\"lat\":37.444008,\"lon\":126.644598,\"sp\":0,\"clt\":1528163880000},{\"tid\":16,\"try\":1,\"lat\":37.444008,\"lon\":126.644592,\"sp\":0,\"clt\":1528163882000},{\"tid\":16,\"try\":1,\"lat\":37.444005,\"lon\":126.644588,\"sp\":0,\"clt\":1528163884000},{\"tid\":16,\"try\":1,\"lat\":37.444005,\"lon\":126.644587,\"sp\":0,\"clt\":1528163886000},{\"tid\":16,\"try\":1,\"lat\":37.444005,\"lon\":126.644587,\"sp\":0,\"clt\":1528163889000},{\"tid\":16,\"try\":1,\"lat\":37.444005,\"lon\":126.644587,\"sp\":0,\"clt\":1528163890000},{\"tid\":16,\"try\":1,\"lat\":37.444005,\"lon\":126.644587,\"sp\":0,\"clt\":1528163893000},{\"tid\":16,\"try\":1,\"lat\":37.444005,\"lon\":126.644587,\"sp\":0,\"clt\":1528163894000},{\"tid\":16,\"try\":1,\"lat\":37.44401,\"lon\":126.6446,\"sp\":5,\"clt\":1528163897000},{\"tid\":16,\"try\":1,\"lat\":37.444027,\"lon\":126.644695,\"sp\":18,\"clt\":1528163898000},{\"tid\":16,\"try\":1,\"lat\":37.444058,\"lon\":126.644838,\"sp\":27,\"clt\":1528163900000},{\"tid\":16,\"try\":1,\"lat\":37.444095,\"lon\":126.645042,\"sp\":33,\"clt\":1528163902000},{\"tid\":16,\"try\":1,\"lat\":37.444147,\"lon\":126.64526,\"sp\":37,\"clt\":1528163904000}]"
  val microtrip_payload = new ArrayType(new StructType()
    .add("lon", DoubleType)
    .add("lat", DoubleType)
    .add("clt", LongType)
    .add("sp", IntegerType)
    .add("em", IntegerType)
    .add("ldw", IntegerType)
    .add("fcw", IntegerType), true)


  //sf-trip,{"data":{"tripId":"887e3be0-47bf-11e8-9875-bf7af28e956c","vehicleId":"8006f5c0-37dd-11e8-8751-bf7af28e956c","userId":"13814000-1dd2-11b2-8080-808080808080","sensorId":"eceac220-37b0-11e8-bc18-956d65b68a0a","companyId":"887e3be0-47bf-11e8-9875-bf7af28e956c","startTs":1524574855000,"startDt":"2018-04-24 22:00:55","endTs":1524574917000,"endDt":"2018-04-24 22:01:57","deviceType":"BLACKBOX","createdTime":1524574858910,"payload":"{\"tid\":98,\"dis\":10,\"stlat\":37.38785,\"stlon\":126.962235,\"edlat\":37.387865,\"edlon\":126.96225}","updated":0},"latestTrip":[{"deviceType":"BLACKBOX","id":null}],"msgType":"trip"}
  val sf_trip = new StructType()
    .add("data", new StructType()
      .add("tripId", StringType)
      .add("vehicleId", StringType)
      .add("userId", StringType)
      .add("sensorId", StringType)
      .add("companyId", StringType)
      .add("startTs", StringType)
      .add("startDt", StringType)
      .add("endTs", StringType)
      .add("endDt", StringType)
      .add("deviceType", StringType)
      .add("createdTime", StringType)
      .add("payload", StringType)
      .add("updated", StringType))
    .add("latestTrip", new ArrayType(new StructType()
      .add("deviceType", StringType)
      .add("id", StringType), true))
    .add("msgType", StringType)

  // "payload":"{\"tid\":98,\"dis\":10,\"stlat\":37.38785,\"stlon\":126.962235,\"edlat\":37.387865,\"edlon\":126.96225}"
  val trip_payload = new StructType()
    .add("tid", IntegerType)
    .add("dis", IntegerType)
    .add("stlat", DoubleType)
    .add("stlon", DoubleType)
    .add("endlat", DoubleType)
    .add("endlon", DoubleType)


  // sf-event,{"data":{"deviceType":"BLACKBOX","eventId":"259be740-6865-11e8-96b3-bf7af28e956c","companyId":"9340ba10-37ae-11e8-ad7d-833dc5b9c077","ty":109,"payload":"{\"mod\":0,\"lat\":0.0,\"lon\":0.0}"
  // ,"createdTime":1528164426675,"tripId":"ae01cd70-6856-11e8-96b3-bf7af28e956c","vehicleId":"2c2df170-6234-11e8-8b0c-833dc5b9c077","eventTs":1528164408000,"userId":"13814000-1dd2-11b2-8080-808080808080","eventDt":"2018-06-05 11:06:48","sensorId":"65263a91-5e57-11e8-9355-bf7af28e956c"},"msgType":"event"}
  val sf_event = new StructType()
    .add("data", new StructType()
      .add("deviceType", StringType)
      .add("eventId", StringType)
      .add("companyId", StringType)
      .add("ty", StringType)
      .add("payload", StringType)
      .add("createdTime", StringType)
      .add("tripId", StringType)
      .add("vehicleId", StringType)
      .add("eventDt", StringType)
      .add("eventTs", StringType)
      .add("userId", StringType)
      .add("sensorId", StringType))
    .add("msgType", StringType)

  val gps_microtrip = new StructType()
    .add("ts", LongType)
    .add("ty", IntegerType)
    .add("pld", new StructType()
        .add("tid", StringType)
        .add("lon", StringType)
        .add("lat", StringType)
        .add("alt", StringType)
        .add("clt", StringType)
        .add("sp", StringType)
        .add("dop", StringType)
        .add("nos", StringType)
        .add("tdis", StringType)
    )

  val gpsTrip = new StructType()
    .add("ts", LongType)
    .add("ty", IntegerType)
    .add("pld", new StructType()
      .add("tid", StringType)       // Trip 고유 번호
      .add("stt", StringType)       // M Trip의 시작 날짜 및 시간
      .add("edt", StringType)       // M Trip의 종료 날짜 및 시간
      .add("dis", StringType)       // Trip의 주행거리
      .add("stlat", StringType)     // 운행 시작 좌표의 위도
      .add("stlon", StringType)     // 운행 시작 좌표의 경도
      .add("edlat", StringType)     // 운행 종료 좌표의 위도
      .add("edlon", StringType)     // 운행 종료 좌표의 경도
      .add("hsts", StringType)      // Trip의 최고 속도
      .add("mesp", StringType)      // Trip의 평균 속도
      .add("fwv", StringType)       // 펌웨어 버전
      .add("dtvt", StringType)      // 주행시간
    )
}
