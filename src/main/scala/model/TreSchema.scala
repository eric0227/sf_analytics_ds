package model

import com.datastax.spark.connector._
import common.TreanaConfig
import common.TreanaConfig._

case class SchemaDef( name:String, schema:Seq[String], columns:Seq[ColumnRef] = Seq.empty[ColumnRef])

object TreSchema {


  /**
    * Configuration for DSE
    */
  object dse {

    val keyspace = TreanaConfig.config.getOrElse[String]("treana.dse.keyspace", "thingsboard")

    val sensor = SchemaDef("tre_sensor", Seq())
    val vehicle = SchemaDef("tre_vehicle", Seq())
    val user = SchemaDef("tre_user", Seq())
    val company = SchemaDef("tre_company", Seq())
    val trip = SchemaDef("tre_trip", Seq(),
      ColumnName("id").as("trip_id") +: Seq("vehicle_id", "user_id", "company_id", "sensor_id", "start_ts", "start_dt", "end_ts", "end_dt",
        "device_type", "created_time", "payload", "updated").map( ColumnName(_)))

    val tripBySparkIdx = SchemaDef("tre_trip_by_spark_idx", Seq(), trip.columns)

    // spark temp tables
    val sparkTempTableTripOverLimit = "trip_overlimit"


    val streamingIdx = SchemaDef( "streaming_idx",
      Seq( s"""CREATE TABLE IF NOT exists $keyspace.streaming_idx
         |(profile text,
         |partition_key_name text,
         |partition_key text,
         |info text,
         |stime timestamp,
         |etime timestamp,
         |elapsed int,
         |primary key (profile));
         |""".stripMargin))

    val streamingHistory = SchemaDef( "streaming_history", Seq(
      s"""CREATE TABLE IF NOT exists $keyspace.streaming_history
         |(id timeuuid,
         |profile text,
         |stime timestamp,
         |etime timestamp,
         |elapsed int,
         |primary key (id));
         |""".stripMargin))


    val microTripByTripId = SchemaDef( "tre_micro_trip_by_trip_id", Seq(),
      ColumnName("id").as("microTripId") +:
        Seq( "trip_id", "sensor_id", "vehicle_id", "date", "ts", "payload", "device_type", "created_time").map( ColumnName(_)))


    val eventByTripId = SchemaDef("tre_event_by_trip_id", Seq(
      s"""CREATE MATERIALIZED VIEW IF NOT exists $keyspace.tre_event_by_trip_id AS
         |SELECT *
         |FROM $keyspace.tre_event
         |WHERE trip_id IS NOT NULL AND vehicle_id IS NOT NULL AND id IS NOT NULL AND sensor_id IS NOT NULL and ty IS NOT NULL
         |PRIMARY KEY (trip_id, id, vehicle_id, sensor_id, ty);
       """.stripMargin
      ),
      ColumnName("id").as("event_id") +: Seq( "id", "event_ts", "event_dt", "vehicle_id", "sensor_id", "device_type",
      "ty", "trip_id", "user_id", "payload", "created_time").map(ColumnName(_))
    )

    val statColumns = Seq( "start"
      ,"stop"
      ,"accel"
      ,"deaccel"
      ,"lturn"
      ,"rturn"
      ,"uturn"
      ,"overlimit"
      ,"lldw"
      ,"rldw"
      ,"fcw1"
      ,"fcw2"
      ,"distance"
      ,"trip_distance"
      )

    val tripScore = SchemaDef("tre_trip_score",
      Seq(s"""CREATE TABLE IF NOT exists $keyspace.tre_trip_score
         |(vehicle_id timeuuid,
         |user_id timeuuid,
         |trip_id timeuuid,
         |dist int,
         |accel int,
         |deaccel int,
         |overlimit int,
         |norm_dist double,
         |norm_accel double,
         |norm_deaccel double,
         |norm_overlimit double,
         |score double,
         |primary key (vehicle_id, user_id, trip_id));
         """.stripMargin)
    )

    val tripStat = SchemaDef( "tre_trip_stat",
      // date : YYYYMMDD '20171201' 하루 단위로 기록
      Seq( s"""CREATE TABLE IF NOT exists $keyspace.tre_trip_stat
         |(trip_id timeuuid,
         |vehicle_id timeuuid,
         |user_id timeuuid,
         |company_id timeuuid,
         |date text,
         |service_type text,
         |device_type text,
         |dtcc text,
         |cwdrv text,
         |cwprk text,
         |bw text,
         |bbxwn text,
         |${statColumns.map(_ + " int,").mkString(" ")}
         |primary key (trip_id,vehicle_id));
         """.stripMargin),
      Seq("start", "stop" ,"accel" ,"deaccel" ,"lturn" ,"rturn"
      ,"uturn" ,"overlimit" ,"lldw" ,"rldw" ,"fcw1" ,"fcw2" ,"distance"
      ,"dtcc" ,"cwdrv" ,"cwprk" ,"bw" ,"bbxwn"
      ,"trip_id", "vehicle_id", "user_id", "company_id", "date", "device_type").map(ColumnName(_))
    )

    // 201805 : 복합 센서 통계 테이블
    val msTripStat = SchemaDef( "tre_ms_trip_stat",
      // date : YYYYMMDD '20171201' 하루 단위로 기록
      Seq( s"""CREATE TABLE IF NOT exists $keyspace.tre_ms_trip_stat
              |(trip_id timeuuid,
              |vehicle_id timeuuid,
              |user_id timeuuid,
              |company_id timeuuid,
              |date text,
              |service_type text,
              |device_type text,
              |dtcc text,
              |cwdrv text,
              |cwprk text,
              |bw text,
              |bbxwn text,
              |${statColumns.map(_ + " int,").mkString(" ")}
              |primary key (trip_id,vehicle_id));
         """.stripMargin),
      Seq("start", "stop" ,"accel" ,"deaccel" ,"lturn" ,"rturn"
        ,"uturn" ,"overlimit" ,"lldw" ,"rldw" ,"fcw1" ,"fcw2" ,"distance"
        ,"dtcc" ,"cwdrv" ,"cwprk" ,"bw" ,"bbxwn"
        ,"trip_id", "vehicle_id", "user_id", "company_id", "date", "device_type").map(ColumnName(_))
    )

    val tripStatByVehicle = SchemaDef( "tre_trip_stat_by_vehicle_id", Seq(
      s"""CREATE MATERIALIZED VIEW IF NOT exists $keyspace.tre_trip_stat_by_vehicle_id AS
         |SELECT *
         |FROM $keyspace.tre_trip_stat
         |WHERE trip_id IS NOT NULL AND vehicle_id IS NOT NULL
         |PRIMARY KEY (vehicle_id, trip_id);
       """.stripMargin
    ))

    val gpsEvent = SchemaDef( "tre_gps_event",
      Seq( s"""CREATE TABLE IF NOT exists $keyspace.tre_gps_event
         |(id timeuuid,
         |vehicle_id timeuuid,
         |trip_id timeuuid,
         |event_ts timestamp,
         |event_type int,
         |info text,
         |pos_lon double,
         |pos_lat double,
         |device_type text,
         |service_type text,
         |speed_mh int,
         |user_id timeuuid,
         |primary key (vehicle_id,trip_id,id));
           """.stripMargin),
      Seq( "id", "vehicle_id", "user_id", "trip_id",
      "pos_lon", "pos_lat", "event_ts", "event_type", "speed_mh", "info", "device_type")
    )

    val vehicleScore = SchemaDef( "tre_vehicle_score", Seq(

        s"""CREATE TABLE IF NOT exists $keyspace.tre_vehicle_score
         |(vehicle_id timeuuid,
         |user_id timeuuid,
         |cnt int,
         |ts timestamp,
         |score double,
         |primary key (vehicle_id,user_id));
         """.stripMargin)
    )

    val vehicleScoreByUserId = SchemaDef( "tre_vehicle_score_by_user_id", Seq(

      s"""CREATE MATERIALIZED VIEW IF NOT exists $keyspace.tre_vehicle_score_by_user_id AS
         |SELECT *
         |FROM $keyspace.tre_vehicle_score
         |WHERE vehicle_id IS NOT NULL AND user_id IS NOT NULL
         |PRIMARY KEY (user_id, vehicle_id);
       """.stripMargin
    ))

    def schema : Seq[String] = Seq( gpsEvent, tripStat, tripScore, streamingIdx, streamingHistory, vehicleScore, vehicleScoreByUserId).flatMap( _.schema)
  }

  object sparkCassandra {

    val keyspace = TreanaConfig.config.getOrElse[String]("treana.spark.cassandra.keyspace", "spark")

    val trip = SchemaDef( "tre_trip", Seq(
      s"""CREATE TABLE IF NOT exists $keyspace.tre_trip (
         |  	id timeuuid,
         |  	start_ts bigint,
         |  	start_dt text,
         |  	end_ts bigint,
         |  	end_dt text,
         |  	vehicle_id timeuuid,
         |  	sensor_id timeuuid,
         |  	device_type text,
         |  	user_id timeuuid,
         |  	company_id timeuuid,
         |  	payload text,
         |    created_time timestamp,
         |  	updated int,
         |    spark_idx int,
         |  	PRIMARY KEY (id, vehicle_id, sensor_id)
         |  );
       """.stripMargin
    ),
      ColumnName("id").as("tripId") +: Seq( "vehicle_id", "user_id", "sensor_id", "company_id", "start_ts", "start_dt",
        "end_ts", "end_dt", "device_type", "created_time", "payload").map( ColumnName(_)))

    val tripEvent = SchemaDef( "tre_event_by_trip_id", Seq(
      s"""CREATE TABLE IF NOT exists $keyspace.tre_event_by_trip_id (
          | id timeuuid,
          | event_ts bigint,
          | event_dt text,
          | vehicle_id timeuuid,
          | sensor_id timeuuid,
          | device_type text,
          | ty int,
          | trip_id timeuuid,
          | user_id timeuuid,
          | payload text,
          | created_time timestamp,
          | PRIMARY KEY (trip_id, vehicle_id, device_type, event_ts)
          |);
       """.stripMargin
    ),
      ColumnName("id").as("eventId") +: Seq( "trip_id", "sensor_id", "vehicle_id", "user_id",
      "created_time", "event_ts", "event_dt", "ty", "payload", "device_type").map(ColumnName(_)))

    val microTripByTripId = SchemaDef( "tre_micro_trip_by_trip_id", Seq(
      s"""CREATE TABLE IF NOT exists $keyspace.tre_micro_trip_by_trip_id (
         |    id timeuuid,
         |    trip_id timeuuid,
         |    sensor_id timeuuid,
         |    device_type text,
         |    vehicle_id timeuuid,
         |    ts bigint,
         |    date text,
         |    payload text,
         |    created_time timestamp,
         |    PRIMARY KEY (trip_id, vehicle_id, device_type, ts)
         |);
       """.stripMargin
    ),
      ColumnName("id").as("microTripId") +: Seq( "trip_id", "sensor_id", "vehicle_id", "date", "ts", "payload", "device_type", "created_time").map( ColumnName(_)))

    // 201805 : 복합 센서 mview - microTripByVehicleId
    val microTripByVehicleId = SchemaDef( "tre_micro_trip_by_vehicle_id", Seq(
      s"""CREATE MATERIALIZED VIEW IF NOT exists $keyspace.tre_micro_trip_by_vehicle_id AS
         |SELECT *
         |FROM $keyspace.tre_micro_trip_by_trip_id
         |WHERE id IS NOT NULL AND trip_id IS NOT NULL AND sensor_id IS NOT NULL AND vehicle_id IS NOT NULL AND device_type is NOT NULL AND ts IS NOT NULL
         |PRIMARY KEY (vehicle_id, device_type, ts, trip_id)
         |WITH CLUSTERING ORDER BY ( id DESC );
       """.stripMargin
    ))

    // 201805 : 복합 센서 mview - tripEventByVehicleId
    val tripEventByVehicleId = SchemaDef( "tre_event_by_vehicle_id", Seq(
      s"""CREATE MATERIALIZED VIEW IF NOT exists $keyspace.tre_event_by_vehicle_id AS
         |SELECT *
         |FROM $keyspace.tre_event_by_trip_id
         |WHERE id IS NOT NULL AND trip_id IS NOT NULL AND sensor_id IS NOT NULL AND vehicle_id IS NOT NULL AND device_type is NOT NULL AND event_ts IS NOT NULL
         |PRIMARY KEY (vehicle_id, device_type, event_ts, trip_id);
       """.stripMargin
    ))

    val microTripColumnList = ColumnName("id").as("microTripId") +:
      Seq( "trip_id", "sensor_id", "vehicle_id", "date", "ts", "payload", "device_type", "created_time").map( ColumnName(_))

    val tripStat = dse.tripStat

    // 201805 : microTripByVehicleId, tripEventByVehicleId 추가
    def schema : Seq[String] = Seq( trip, microTripByTripId, microTripByVehicleId, tripEvent, tripEventByVehicleId, tripStat).flatMap( _.schema)
  }

}
