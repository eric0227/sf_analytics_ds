
DROP VIEW "sf_microtrip";
CREATE VIEW "sf_microtrip" (
  ROWKEY                    VARCHAR PRIMARY KEY,
  "cf1"."micro_trip_id"     VARCHAR,
  "cf1"."vehicle_id"        VARCHAR,
  "cf1"."trip_id"           VARCHAR,
  "cf1"."sensor_id"         VARCHAR,
  "cf1"."created_time"      VARCHAR,
  "cf1"."date"              VARCHAR,
  "cf1"."device_type"       VARCHAR,
  "cf1"."payload"           VARCHAR,
  "cf1"."ts"                UNSIGNED_LONG
);
CREATE LOCAL INDEX sf_microtrip_index ON "sf_microtrip"("cf1"."vehicle_id");
CREATE LOCAL INDEX sf_microtrip_index2 ON "sf_microtrip"("cf1"."trip_id");
!indexes "sf_microtrip"

DROP TABLE "sf_trip";
CREATE VIEW "sf_trip" (
  ROWKEY                VARCHAR PRIMARY KEY,
  "cf1"."vehicle_id"    VARCHAR,
  "cf1"."trip_id"       VARCHAR,
  "cf1"."sensor_id"     VARCHAR,
  "cf1"."created_time"  VARCHAR,
  "cf1"."date"          VARCHAR,
  "cf1"."device_type"   VARCHAR,
  "cf1"."end_dt"        VARCHAR,
  "cf1"."end_ts"        VARCHAR,
  "cf1"."start_dt"      VARCHAR,
  "cf1"."start_ts"      VARCHAR,
  "cf1"."payload"       VARCHAR,
  "cf1"."spark_idx"     VARCHAR,
  "cf1"."updated"       VARCHAR,
  "cf1"."user_id"       VARCHAR
) ;
CREATE LOCAL INDEX sf_trip_index ON "sf_trip"("cf1"."vehicle_id");
CREATE LOCAL INDEX sf_trip_index2 ON "sf_trip"("cf1"."trip_id");
!indexes "sf_trip"

DROP TABLE "sf_event";
CREATE VIEW "sf_event" (
  ROWKEY                VARCHAR PRIMARY KEY,
  "cf1"."vehicle_id"    VARCHAR,
  "cf1"."trip_id"       VARCHAR,
  "cf1"."event_id"      VARCHAR,
  "cf1"."company_id"    VARCHAR,
  "cf1"."ty"            VARCHAR,
  "cf1"."sensor_id"     VARCHAR,
  "cf1"."created_time"  VARCHAR,
  "cf1"."event_dt"      VARCHAR,
  "cf1"."event_ts"      VARCHAR,
  "cf1"."payload"       VARCHAR,
  "cf1"."device_type"   VARCHAR,
  "cf1"."user_id"       VARCHAR
) ;
CREATE LOCAL INDEX sf_event_index ON "sf_event"("cf1"."vehicle_id");
CREATE LOCAL INDEX sf_event_index2 ON "sf_event"("cf1"."trip_id");
!indexes "sf_event"