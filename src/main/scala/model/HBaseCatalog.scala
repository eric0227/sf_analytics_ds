package model

object HBaseCatalog {

  def sf_microtrip = s"""
       |{
       |  "table":{"namespace":"default", "name":"sf_microtrip"},
       |  "rowkey":"key1",
       |  "columns":{
       |    "key1"          :{"cf":"rowkey", "col":"key1", "type":"string"},
       |    "micro_trip_id" :{"cf":"cf1", "col":"micro_trip_id", "type":"string"},
       |    "vehicle_id"   :{"cf":"cf1", "col":"vehicle_id", "type":"string"},
       |    "trip_id"      :{"cf":"cf1", "col":"trip_id", "type":"string"},
       |    "sensor_id"    :{"cf":"cf1", "col":"sensor_id", "type":"string"},
       |    "created_time" :{"cf":"cf1", "col":"created_time", "type":"string"},
       |    "date"         :{"cf":"cf1", "col":"date", "type":"string"},
       |    "device_type"  :{"cf":"cf1", "col":"device_type", "type":"string"},
       |    "payload"      :{"cf":"cf1", "col":"payload", "type":"string"},
       |    "ts"           :{"cf":"cf1", "col":"ts", "type":"long"}
       |  }
       |}""".stripMargin

  def sf_trip = s"""
        |{
        |  "table":{"namespace":"default", "name":"sf_trip"},
        |  "rowkey":"key1",
        |  "columns":{
        |    "key1"           :{"cf":"rowkey", "col":"key1", "type":"string"},
        |    "vehicle_id"     :{"cf":"cf1", "col":"vehicle_id", "type":"string"},
        |    "trip_id"        :{"cf":"cf1", "col":"trip_id", "type":"string"},
        |    "sensor_id"      :{"cf":"cf1", "col":"sensor_id", "type":"string"},
        |    "created_time"   :{"cf":"cf1", "col":"created_time", "type":"string"},
        |    "date"           :{"cf":"cf1", "col":"date", "type":"string"},
        |    "device_type"    :{"cf":"cf1", "col":"device_type", "type":"string"},
        |    "end_dt"         :{"cf":"cf1", "col":"end_dt", "type":"string"},
        |    "end_ts"         :{"cf":"cf1", "col":"end_ts", "type":"string"},
        |    "start_dt"       :{"cf":"cf1", "col":"start_dt", "type":"string"},
        |    "start_ts"       :{"cf":"cf1", "col":"start_ts", "type":"string"},
        |    "payload"        :{"cf":"cf1", "col":"payload", "type":"string"},
        |    "spark_idx"      :{"cf":"cf1", "col":"spark_idx", "type":"string"},
        |    "updated"        :{"cf":"cf1", "col":"updated", "type":"string"},
        |    "user_id"        :{"cf":"cf1", "col":"user_id", "type":"string"}
        |  }
        |}""".stripMargin

  def sf_event = s"""
       |{
       |  "table":{"namespace":"default", "name":"sf_event"},
       |  "rowkey":"key1",
       |  "columns":{
       |    "key1"           :{"cf":"rowkey", "col":"key1", "type":"string"},
       |    "vehicle_id"     :{"cf":"cf1", "col":"vehicle_id", "type":"string"},
       |    "trip_id"        :{"cf":"cf1", "col":"trip_id", "type":"string"},
       |    "event_id"       :{"cf":"cf1", "col":"event_id", "type":"string"},
       |    "company_id"     :{"cf":"cf1", "col":"company_id", "type":"string"},
       |    "ty"             :{"cf":"cf1", "col":"ty", "type":"string"},
       |    "sensor_id"      :{"cf":"cf1", "col":"sensor_id", "type":"string"},
       |    "created_time"   :{"cf":"cf1", "col":"created_time", "type":"string"},
       |    "event_dt"       :{"cf":"cf1", "col":"event_dt", "type":"string"},
       |    "event_ts"       :{"cf":"cf1", "col":"event_ts", "type":"string"},
       |    "payload"        :{"cf":"cf1", "col":"payload", "type":"string"},
       |    "device_type"    :{"cf":"cf1", "col":"device_type", "type":"string"},
       |    "user_id"        :{"cf":"cf1", "col":"user_id", "type":"string"}
       |  }
       |}""".stripMargin
}
