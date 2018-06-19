package app

import org.scalatest.FunSuite
import com.github.mrpowers.spark.fast.tests.{DataFrameComparer, DatasetComparer}
import org.apache.spark.sql.functions._

class SfMicroTripStreamingTest  extends FunSuite  with SparkSessionTestWrapper with DatasetComparer {
  import spark.implicits._

  SfMicroTripStreaming
}
