package interview

import org.apache.spark.sql.SparkSession

object SparkHolder {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("interview")
    .getOrCreate()
}
