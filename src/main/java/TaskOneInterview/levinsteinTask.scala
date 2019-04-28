package TaskOneInterview

import java.util
import java.util.Arrays

import interview.SparkHolder
import org.apache.spark.sql.{SparkSession, functions}
//import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.functions._

import scala.math.{max, min}

object levinsteinTask {


  def levenshtein(a: String, b: String):Int =
    ((0 to b.size).toList /: a)((prev, x) =>
      (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
        case (h, ((d, v), y)) => min(min(h + 1, v + 1), d + (if (x == y) 0 else 1))
      }) last

  def substringA(a: String): String ={
    a.substring(0, a.indexOf("@"))
  }



  def main(args: Array[String]): Unit = {
    val dataframe1 = SparkHolder.spark.read.text("data/Emails/emails.txt")
    val dataframe2 = SparkHolder.spark.read.text("data/Emails/names.txt")

    val levenshteinUDF = udf[Int, String, String](levenshtein)
    val substringAUDF = udf[String, String](substringA)

    import SparkHolder.spark.implicits._

    val df1 =  dataframe1.toDF("email")
    val df2 =  dataframe2.toDF("name")

   val df3 =
     df1
     .crossJoin(df2)
     .select(df1("email").as("email"),substringAUDF(df1("email")).as("word"), df2("name"))
       .withColumn("levenshtein",
       levenshteinUDF(col("word"), col("name")))

    df3.join(
      df3.groupBy(s"email")
        .agg(functions.min("levenshtein")
          .as("r_levenshtein"))
        .withColumnRenamed("email", "r_email"),
      $"email" === $"r_email" && $"levenshtein" === $"r_levenshtein")
      .select("name", "email", "levenshtein")
      .show()
  }
}
