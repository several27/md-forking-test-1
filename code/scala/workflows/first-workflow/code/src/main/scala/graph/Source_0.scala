package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._

object Source_0 {

  def apply(spark: SparkSession): DataFrame = {
    Config.fabricName match {
      case "livy" =>
        spark.read
          .format("parquet")
          .load(
            "file:/storage/workflowdata/visa/pdg-data/pdg-data/b2smsotc_dtl_dup/"
          )
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
