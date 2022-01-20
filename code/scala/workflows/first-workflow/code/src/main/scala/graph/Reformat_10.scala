package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object Reformat_10 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.select(col("cmls_3ds_authntn_mthd"),
              col("cmls_acct_fundg_srce_cd"),
              col("cmls_acct_fundg_srce_cd_enr"),
              col("cmls_acct_fundg_subtyp_cd")
    )

}
