import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import graph._
import graph.SubGraph_1

object Main {

  def apply(spark: SparkSession): Unit = {
    val df_Source_0   = Source_0(spark)
    val df_Reformat_1 = Reformat_1(spark,       df_Source_0)
    val df_Reformat_2 = Reformat_2(spark,       df_Reformat_1)
    val df_SubGraph_1 = SubGraph_1.apply(spark, df_Reformat_2)
    val df_Reformat_4 = Reformat_4(spark,       df_SubGraph_1)
  }

  def main(args: Array[String]): Unit = {
    import config._
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Workflow")
      .config("spark.default.parallelism", "4")
      .enableHiveSupport()
      .getOrCreate()
    apply(spark)
  }

}
