package Dao

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object test {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkSession = SparkSession.
      builder().
      master("local[*]").
      appName("test").
      //config("spark.sql.shuffle.partitions", 500).
      //config("spark.sql.parquet.binaryAsString", "true").
      //config("hive.exec.dynamici.partition", true).
      //config("hive.exec.dynamic.partition.mode", "nonstrict").
      //enableHiveSupport().
      getOrCreate()

    sparkSession.sql(
      """
        |select 'hello world'
        |""".stripMargin).show()




      sparkSession.stop()



    // 初始化Spark环境

    /*val spark = SparkSession
      .builder()
      // .config(sparkConf)
      .config("spark.sql.crossJoin.enabled","true")
      .appName(getClass.getName)
   //   .enableHiveSupport()
      .getOrCreate()

    spark.sql(
      """
        |select 'hello world'
        |""".stripMargin).show()

    spark.close()*/
  }

}
