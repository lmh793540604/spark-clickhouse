import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, SQLException, Statement}
import java.util.{Date, Properties}

import Dao.OrcaleSelect
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object orcale2clickhouse {

  /**
   * 主函数
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    //clickhouse 数据导入结果表
    val click_table_name = "kkx_sbd.zc_yx_all"
    val choose = args(0) // 接受参数 211 116 环境控制
    var oracleJdbcUrl = ""
    var chJdbcUrl = ""
    val oracleDsProp = new Properties
    if (choose == "116"){
      oracleJdbcUrl = "jdbc:oracle:thin:@oracle111:1521:orcl"
      oracleDsProp.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
      oracleDsProp.setProperty("user","KKX_CENTER_DB") //oracle 连接配 176北京
      oracleDsProp.setProperty("password","KKX_CENTER_DB")//oracle 连接配置 176北京

      chJdbcUrl = "jdbc:clickhouse://hadoop101:8123/kkx_sbd"
    }
    else if (choose == "211"){
      //oracle 连接配置 211北京
      oracleJdbcUrl = "jdbc:oracle:thin:@10.1.64.212:1521:orcl"
      oracleDsProp.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
      oracleDsProp.setProperty("user","KKX_CENTER") //oracle 连接配 176北京
      oracleDsProp.setProperty("password","KKX_CENTER")//oracle 连接配置 176北京

      chJdbcUrl = "jdbc:clickhouse://10.1.64.206:8123/kkx_sbd"
    }
    //clickhouse 连接配置
    val chDsProp = new Properties
    chDsProp.setProperty("driver", "ru.yandex.clickhouse.ClickHouseDriver")
    // 默认没有密码，所以不需要设置user、password属性
    //chDsProp.setProperty("user", "")
    //chDsProp.setProperty("password", "")
    chDsProp.put("batchsize", "100000")
    chDsProp.put("socket_timeout", "300000")
    chDsProp.put("numPartitions", "8")
    chDsProp.put("rewriteBatchedStatements", "true")




    // 初始化Spark环境
    // val sparkConf = new SparkConf().setMaster("local[*]")
    val start = new Date().getTime
    val spark = SparkSession
      .builder()
      // .config(sparkConf)
      .config("spark.sql.crossJoin.enabled", "true")
      .appName(getClass.getName)
      // .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    //logInfo("参数示例: " + args(0))
    val yxJkxlDf = spark.read.jdbc(oracleJdbcUrl, "YX_JKXL", oracleDsProp)
      .createOrReplaceTempView("YX_JKXL")
    val zcJkxlDf = spark.read.jdbc(oracleJdbcUrl, "ZC_JKXL", oracleDsProp)
      .createOrReplaceTempView("ZC_JKXL")
    val EMPTY = spark.read.jdbc(oracleJdbcUrl, "EMPTY", oracleDsProp)
      .createOrReplaceTempView("EMPTY")
    val DM_JC = spark.read.jdbc(oracleJdbcUrl, "DM_JC", oracleDsProp)
      .createOrReplaceTempView("DM_JC")
    val DM_XSDW = spark.read.jdbc(oracleJdbcUrl, "DM_XSDW", oracleDsProp)
      .createOrReplaceTempView("DM_XSDW")
    val DM_BDZ = spark.read.jdbc(oracleJdbcUrl, "DM_BDZ", oracleDsProp)
      .createOrReplaceTempView("DM_BDZ")
    val DM_XHGG = spark.read.jdbc(oracleJdbcUrl, "DM_XHGG", oracleDsProp)
      .createOrReplaceTempView("DM_XHGG")
    val DM_QY = spark.read.jdbc(oracleJdbcUrl, "DM_QY", oracleDsProp)
      .createOrReplaceTempView("DM_QY")
    val DM_SJ = spark.read.jdbc(oracleJdbcUrl, "DM_SJ", oracleDsProp)
      .createOrReplaceTempView("DM_SJ")
    val DM_SB = spark.read.jdbc(oracleJdbcUrl, "DM_SB", oracleDsProp)
      .createOrReplaceTempView("DM_SB")
    val DM_JSYY = spark.read.jdbc(oracleJdbcUrl, "DM_JSYY", oracleDsProp)
      .createOrReplaceTempView("DM_JSYY")
    val DM_ZRYY = spark.read.jdbc(oracleJdbcUrl, "DM_ZRYY", oracleDsProp)
      .createOrReplaceTempView("DM_ZRYY")
    val ZC_BYQ = spark.read.jdbc(oracleJdbcUrl, "ZC_BYQ", oracleDsProp)
      .createOrReplaceTempView("ZC_BYQ")
    val yx_byq = spark.read.jdbc(oracleJdbcUrl, "yx_byq", oracleDsProp)
      .createOrReplaceTempView("yx_byq")
    val ZC_DLQ = spark.read.jdbc(oracleJdbcUrl, "ZC_DLQ", oracleDsProp)
      .createOrReplaceTempView("ZC_DLQ")
    val YX_DLQ = spark.read.jdbc(oracleJdbcUrl, "YX_DLQ", oracleDsProp)
      .createOrReplaceTempView("YX_DLQ")
    val DM_SSXS = spark.read.jdbc(oracleJdbcUrl, "DM_SSXS", oracleDsProp)
      .createOrReplaceTempView("DM_SSXS")
    val YX_DKQ = spark.read.jdbc(oracleJdbcUrl, "YX_DKQ", oracleDsProp)
      .createOrReplaceTempView("YX_DKQ")
    val DM_LSGX = spark.read.jdbc(oracleJdbcUrl, "DM_LSGX", oracleDsProp)
      .createOrReplaceTempView("DM_LSGX")
    val ZC_DLHGQ = spark.read.jdbc(oracleJdbcUrl, "ZC_DLHGQ", oracleDsProp)
      .createOrReplaceTempView("ZC_DLHGQ")
    val YX_DLHGQ = spark.read.jdbc(oracleJdbcUrl, "YX_DLHGQ", oracleDsProp)
      .createOrReplaceTempView("YX_DLHGQ")
    val ZC_DYHGQ = spark.read.jdbc(oracleJdbcUrl, "ZC_DYHGQ", oracleDsProp)
      .createOrReplaceTempView("ZC_DYHGQ")
    val YX_DYHGQ = spark.read.jdbc(oracleJdbcUrl, "YX_DYHGQ", oracleDsProp)
      .createOrReplaceTempView("YX_DYHGQ")
    val ZC_GLKG = spark.read.jdbc(oracleJdbcUrl, "ZC_GLKG", oracleDsProp)
      .createOrReplaceTempView("ZC_GLKG")
    val YX_GLKG = spark.read.jdbc(oracleJdbcUrl, "YX_GLKG", oracleDsProp)
      .createOrReplaceTempView("YX_GLKG")
    val zc_blq = spark.read.jdbc(oracleJdbcUrl, "zc_blq", oracleDsProp)
      .createOrReplaceTempView("zc_blq")
    val YX_BLQ = spark.read.jdbc(oracleJdbcUrl, "YX_BLQ", oracleDsProp)
      .createOrReplaceTempView("YX_BLQ")
    val ZC_DKQ = spark.read.jdbc(oracleJdbcUrl, "ZC_DKQ", oracleDsProp)
      .createOrReplaceTempView("ZC_DKQ")
    val ZC_OHDRQ = spark.read.jdbc(oracleJdbcUrl, "ZC_OHDRQ", oracleDsProp)
      .createOrReplaceTempView("ZC_OHDRQ")
    val YX_OHDRQ = spark.read.jdbc(oracleJdbcUrl, "YX_OHDRQ", oracleDsProp)
      .createOrReplaceTempView("YX_OHDRQ")
    val ZC_ZBQ = spark.read.jdbc(oracleJdbcUrl, "ZC_ZBQ", oracleDsProp)
      .createOrReplaceTempView("ZC_ZBQ")
    val YX_ZBQ = spark.read.jdbc(oracleJdbcUrl, "YX_ZBQ", oracleDsProp)
      .createOrReplaceTempView("YX_ZBQ")
    val ZC_DLXL = spark.read.jdbc(oracleJdbcUrl, "ZC_DLXL", oracleDsProp)
      .createOrReplaceTempView("ZC_DLXL")
    val YX_DLXL = spark.read.jdbc(oracleJdbcUrl, "YX_DLXL", oracleDsProp)
      .createOrReplaceTempView("YX_DLXL")
    val DM_XL = spark.read.jdbc(oracleJdbcUrl, "DM_XL", oracleDsProp)
      .createOrReplaceTempView("DM_XL")
    val ZC_MX = spark.read.jdbc(oracleJdbcUrl, "ZC_MX", oracleDsProp)
      .createOrReplaceTempView("ZC_MX")
    val YX_MX = spark.read.jdbc(oracleJdbcUrl, "YX_MX", oracleDsProp)
      .createOrReplaceTempView("YX_MX")
    val ZC_ZHDQ_JG_YJ = spark.read.jdbc(oracleJdbcUrl, "ZC_ZHDQ_JG_YJ", oracleDsProp)
      .createOrReplaceTempView("ZC_ZHDQ_JG_YJ")
    val ZC_ZHDQ_JG = spark.read.jdbc(oracleJdbcUrl, "ZC_ZHDQ_JG", oracleDsProp)
      .createOrReplaceTempView("ZC_ZHDQ_JG")
    val ZC_ZHDQ = spark.read.jdbc(oracleJdbcUrl, "ZC_ZHDQ", oracleDsProp)
      .createOrReplaceTempView("ZC_ZHDQ")
    val YX_ZHDQ = spark.read.jdbc(oracleJdbcUrl, "YX_ZHDQ", oracleDsProp)
      .createOrReplaceTempView("YX_ZHDQ")

    //通过JDBC 执行SQL
    //清表更新数据
    exeSql(s"truncate TABLE kkx_sbd.DM_SB ",chJdbcUrl)
    spark.sql(
      """
        |select
        | id
        | ,create_by
        | ,create_time
        | ,update_by
        | ,update_time
        | ,del_status
        | ,zsblb
        | ,xthsb
        | ,zxthbj
        | ,sbdm
        | ,sbmc
        | ,sbqc
        | ,sbcj
        | ,zsblbmc
        | ,xthsbmc
        | ,zxthbjmc
        | ,parent_id
        | ,fid
        | ,MD5
        | ,sync_status
        |FROM
        |DM_SB
        |""".stripMargin).write.mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000)
      .jdbc(chJdbcUrl, "kkx_sbd.DM_SB", chDsProp)


    //清表更新数据
    exeSql(s"truncate TABLE kkx_sbd.DM_SJ ",chJdbcUrl)
    spark.sql(
      """
        |select
        |id,create_by,create_time,update_by,update_time,del_status,sjztbm,sjztfh,sjztmc,sjztyxj,sbxqxbz,md5,sync_status
        | from DM_SJ
        |""".stripMargin).write.mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000)
      .jdbc(chJdbcUrl, "kkx_sbd.DM_SJ", chDsProp)

    //清表更新数据
    exeSql(s"truncate TABLE kkx_sbd.DM_ZRYY ",chJdbcUrl)
    spark.sql(
      """
        |select
        |id,create_by,create_time,update_by,update_time,del_status,parent_id,zryydm,zryymc,zryyqc,md5,sync_status
        |from DM_ZRYY
        |""".stripMargin).write.mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000)
      .jdbc(chJdbcUrl, "kkx_sbd.DM_ZRYY", chDsProp)


//    清表更新数据
    exeSql(s"truncate TABLE kkx_sbd.zc_yx_local ON CLUSTER ck_cluster_1 ",chJdbcUrl)

//    全量导入数据
//     SQL1 变压器
    val SQL1 = OrcaleSelect.sql1(spark)
    SQL1.write.mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000)
      .jdbc(chJdbcUrl, click_table_name, chDsProp)

    //SQL2  架空线路
    val SQL2 = OrcaleSelect.sql2(spark)
    SQL2.write.mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000)
      .jdbc(chJdbcUrl, click_table_name, chDsProp)

    val SQL3 = OrcaleSelect.sql3(spark)
    SQL3.write.mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000)
      .jdbc(chJdbcUrl, click_table_name, chDsProp)

    val SQL4 = OrcaleSelect.sql4(spark)
    SQL4.write.mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000)
      .jdbc(chJdbcUrl, click_table_name, chDsProp)

    val SQL5 = OrcaleSelect.sql5(spark)
    SQL5.write.mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000)
      .jdbc(chJdbcUrl, click_table_name, chDsProp)

    val SQL6 = OrcaleSelect.sql6(spark)
    SQL6.write.mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000)
      .jdbc(chJdbcUrl, click_table_name, chDsProp)

    val SQL7 = OrcaleSelect.sql7(spark)
    SQL7.write.mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000)
      .jdbc(chJdbcUrl, click_table_name, chDsProp)

    val SQL8 = OrcaleSelect.sql8(spark)
    SQL8.write.mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000)
      .jdbc(chJdbcUrl, click_table_name, chDsProp)

    val SQL9 = OrcaleSelect.sql9(spark)
    SQL9.write.mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000)
      .jdbc(chJdbcUrl, click_table_name, chDsProp)

    val SQL10 = OrcaleSelect.sql10(spark)
    SQL10.write.mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000)
      .jdbc(chJdbcUrl, click_table_name, chDsProp)

    val SQL11 = OrcaleSelect.sql11(spark)
    SQL11.write.mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000)
      .jdbc(chJdbcUrl, click_table_name, chDsProp)

    val SQL12 = OrcaleSelect.sql12(spark)
    SQL12.write.mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000)
      .jdbc(chJdbcUrl, click_table_name, chDsProp)

    val SQL13 = OrcaleSelect.sql13(spark)
    SQL13.write.mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 100000)
      .jdbc(chJdbcUrl, click_table_name, chDsProp)

    //4444
    val end = new Date().getTime
    println("-----------------------运行时间：" + (end - start).toString)

    spark.stop()
  }


  def exeSql(sql: String,chJdbcUrl :String): Unit = {
    var connection: Connection = null
    var statement: Statement = null
    var results: ResultSet = null

    Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
    connection = DriverManager.getConnection(chJdbcUrl)
    statement = connection.createStatement
    val begin: Long = System.currentTimeMillis
    results = statement.executeQuery(sql)
    val end: Long = System.currentTimeMillis
    System.out.println("执行（" + sql + "）耗时：" + (end - begin) + "ms")
    if (results != null) results.close()
    if (statement != null) statement.close()
    if (connection != null) connection.close()
    //      val rsmd: ResultSetMetaData = results.getMetaData
    //      val list = new util.ArrayList[util.HashMap[String,String]]
    //      while ( {
    //        results.next
    //      }) {
    //        val map = new util.HashMap[String,String]
    //        var i = 1
    //        while ( {
    //          i <= rsmd.getColumnCount
    //        }) {
    //          map.put(rsmd.getColumnName(i), results.getString(rsmd.getColumnName(i)))
    //
    //          {
    //            i += 1; i - 1
    //          }
    //        }
    //        list.add(map)
    //      }
    //      import scala.collection.JavaConversions._
    //      if (list.length>0){
    //        for (map <- list) {
    //          System.err.println(map)
    //        }
    //      }
    //    } catch {
    //      case e: Exception =>
    //        e.printStackTrace()
    //    } finally {
    //关闭连接
    //      try {
    //        if (results != null) results.close()
    //        if (statement != null) statement.close()
    //        if (connection != null) connection.close()
    //      } catch {
    //        case e: SQLException =>
    //          e.printStackTrace()
    //      }
  }
}
