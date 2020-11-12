package xyz.anydata.bigdata.flink.v11.example

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Apach Flink CDC Test
 *
 * @author <a href="mailto:caingao@antcloud.io">caingao</a>
 * @date 2020/11/10 8:04 下午 
 * @version 1.0
 */
object FlinkCdcExecutor {
  def main(args: Array[String]): Unit = {
    val envSetting = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env, envSetting)
    val sourceDDL =
      "CREATE TABLE test_binlog (" +
      "   id INT NOT NULl," +
      "   ip STRING," +
      "   size INT" +
      ") WITH (" +
      "'connector' = 'mysql-cdc'," +
      "'hostname' = 'localhost'," +
      "'port' = '3306'," +
      "'username' = 'root'," +
      "'password' = 'cain'," +
      "'database-name' = 'test'," +
      "'table-name' = 't_test'" +
      ")"

    // 输出目标表
    val sinkDDL =
      "CREATE TABLE test_sink (\n" +
        " ip STRING,\n" +
        " countSum BIGINT,\n" +
        " PRIMARY KEY (ip) NOT ENFORCED\n" +
        ") WITH (\n" +
        " 'connector' = 'print'\n" +
        ")"

    val exeSQL =
      "INSERT INTO test_sink " +
        "SELECT ip, SUM(size) " +
        "FROM test_binlog " +
        "GROUP BY ip"

    tableEnv.executeSql(sourceDDL)

    tableEnv.executeSql(sinkDDL)

    val result = tableEnv.executeSql(exeSQL)
    result.print()
  }

}