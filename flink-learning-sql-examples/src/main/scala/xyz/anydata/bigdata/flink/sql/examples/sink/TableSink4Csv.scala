package xyz.anydata.bigdata.flink.sql.examples.sink

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import xyz.anydata.bigdata.flink.sql.examples.model.User
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sinks.CsvTableSink

/**
  * 功能描述 TableCsvSink
  *
  * @author CainGao
  * @version V_1.0
  * @date 2019/9/24 11:59
  */
object TableSink4Csv {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    val dataset:DataSet[User] = env.fromElements(
      User("CainGao",10),
      User("CainGao",1),
      User("CainGao",65)
    )
    tableEnv.registerDataSet("USER",dataset)

    tableEnv.sqlQuery("SELECT * FROM `USER`")

    val csvPath = "D:/flink.csv"
    val fieldNames = Array[String]("user","age")
    val fieldTypes = Array[TypeInformation[_]](Types.STRING,Types.INT)

    val csvSink:CsvTableSink = new CsvTableSink(csvPath,",")

    tableEnv.registerTableSink("csv",fieldNames,fieldTypes,csvSink)

    tableEnv.sqlQuery("SELECT * FROM `USER` ").insertInto("csv")

    env.execute()
  }

}
