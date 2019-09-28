package xyz.anydata.bigdata.flink.sql.examples.connector

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors._
import org.apache.flink.types.Row

/**
  * 功能描述 TODO 
  *
  * @author CainGao
  * @version V_1.0
  * @date 2019/9/27 15:15
  */
object CsvTableConnectorTests {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    val tableSchema = TableSchema.builder()
      .field("user",Types.STRING)
      .field("age",Types.INT)
      .build()

    val schema = new Schema()
      .field("user",Types.STRING)
      .field("age",Types.INT)

    tableEnv.connect(new FileSystem()
      .path("D:/flink.csv"))
      .withFormat(
        //Csv的方式一直注册失败.
        new OldCsv().schema(tableSchema).fieldDelimiter(",")
      )
      .withSchema(schema)
      .registerTableSink("SYS_USER")

    val result = tableEnv.sqlQuery("SELECT * FROM `SYS_USER`")
    result.printSchema()


  }

}
