package xyz.anydata.bigdata.flink.sql.examples.source

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sources.CsvTableSource

/**
  * 功能描述 使用CSV格式注册Table
  *
  * @author CainGao
  * @version V_1.0
  * @date 2019/9/24 10:52
  */
object CsvTableSourceWordCount {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    //使用CSV的方式进行注册表结构.
    // 参数(path:数据的路径地址,fieldNames:字段名称,fieldTypes:字段类型,fieldDelim:csv分隔符,rowDelim:行分割方式)
    val csvTableSource:CsvTableSource =new CsvTableSource("datas.csv",Array("exitcode","count"),Array(Types.STRING,Types.INT),",","\n")

    tableEnv.registerTableSource("csv",csvTableSource)

    env.execute()
  }

}
