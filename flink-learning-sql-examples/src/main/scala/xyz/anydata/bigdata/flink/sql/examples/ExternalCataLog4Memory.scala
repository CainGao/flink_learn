package xyz.anydata.bigdata.flink.sql.examples

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.catalog.{ExternalCatalog, InMemoryExternalCatalog}

/**
  * 功能描述 内存CataLog实现
  *
  * @author CainGao
  * @version V_1.0
  * @date 2019/9/24 14:35
  */
object ExternalCataLog4Memory {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    val memoryCataLog:ExternalCatalog = new InMemoryExternalCatalog("UserCataLog")
    tableEnv.registerExternalCatalog("user",memoryCataLog)



  }

}
