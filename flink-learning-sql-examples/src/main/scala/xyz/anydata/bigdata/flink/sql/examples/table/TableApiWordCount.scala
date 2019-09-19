package xyz.anydata.bigdata.flink.sql.examples.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.types.Row
import xyz.anydata.bigdata.flink.sql.examples.model.User

/**
  * 功能描述 构建Table API 与 执行查询
  *
  * @author CainGao
  * @version V_1.0
  * @date 2019/9/19 16:35
  */
object TableApiWordCount {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    val dataset:DataSet[User] = env.fromElements(
      User("CainGao",10),
      User("CainGao",1),
      User("CainGao",65)
    )

    val table:Table = tableEnv.fromDataSet(dataset)

    table.printSchema()

    table
      .groupBy("name")
      .select("name,age.sum as ages")
      .toDataSet[Row]
      .print()

    tableEnv.registerDataSet("USER",dataset,'name,'age)

    val result = tableEnv.sqlQuery("SELECT name,age FROM `USER`")

    result.toDataSet[Row].print()

  }

}
