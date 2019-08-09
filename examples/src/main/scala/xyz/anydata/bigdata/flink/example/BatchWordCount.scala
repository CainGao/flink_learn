package xyz.anydata.bigdata.flink.example

import org.apache.flink.api.scala._

/**
  * 功能描述 批处理进行WordCount
  *
  * @author CainGao
  * @version V_1.0
  * @date 2019/8/9 10:32
  */
object BatchWordCount {

  def main(args: Array[String]): Unit = {
    //构建引擎
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 创建DataSource
    val text = env.fromElements(
      "Best Data Processing Engine")
    //flatMap : 把字符串转换为小写,并且按照空白分割为一个个的单词.
    //filter: 过滤非空结果
    //map: 把切割的单词转换为 单词,1
    //groupBy:按照下标位0进行分组
    //sum: 计算 下标位1的结果
    val counts = text.flatMap { _.toLowerCase.split("\\W+")
      .filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)
    //打印结果到控制台
    counts.print()
  }

}
