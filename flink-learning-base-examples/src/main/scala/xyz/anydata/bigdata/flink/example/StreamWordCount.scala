package xyz.anydata.bigdata.flink.example

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * 功能描述 TODO 
  *
  * @author CainGao
  * @version V_1.0
  * @date 2019/8/9 10:58
  */
object StreamWordCount {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost",9999)
    //flatMap : 把字符串转换为小写,并且按照空白分割为一个个的单词.
    //filter: 过滤非空结果
    //map: 把切割的单词转换为 单词,1
    //timeWindow: 按照时间,每5s获取进行一次计算
    //sum: 计算 下标位1的结果
    val counts = text.flatMap { _.toLowerCase.split("\\W+")
      .filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    //打印结果到控制台
    counts.print()

  }


}
