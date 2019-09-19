package xyz.anydata.bigdata.flink.example.window

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * 功能描述 时间窗口
  *
  * @author CainGao
  * @version V_1.0
  * @date 2019/8/9 15:01
  */
object TimeWindow {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置引擎的执行为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val text = env.socketTextStream("localhost",9999)
    //设置时间戳与Watermark
    val eventText = text.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[String] {
      val maxOutOfOrderTime = 10000L  //设置10s的时间,意思是超过10s到达的数据将不会被处理
      var currentTimestamp:Long = _    // 从数据上获取到的当前时间
      override def getCurrentWatermark: Watermark = {
        //根据可容忍的最大延迟时间获取watermark
        new Watermark(currentTimestamp-maxOutOfOrderTime)
      }
      //从String中提取出事件时间
      override def extractTimestamp(str: String, l: Long): Long = {
        val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,S")
        //获取到数据的事件时间
        currentTimestamp = sdf.parse(str.split("\\|")(0)).getTime
        currentTimestamp
      }
    })

    val count = eventText.map(res=>{
      val ress  = res.split("\\|")
      (ress(1),1)
    }).keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
    //输出结果
    count.print()
    env.execute("Apache Flink Event Time Watermark")
  }
}
