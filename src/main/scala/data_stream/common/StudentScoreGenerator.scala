package data_stream.common


import data_stream.data_stream_api.StudentScore
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.util.Random

class StudentScoreGenerator extends RichSourceFunction[StudentScore] {
  @volatile private var isRunning = true
  override def run(ctx: SourceFunction.SourceContext[StudentScore]): Unit = {
    val rand = new Random()
    while (isRunning) {
      val id = rand.nextInt(10)
      val cla = rand.nextInt(2)
      val chinese_score = 60 + rand.nextInt(90)
      val math_score = 30 + rand.nextInt(120)
      val engnish_score = 30 + rand.nextInt(120)

      ctx.collect(StudentScore(id, cla, chinese_score, math_score, engnish_score, System.currentTimeMillis()))
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = {
     isRunning = false
  }
}
