package data_stream.common

import org.apache.flink.streaming.api.functions.source.SourceFunction

class SimpleSource extends SourceFunction[String] {
  @volatile private var isRunning = true
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    println(s"in run, is_running = ${isRunning}")
    while (isRunning) {
      println("getting into simple Source")
      ctx.collect("hello world")
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
