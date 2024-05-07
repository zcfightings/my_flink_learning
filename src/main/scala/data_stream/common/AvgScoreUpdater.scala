package data_stream.common

import data_stream.data_stream_api.StudentScore
import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

case class SubjectScore(subject: String, studentId: Int, avg_score: Double)

class AvgScoreUpdater extends KeyedProcessFunction[Int, StudentScore,SubjectScore] {
  private var scoreState:MapState[String, (Int, Int)] = _
  override def open(openContext: OpenContext): Unit = {
    val stateDescriptor = new MapStateDescriptor[String, (Int, Int)]("scoreStateDescriptor", classOf[String], classOf[(Int, Int)])
    scoreState = getRuntimeContext.getMapState(stateDescriptor)
  }
  override def processElement(value: StudentScore, ctx: KeyedProcessFunction[Int, StudentScore, SubjectScore]#Context, out: Collector[SubjectScore]): Unit = {
    val chineseKey = s"${value.cla}_Chinese__${value.id}"
    val mathKey = s"${value.cla}_Math__${value.id}"
    val engnishKey = s"${value.cla}_Engnish__${value.id}"

    if (scoreState.contains(chineseKey)) {
      scoreState.put(chineseKey, (scoreState.get(chineseKey)._1 + value.math_score, scoreState.get(chineseKey)._2 + 1))
    } else {
      scoreState.put(chineseKey, (value.chinese_score, 1))
    }

    if (scoreState.contains(mathKey)) {
      scoreState.put(mathKey, (scoreState.get(mathKey)._1 + value.math_score, scoreState.get(mathKey)._2 + 1))
    } else {
      scoreState.put(mathKey, (value.math_score, 1))
    }

    if (scoreState.contains(engnishKey)) {
      scoreState.put(engnishKey, (scoreState.get(engnishKey)._1 + value.math_score, scoreState.get(engnishKey)._2 + 1))
    } else {
      scoreState.put(engnishKey, (value.english_score, 1))
    }

    val chinese = scoreState.entries().asScala.filter(_.getKey.contains("Chinese")).map(x => (x.getKey.split("__")(0).toInt, x.getValue._1.toDouble / x.getValue._2)).maxBy(_._2)
    val math = scoreState.entries().asScala.filter(_.getKey.contains("Math")).map(x => (x.getKey.split("__")(0).toInt, x.getValue._1.toDouble / x.getValue._2)).maxBy(_._2)
    val english = scoreState.entries().asScala.filter(_.getKey.contains("English")).map(x => (x.getKey.split("__")(0).toInt, x.getValue._1.toDouble / x.getValue._2)).maxBy(_._2)

    out.collect(SubjectScore("Chinese", chinese._1, chinese._2))
    out.collect(SubjectScore("Math", math._1, math._2))
    out.collect(SubjectScore("English", english._1, english._2))
  }

}
