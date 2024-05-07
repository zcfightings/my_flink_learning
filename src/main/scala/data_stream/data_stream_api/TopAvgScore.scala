package data_stream.data_stream_api

import data_stream.common.{AvgScoreUpdater, SimpleSource, StudentScoreGenerator}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector


case class StudentScore(id: Int, cla: Int, chinese_score: Int, math_score: Int, english_score: Int, ts: Long)
case class SubjectAverage(subject: String, id:Int, average: Double)


object TopAvgScore {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val score = env.addSource(new StudentScoreGenerator())
    val result = score.keyBy(new KeySelector[StudentScore, Int] {
      override def getKey(value: StudentScore): Int = value.id}
    ).flatMap(new RichFlatMapFunction[StudentScore, SubjectAverage]() {
        private var sum: MapState[String, java.util.HashMap[Int, (Long, Int)]] = _
        override def open(parameters: Configuration): Unit = {
          sum = getRuntimeContext.getMapState(new MapStateDescriptor[String, java.util.HashMap[Int, (Long, Int)]]("average", classOf[String], classOf[java.util.HashMap[Int, (Long, Int)]]))
        }
        override def flatMap(score: StudentScore, out: Collector[SubjectAverage]): Unit = {
          if (sum.contains("Chinese")) {
            val subjectDetail = sum.get("Chinese")
            if (subjectDetail.containsKey(score.id)) {
              subjectDetail.put(score.id, ( subjectDetail.get(score.id)._1 + score.chinese_score, subjectDetail.get(score.id)._2 + 1))
            } else {
              subjectDetail.put(score.id, (score.chinese_score, 1))
            }
          } else {
            val subjectDetail = new java.util.HashMap[Int, (Long, Int)]()
            subjectDetail.put(score.id, (score.chinese_score, 1))
            sum.put("Chinese", subjectDetail)
          }

          if (sum.contains("Math")) {
            val subjectDetail = sum.get("Math")
            if (subjectDetail.containsKey(score.id)) {
              subjectDetail.put(score.id, (subjectDetail.get(score.id)._1 + score.math_score, subjectDetail.get(score.id)._2 + 1))
            } else {
              subjectDetail.put(score.id, (score.math_score, 1))
            }
          } else {
            val subjectDetail = new java.util.HashMap[Int, (Long, Int)]()
            subjectDetail.put(score.id, (score.math_score, 1))
            sum.put("Math", subjectDetail)
          }

          if (sum.contains("English")) {
            val subjectDetail = sum.get("English")
            if (subjectDetail.containsKey(score.id)) {
              subjectDetail.put(score.id, (subjectDetail.get(score.id)._1 + score.english_score, subjectDetail.get(score.id)._2 + 1))
            } else {
              subjectDetail.put(score.id, (score.english_score, 1))
            }
          } else {
            val subjectDetail = new java.util.HashMap[Int, (Long, Int)]()
            subjectDetail.put(score.id, (score.english_score, 1))
            sum.put("English", subjectDetail)
          }

          out.collect(SubjectAverage("Chinese",  score.id,  sum.get("Chinese").get(score.id)._1.toDouble / sum.get("Chinese").get(score.id)._2))
          out.collect(SubjectAverage("Math",  score.id,  sum.get("Math").get(score.id)._1.toDouble / sum.get("Math").get(score.id)._2))
          out.collect(SubjectAverage("English",  score.id,  sum.get("English").get(score.id)._1.toDouble / sum.get("English").get(score.id)._2))
        }
      }).keyBy(new KeySelector[SubjectAverage, String] {
      override def getKey(value: SubjectAverage): String = value.subject
    }).maxBy("average")

    result.print()
    env.execute("Calculate max average scores")
  }

}
