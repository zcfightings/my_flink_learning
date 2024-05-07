package dataset

import org.apache.flink.table.api.Expressions.row
import org.apache.flink.table.api.{$, DataTypes, EnvironmentSettings, FieldExpression, Schema, TableDescriptor, TableEnvironment}


object ConsumeKafkaTest {
  def main(args: Array[String]): Unit = {
    val envSetting = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()

    val env = TableEnvironment.create(envSetting)
    env.executeSql(
      """
        |create table score (
        |  id BIGINT,
        |  name STRING,
        |  age INT,
        |  math_score INT,
        |  chinese_score INT,
        |  english_score INT,
        |  created_at TIMESTAMP(3)
        |  )
        |with (
        |  'connector' = 'datagen',
        |  'rows-per-second' = '1',
        |  'fields.id.kind' = 'random',
        |  'fields.id.min' = '1',
        |  'fields.id.max' = '10',
        |  'fields.name.length' = '10',
        |  'fields.age.kind' = 'random',
        |  'fields.age.min' = '12',
        |  'fields.age.max' = '20',
        |  'fields.math_score.kind' = 'random',
        |  'fields.math_score.min' = '30',
        |  'fields.math_score.max' = '150',
        |  'fields.chinese_score.kind' = 'random',
        |  'fields.chinese_score.min' = '60',
        |  'fields.chinese_score.max' = '130',
        |  'fields.english_score.kind' = 'random',
        |  'fields.english_score.min' = '50',
        |  'fields.english_score.max' = '150'
        |)
        |""".stripMargin)

    env.executeSql(
      """
        CREATE VIEW StudentAverageScores AS
        SELECT
          id,
          avg(chinese_score)  AS avg_chinese,
          avg(math_score) AS avg_math,
          avg(english_score) AS avg_english
        FROM
          score
        group by id
        """)

    val result = env.executeSql(
      """
        SELECT subject, top_student_id, top_avg_score
        FROM (
          SELECT
            'Chinese' AS subject,
            id AS top_student_id,
            avg_chinese AS top_avg_score,
            RANK() OVER (ORDER BY avg_chinese DESC) as rank1
          FROM
            StudentAverageScores
          UNION ALL
          SELECT
            'Math',
            id,
            avg_math,
            RANK() OVER (ORDER BY avg_math DESC) as rank1
          FROM
            StudentAverageScores
          UNION ALL
          SELECT
            'English',
            id,
            avg_english,
            RANK() OVER (ORDER BY avg_english DESC) as rank1
          FROM
            StudentAverageScores
        ) WHERE rank1 = 1
        """)

    result.print()
  }

}
