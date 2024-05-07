package common

import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.JavaConverters._
import java.util.Properties
import scala.io.Source

object KafkaUtil {

  def main(args: Array[String]): Unit = {
    Source.fromFile("/Users/chen.zhao5/Desktop/UserBehavior_part_0.csv")
      .getLines()
      .foreach(line => send("first_test_topic", line))
    close()
  }


  val producer = {
    val props = new Properties()
    props.putAll(Config.kafkaProducerConfig.asJava)
    new org.apache.kafka.clients.producer.KafkaProducer[String, String](props)
  }

  val consumer = {
    val props = new Properties()
    props.putAll(Config.kafkaConsumerConfig.asJava)
    new org.apache.kafka.clients.consumer.KafkaConsumer[String, String](props)
  }

  def send(topic : String, message : String) = {
    val record = new org.apache.kafka.clients.producer.ProducerRecord[String, String](topic, message)
    producer.send(record)
  }

  def consume(topic : String) = {
    consumer.subscribe(List(topic).asJava)
    val records = consumer.poll(1000)
    records.iterator().asScala.map(_.value()).toList
  }

  def close() = {
    producer.close()
    consumer.close()
  }


  def send(topic : String, messages : List[String]) = {
    for (msg <- messages) {
      val record = new ProducerRecord[String, String]("topic1",  msg)
      producer.send(record)
    }
  }


}
