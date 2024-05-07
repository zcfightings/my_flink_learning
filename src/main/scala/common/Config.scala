package common

object Config {
  val kafkaProducerConfig = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "ack" -> "all",
    "retries" -> "0",
    "batch.size" -> "16384",
    "linger.ms" -> "1",
    "buffer.memory" -> "33554432",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
  )

  val kafkaConsumerConfig = Map(
    "bootstrap.servers" -> "localhost:9092",
    "group.id" -> "test-consumer-group",
    "enable.auto.commit" -> "true",
    "auto.commit.interval.ms" -> "5000",
    "session.timeout.ms" -> "30000",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
  )





}
