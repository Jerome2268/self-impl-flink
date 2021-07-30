package com.dorr.flink.common.utils

import java.util
import java.util.Properties

import com.dorr.flink.common.variable.Constants
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

object KafkaUtils {

  def ConsumeKafka(kafkaConsumer: KafkaConsumer[String, String])
                  (logic: KafkaConsumer[String, String] => Unit): Unit = {
    logic(kafkaConsumer)
  }

  def getKafkaTopicPartitions(topic: String): util.ArrayList[Int] = {
    val prop = new Properties
    prop.put("bootstrap.servers", Constants.kafkaConnect)
    // 指定消费者组
    prop.put("group.id", "group01")
    prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // 指定消费的value的反序列化方式
    prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val kafkaConsumer = new KafkaConsumer[String, String](prop)
    val partitionInfoes: util.List[PartitionInfo] = kafkaConsumer.partitionsFor(topic)
    val topicPartitions = new util.ArrayList[Int]()
    for (index <- 0 to (partitionInfoes.size() - 1)) {
      topicPartitions.add(partitionInfoes.get(index).partition())
    }
    topicPartitions
  }

  def getKafkaConsumerByTopicPartition[In, Out](topicPartitions: util.List[TopicPartition], offset: Long) = {
    val prop = new Properties
    prop.put("bootstrap.servers", Constants.kafkaConnect)
    prop.put("group.id", "group01")
    prop.put("auto.offset.reset", "earliest")
    prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("enable.auto.commit", "true")
    prop.put("session.timeout.ms", "30000")
    val kafkaConsumer = new KafkaConsumer[In, Out](prop)
    kafkaConsumer.assign(topicPartitions)
    for (index <- 0 to (topicPartitions.size() - 1)) {
      kafkaConsumer.seek(topicPartitions.get(index), offset)
    }
    kafkaConsumer
  }

  def getKafkaProducer[T](topic: String): KafkaProducer[Nothing, T] = {
    val props = new Properties()
    props.put("bootstrap.servers", Constants.kafkaConnect)
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")

    new KafkaProducer[Nothing, T](props)
//    try {
//      val record = new ProducerRecord[Nothing, T](topic, "mumianmianmianmian".asInstanceOf[T])
//      producer.send(record, new Callback {
//        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
//          println("Finished ! ")
//        }
//      })
//    } catch {
//      case e: Exception => e.printStackTrace()
//    } finally {
//      producer.close()
//    }
  }


}
