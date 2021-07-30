package com.dorr.flink.function.source.kafka

import java.util

import com.dorr.flink.common.utils.KafkaUtils
import com.dorr.flink.function.source.{Source, SourceContext}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._

class KafkaSource[K, V](topic: String) extends Source {

  var kafkaConsumer: KafkaConsumer[K, V] = _
  var partitions: util.ArrayList[Int] = _

  override def open() = {

    val topicPartitions = partitions.map(a => new TopicPartition(topic, a))
    kafkaConsumer = KafkaUtils.getKafkaConsumerByTopicPartition[K, V](topicPartitions.toList, 0)
  }

  override def close(): Unit = {}

  def ConsumeKafka(kafkaConsumer: KafkaConsumer[K, V])
                  (logic: KafkaConsumer[K, V] => Unit): Unit = {
    logic(kafkaConsumer)
  }


  override def collect[T](ctx: SourceContext[T]): Unit = {
    ConsumeKafka(kafkaConsumer) {
      kafka => {
        while (true) {
          val msgs: ConsumerRecords[K, V] = kafka.poll(2000)
          val it = msgs.iterator()
          while (it.hasNext) {
            val msg = it.next()
            ctx.collect(msg.value().asInstanceOf[T])
          }
        }
      }
    }

  }
}
