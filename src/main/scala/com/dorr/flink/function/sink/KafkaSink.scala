package com.dorr.flink.function.sink

import com.dorr.flink.common.entity.StreamRecord
import com.dorr.flink.common.utils.KafkaUtils
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

class KafkaSink[V](topic: String) extends Sink {
  var kafkaProducer: KafkaProducer[Nothing, V] = _

  override def sinkTo[T](streamRecord: StreamRecord[T]) = {
    try {
      val record = new ProducerRecord[Nothing, V](topic, streamRecord.data.asInstanceOf[V])
      kafkaProducer.send(record, new Callback {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          println("Finished ! ")
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  override def open(): Unit = kafkaProducer = KafkaUtils.getKafkaProducer[V](topic)

  override def close(): Unit = {
    if (kafkaProducer != null) kafkaProducer.close()
  }
}
