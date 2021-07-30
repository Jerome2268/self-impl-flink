package com.dorr.example

import akka.actor.ActorRef
import com.dorr.example.function.KafkaStringProcessFunction
import com.dorr.flink.client.FlinkCli
import com.dorr.flink.common.entity.Jobs
import com.dorr.flink.common.variable.Constants
import com.dorr.flink.function.sink.{FileSink, KafkaSink}
import com.dorr.flink.function.source.kafka.KafkaSource

object App {

  def main(args: Array[String]): Unit = {
    val miniCluster: ActorRef = FlinkCli.get(Constants.clusterRpcPort)

    //    new Thread(new Runnable {
    //      override def run(): Unit = {
    //        val arr = Array(1, 2, 3, 4, 5, 6)
    //        miniCluster ! Job(arr)
    //      }
    //    }).start()
    // Submit job
    new Thread(new Runnable {
      override def run(): Unit = {
        val source = new KafkaSource[String, String](Constants.topic)
        val sink = new FileSink()
//                val sink = new KafkaSink[String](Constants.topic)
        val function = new KafkaStringProcessFunction
        miniCluster ! Jobs(source, sink, function)
      }
    }).start()
  }

}
