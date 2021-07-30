package com.dorr.flink.common.entity

import akka.actor.ActorRef
import com.dorr.flink.function.source.Source
import com.dorr.flink.function.process.Operator
import com.dorr.flink.function.sink.Sink

trait TypeInformation

final case class Job(jobList: Array[Int]) extends TypeInformation

final case class Jobs[In](source: Source, sink: Sink, operator: Operator) extends TypeInformation

// If i use a mulitply operators then there will be a operator chain
final case class Tasks[In](source: Source, sink: Sink, operator: Operator) extends TypeInformation

case object Register extends TypeInformation

final case class Remove(taskmanager: ActorRef) extends TypeInformation

final case class Message(message: String) extends TypeInformation

final case class JobFailed(reason: String, job: Any) extends TypeInformation //任务失败相应原因

final case class Task(num: Int) extends TypeInformation

final case class Handled(num: Int) extends TypeInformation

case object ClusterUp extends TypeInformation

case object ClusterDown extends TypeInformation

case object JobManagerStarted extends TypeInformation