package com.dorr.flink.jobcluster

import java.util

import akka.actor.{Actor, ActorRef, actorRef2Scala}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{MemberEvent, MemberRemoved, MemberUp}
import com.dorr.flink.common.entity.{ClusterDown, Handled, Job, JobFailed, JobManagerStarted, Jobs, Message, Register, Remove, Task, Tasks}
import com.dorr.flink.common.utils.{AkkaZkSerializer, KafkaUtils, ZkUtils}
import com.dorr.flink.common.variable.Constants
import com.dorr.flink.function.source.kafka.KafkaSource
import org.apache.log4j.Logger

class JobManagerBackend extends Actor {
  val log = Logger.getLogger(this.getClass)
  val client = context.actorSelection(ZkUtils.getData[String](Constants.clusterZkMetaPath))

  private var backends: IndexedSeq[ActorRef] = IndexedSeq.empty[ActorRef]
  var retryForSubmitJob: Int = 0
  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent]) //在启动Actor时将该节点订阅到集群中
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  var result = 0;

  def handleJob(job: Job): Unit = {
    val len = job.jobList.length
    for (i <- 0 to len - 1) {
      backends(partitions(i)) ! Task(job.jobList(i))
    }
  }

  protected def partitions(i: Int): Int = {
    i % backends.length
  }

  override def receive: Receive = {
    case job: Job =>
      // send to clusterObject jobFinished
      validateIfJobIsReadrToSubmit(job)
    case Register if !backends.contains(sender()) =>
      context watch sender()
      backends = backends :+ sender()
      log.info(s"Successfully register taskmanager: ${sender()}, currently backend paralilim is ${backends.size} ")
    // 检查actor是否存活， remove taskmanager
    case Handled(num)
    =>
      result = result + num
      println(s"${Constants.clusterName} >> $result")
    case Remove
      if backends.contains(sender())
    =>
      backends = backends.filterNot(_ == sender())
    case ClusterDown =>
      log.error("clusterDown! ")
      backends.foreach(context.stop)
      context.stop(self)
    case MemberUp(m)
      if m.getRoles.contains(Constants.jobmanagerName)
    =>
      log.info("jobmanager start!")
      ZkUtils.save[String](Constants.jobmanagerZkMetaPath, cluster.selfUniqueAddress.address.toString + Constants.jobmanagerRootPath)
      client ! JobManagerStarted
    case MemberRemoved(m, status)
      if (m.address.port.getOrElse[Int](-1) != Constants.jobmanagerRpcPort)
    => self ! Remove(sender())

    case jobs: Jobs[String] =>
      validateIfJobIsReadrToSubmit(jobs)

  }

  @throws[IllegalArgumentException]
  def divideIntoNArr[T](arr: util.ArrayList[T], backends: IndexedSeq[ActorRef]) = {
    // if num = 0 return
    val num = backends.size
    if (num == 0) throw new IllegalArgumentException("backend has no executor to. Num can not be empty or 0")
    var map = Map[ActorRef, util.ArrayList[T]]()
    for (index <- 0 to (num - 1)) {
      map += (backends(index) -> new util.ArrayList[T]())
    }
    for (index <- 0 to (arr.size() - 1)) {
      map.get(backends(index % num)).get.add(arr.get(index))
    }
    map
  }

  def validate[T](job: T)
                 (logic: (T) => Unit): Unit = {
    if (backends.size == 0) {
      if (retryForSubmitJob < Constants.maxRetryCount) {
        client ! JobFailed(s"Fail to execute for no available executors. Will retry for ${Constants.maxRetryCount - retryForSubmitJob} times ", job)
        self ! job
        Thread.sleep(2000)
        retryForSubmitJob = retryForSubmitJob + 1
      } else {
        client ! JobFailed(s"Fail to execute for no available executors. Will not", job)
      }
    }
    else if (backends.size < Constants.parallelism) {
      if (retryForSubmitJob < Constants.maxRetryCount) {
        client ! Message(s"We do not have plenty of resources to execute this job. Palisilim: ${backends.size} Pls wait for retry. Reserve retry times: ${Constants.maxRetryCount - retryForSubmitJob}")
        self ! job
        retryForSubmitJob = retryForSubmitJob + 1
      } else {
        client ! JobFailed(s"Fail to execute for no plenty of executors. Will not", job)
      }
    }
    else {
      client ! Message(s"Prepared ready.Palisilim: ${backends.size},  Submit job....")
      logic(job)
    }


  }

  val validateIfJobIsReadrToSubmit: PartialFunction[Any, Unit] = {
    case jobs: Jobs[String] =>
      val job: Jobs[String] = jobs.asInstanceOf[Jobs[String]]
      validate[Jobs[String]](job)(handleJobs[String])
    case job: Job =>
      val job1 = job.asInstanceOf[Job]
      validate(job1)(handleJob)
  }

  def handleJobs[T](jobs: Jobs[T]) = {
    val source = jobs.source
    if (source.isInstanceOf[KafkaSource[String, String]]) {
      val kafkaSourceBase = source.asInstanceOf[KafkaSource[String, String]]
      val map: Map[ActorRef, util.ArrayList[Int]] = divideIntoNArr[Int](KafkaUtils.getKafkaTopicPartitions(Constants.topic), backends)
      for (elem <- map) {
        val sourceTmp: KafkaSource[String, String] = AkkaZkSerializer.
          deepCopyObject[KafkaSource[String, String]](kafkaSourceBase)
        sourceTmp.partitions = elem._2
        elem._1 ! new Tasks[String](sourceTmp, jobs.sink, jobs.operator)
      }
    }

  }
}

//}
