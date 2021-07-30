package com.dorr.flink.taskmanager

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor.{Actor, OneForOneStrategy, RootActorPath}
import akka.cluster.ClusterEvent.{MemberEvent, MemberRemoved, MemberUp}
import akka.cluster.{Cluster, Member}
import com.dorr.flink.common.variable.Constants
import com.dorr.flink.common.entity.{Handled, Jobs, Register, Task, Tasks}
import com.dorr.flink.common.utils.ZkUtils
import com.dorr.flink.function.source.kafka.KafkaSource
import com.dorr.flink.function.source.SourceContext
import com.dorr.flink.function.process.ProcessContext
import org.apache.log4j.Logger

class TaskmanagerBackend extends Actor {
  var sourceContext: SourceContext[String] = _
  private val processContext = new ProcessContext[String]
  val log = Logger.getLogger(this.getClass);
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10) {
      case e: IllegalArgumentException => {
        log.debug(s"Restarting after receiving Illegal arguments : ${e.getMessage}")
        Restart
      }
      case unknownException: Exception => {
        log.debug(s"Giving up. Can you recover from this? :$unknownException")
        Stop
      }
      case unknownCase: Any => {
        log.debug("Giving up on unexpected case : {}", unknownCase)
        Stop
      }
    }
  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent]) //在启动Actor时将该节点订阅到集群中
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  val jobmanager = context.actorSelection(ZkUtils.getData[String](Constants.jobmanagerZkMetaPath))

  override def receive: Receive = {
    case Task(num) =>
      log.debug(s" ${self.toString()} is handling message $num")
      //      val jobmanager = context.actorSelection(s"akka.tcp://${Constants.clusterName}@${Constants.hostname}:${Constants.jobmanagerRpcPort}${Constants.jobmanagerRootPath}")
      jobmanager ! Handled(num)
    case MemberUp(m) if m.getRoles.contains(Constants.taskmanagerName) => register(m)
    case Tasks(source, sink, operator) => {
      if (sourceContext == null) sourceContext = new SourceContext[String]
      new Thread(new Runnable {
        if (sourceContext == null) sourceContext = new SourceContext[String]

        override def run(): Unit = {
          source.open()
          source.collect(sourceContext)
        }
      }).start()
      new Thread(new Runnable {
        override def run(): Unit = {
          operator.open()
          while (true) {
            Thread.sleep(1)
            if (sourceContext.queue.size > 0) {
              operator.processElement(sourceContext.queue.dequeue(), processContext)
            }
          }
        }
      }).start()
      new Thread(new Runnable {
        override def run(): Unit = {
          sink.open()
          while (true) {
            Thread.sleep(1)
            if (processContext.queue.size > 0) {
              sink.sinkTo[String](processContext.queue.dequeue())
            }
          }
        }
      }).start()
    }
  }


  def register(member: Member): Unit = {
    jobmanager ! Register
  }

}
