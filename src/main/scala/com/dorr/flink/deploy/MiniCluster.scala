package com.dorr.flink.deploy

import akka.actor.Actor
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{MemberEvent, MemberRemoved, MemberUp}
import com.dorr.flink.common.entity._
import com.dorr.flink.common.utils.ZkUtils
import com.dorr.flink.common.variable.Constants
import com.dorr.flink.jobcluster.Jobmanager
import com.dorr.flink.taskmanager.Taskmanager

class MiniCluster extends Actor with DeployMode {
  val cluster = Cluster(context.system)
  var metaUpdated = false

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent]) //在启动Actor时将该节点订阅到集群中
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  //    val initialContacts = Set(
  //      ActorPath.fromString(s"akka.tcp://${Constants.clusterName}@${Constants.hostname}:${Constants.clusterRpcPort}/system/receptionist"))
  //    val settings = ClusterClientSettings(context.system)
  //      .withInitialContacts(initialContacts)
  //
  //    val c = context.system.actorOf(ClusterClient.props(settings), Constants.clusterName)

  override def receive: Receive = {
    case jobs: Jobs[String] =>
      val jobmanager = context.actorSelection(ZkUtils.getData[String](Constants.jobmanagerZkMetaPath))
      jobmanager ! jobs
    case job: Job =>
      //      implicit val timeout = Timeout(5 seconds)
      //      ClusterClient.Send(Constants.jobmanagerRootPath, job, localAffinity = true)
      //      val result = Patterns.ask(c, ClusterClient.Send(Constants.jobmanagerRootPath, job, localAffinity = true), timeout)
      //      println(self.path)
      //      result.onComplete {
      //        case Success(transformationResult) => {
      //          println(s"success ! ${self.path.address}")
      //        }
      //        case Failure(t) => println("An error has occured: " + t.getMessage)
      //        case _ =>
      //          println("An Stream and have no ends")
      //
      //      }
      //      val jobmanager = context.actorSelection(s"akka.tcp://${Constants.clusterName}@${Constants.hostname}:${Constants.jobmanagerRpcPort}${Constants.jobmanagerRootPath}")
      val jobmanager = context.actorSelection(ZkUtils.getData[String](Constants.jobmanagerZkMetaPath))
      jobmanager ! job
    case ClusterUp =>
      if (!metaUpdated) {
        self ! ClusterUp
        Thread.sleep(1000)
      } else {
        println("start to launch the essential aritificals")
        Jobmanager start Constants.jobmanagerRpcPort
      }
    case ClusterDown =>
      val jobmanager = context.actorSelection(ZkUtils.getData[String](Constants.jobmanagerZkMetaPath))
      jobmanager ! ClusterDown
      context.stop(self)
      Thread.currentThread().interrupt()
    case JobFailed(reason, job) =>
      println(reason)
    case Message(message) =>
      println(message)
    case MemberUp(m) if m.getRoles.contains(Constants.clusterName) =>
      println("Cluster start!")
      initMeta()
      ZkUtils.save[String](Constants.jobmanagerZkMetaPath, cluster.selfUniqueAddress.address.toString + Constants.jobmanagerRootPath)
      metaUpdated = true
    case JobManagerStarted =>
      println("Startup taskmanagers")
      Constants.taskmanagerStartPort + 1 to Constants.parallelism + Constants.taskmanagerStartPort foreach Taskmanager.start
    case MemberRemoved(m, status) if (m.address.port.getOrElse[Int](-1) != Constants.jobmanagerRpcPort) => self ! Remove(sender())
  }

  def initMeta(): Unit = {
    ZkUtils.save[String]("/dorr", "dorr-namespace")
    ZkUtils.save[String]("/dorr/flink", "application: flink of dorr")
    ZkUtils.save[String]("/dorr/flink/jobmanager", "save meta data for this self made flink application")
    ZkUtils.save[String]("/dorr/flink/cluster", "save meta data for this self made flink application")
    ZkUtils.save[String](Constants.clusterZkMetaPath, s"akka.tcp://${Constants.clusterName}@${Constants.hostname}:${Constants.clusterRpcPort}${Constants.clusterRootPath}")
  }
}
