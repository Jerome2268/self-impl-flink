package com.dorr.flink.taskmanager

import akka.actor.{ActorSystem, Props}
import com.dorr.flink.common.variable.Constants
import com.typesafe.config.ConfigFactory

object Taskmanager {

  def start(port: Int) = {
    val config = ConfigFactory.parseString(
      s"""
         |  akka {
         |  actor {
         |    provider = "akka.cluster.ClusterActorRefProvider"
         |  }
         |  remote {
         |    log-remote-lifecycle-events = off
         |    netty.tcp {
         |      hostname = ${Constants.hostname}
         |      port = $port
         |    }
         |  }
         |  cluster {
         |    seed-nodes = [
         |     "akka.tcp://${Constants.clusterName}@${Constants.hostname}:${Constants.clusterRpcPort}",
         |     "akka.tcp://${Constants.clusterName}@${Constants.hostname}:${Constants.jobmanagerRpcPort}"]
         |    roles = [${Constants.taskmanagerName}]
         |    auto-down-unreachable-after = 15s
         |    }
         |}
         |""".stripMargin)
      .withFallback(ConfigFactory.load())
    val system = ActorSystem(Constants.clusterName, config)
    val tm = system.actorOf(Props[TaskmanagerBackend], name = Constants.taskmanagerName)
    tm
  }
}
