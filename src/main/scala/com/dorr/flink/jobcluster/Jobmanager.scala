package com.dorr.flink.jobcluster

import akka.actor.{ActorSystem, Props}
import akka.cluster.client.ClusterClientReceptionist
import com.dorr.flink.common.variable.Constants
import com.typesafe.config.ConfigFactory

object Jobmanager {

  def start(port: Int): Boolean = {
    val config = ConfigFactory.parseString(
      s"""
         | akka {
         |            actor {
         |              provider = "akka.cluster.ClusterActorRefProvider"
         |            }
         |            remote {
         |              log-remote-lifecycle-events = off
         |              netty.tcp {
         |                hostname = "${Constants.hostname}"
         |                port = $port
         |              }
         |            }
         |        cluster {
         |          seed-nodes = [
         |            "akka.tcp://${Constants.clusterName}@${Constants.hostname}:$port",
         |             "akka.tcp://${Constants.clusterName}@${Constants.hostname}:${Constants.clusterRpcPort}"]
         |
         |          roles = [${Constants.jobmanagerName}]
         |          auto-down-unreachable-after = 10s
         |        }
         |        extensions = ["akka.cluster.client.ClusterClientReceptionist"]
         |      }
         |""".stripMargin)
      .withFallback(ConfigFactory.load())
    val system = ActorSystem(Constants.clusterName, config)
    val jm = system.actorOf(Props[JobManagerBackend], name = Constants.jobmanagerName)
    ClusterClientReceptionist(system).registerService(jm)
    true
  }

}
