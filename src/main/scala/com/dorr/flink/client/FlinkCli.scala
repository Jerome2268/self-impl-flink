package com.dorr.flink.client

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.cluster.client.ClusterClientReceptionist
import com.dorr.flink.common.variable.Constants
import com.typesafe.config.ConfigFactory
import com.dorr.flink.common.entity.{ClusterDown, ClusterUp}
import com.dorr.flink.deploy.MiniCluster

object FlinkCli {
  def get(port: Int): ActorRef = {
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
         |                port = ${port}
         |              }
         |            }
         |        cluster {
         |          seed-nodes = [
         |            "akka.tcp://${Constants.clusterName}@${Constants.hostname}:${port}"]
         |          roles = [${Constants.clusterName}]
         |          auto-down-unreachable-after = 10s
         |        }
         |        extensions = ["akka.cluster.client.ClusterClientReceptionist"]
         |      }
         |""".stripMargin)
      .withFallback(ConfigFactory.load())
    val system = ActorSystem(Constants.clusterName, config)
    val cluster = system.actorOf(Props[MiniCluster], name = "cluster")
    ClusterClientReceptionist(system).registerService(cluster)
    cluster ! ClusterUp
    cluster
  }

}
