package com.dorr.flink.common.variable

object Constants {
  val topic = "flink-akka-dorr-demo"
  val jobmanagerRootPath = "/user/jobmanager";
  val clusterRootPath = "/user/cluster";
  val jobmanagerRpcPort = 6058;
  val taskmanagerStartPort = 6000;
  val clusterRpcPort = 6059;
  val parallelism = 5;
  val maxRetryCount = 20;
  val hostname = "localhost"
  val jobmanagerName = "jobmanager"
  val taskmanagerName = "taskmanager"
  val clusterName = "flink"
  val OutputPath = "/opt/flink/dorr"
  val kafkaConnect = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
  val zookeeperConnect = "hadoop102:2181,hadoop103:2181,hadoop104:2181"
  val jobmanagerZkMetaPath = "/dorr/flink/jobmanager/path"
  val clusterZkMetaPath = "/dorr/flink/cluster/path"
}
