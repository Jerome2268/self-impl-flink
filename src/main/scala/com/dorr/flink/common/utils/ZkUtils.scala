package com.dorr.flink.common.utils

import com.dorr.flink.common.variable.Constants
import org.apache.log4j.Logger
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

object ZkUtils {
  val log = Logger.getLogger(this.getClass);
//  val zk = new ZooKeeper(Constants.zookeeperConnect, 20000, null)
    val zk = new ZooKeeper(Constants.zookeeperConnect, 20000, new Watcher {
      override def process(watchedEvent: WatchedEvent): Unit = {
        val path = watchedEvent.getPath
        val state = watchedEvent.getState
        val value = watchedEvent.getType
        log.info(s"path: $path. state: $state: value: $value")
      }
    })


  def save[T](path: String, data: T) = {

    if (zk.exists(path, false) == null) {
      zk.create(path, AkkaZkSerializer.serialize[T](data), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    } else {
      zk.setData(path, AkkaZkSerializer.serialize[T](data), -1)
    }

  }

  @throws[Exception]
  def getData[T](path: String): T = {
    if (zk.exists(path, false) != null) AkkaZkSerializer.deserialize[T](zk.getData(path, null, null))
    else throw new Exception(s"No data stored in $path")
  }


  case class Student(name: String)

}
