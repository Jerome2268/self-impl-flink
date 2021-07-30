package com.dorr.flink.taskmanager

import com.dorr.flink.common.utils.AkkaZkSerializer

object Test {
  def main(args: Array[String]): Unit = {

    val mumian = Students("mumian")
    val students = AkkaZkSerializer.deepCopyObject[Students](mumian)
    println(students.name)


  }

  case class Students(name:String) extends Serializable

}
