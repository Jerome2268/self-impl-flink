package com.dorr.flink.common.utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

object AkkaZkSerializer {
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream() //内存输出流，和磁盘输出流从操作上讲是一样的
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }


  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }


  def serialize_file[T](path: String, o: T) = {
    val bos = new FileOutputStream(path)
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
  }


  def deserialize_file[T](file: String): T = {
    val bis = new FileInputStream(file)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }

  def deepCopyObject[T](o: T): T = {
    val bos = new ByteArrayOutputStream
    val objs = new ObjectOutputStream(bos)
    objs.writeObject(o)
    val bis = new ByteArrayInputStream(bos.toByteArray)
    new ObjectInputStream(bis).readObject().asInstanceOf[T]
  }

}
