package com.dorr.flink.function.sink

import com.dorr.flink.common.entity.StreamRecord

class FileSink extends Sink {
  override def open(): Unit = {
  }

  override def close(): Unit = {}

  override def sinkTo[T](streamRecord: StreamRecord[T]): Unit = {

    if (streamRecord != null && streamRecord.data != null) {
      println(s"File sink >> ${streamRecord.data}")

    }
  }
}
