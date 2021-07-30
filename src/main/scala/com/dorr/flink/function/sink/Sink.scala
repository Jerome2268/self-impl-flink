package com.dorr.flink.function.sink

import com.dorr.flink.common.entity.StreamRecord

trait Sink {
  def sinkTo[T](streamRecord: StreamRecord[T]): Unit

  def open(): Unit

  def close(): Unit
}
