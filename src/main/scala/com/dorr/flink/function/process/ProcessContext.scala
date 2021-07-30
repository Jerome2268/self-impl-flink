package com.dorr.flink.function.process

import com.dorr.flink.common.entity.StreamRecord
import com.dorr.flink.function.source.FlinkDorrContext

import scala.collection.mutable

class ProcessContext[T] extends FlinkDorrContext {
  val queue = new mutable.Queue[StreamRecord[T]]

  def collect(t: T) = queue.enqueue(StreamRecord[T](t))

}
