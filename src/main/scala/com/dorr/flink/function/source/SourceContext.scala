package com.dorr.flink.function.source

import com.dorr.flink.common.entity.StreamRecord

import scala.collection.mutable

trait FlinkDorrContext extends Serializable

class SourceContext[T] extends FlinkDorrContext {
  val queue = new mutable.Queue[StreamRecord[T]]

  def collect(t: T) = queue.enqueue(StreamRecord[T](t))
}
