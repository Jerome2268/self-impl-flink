package com.dorr.flink.function.source

abstract class Source extends Serializable {
  def collect[T](ctx: SourceContext[T])

  def open(): Unit

  def close(): Unit
}
