package com.dorr.flink.common.entity

trait Data

case class StreamRecord[T](data: T) extends Data

