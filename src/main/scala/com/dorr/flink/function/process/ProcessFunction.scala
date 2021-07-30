package com.dorr.flink.function.process

abstract class ProcessFunction extends Operator {
  override def open(): Unit = {}

  override def close(): Unit = {}
}
