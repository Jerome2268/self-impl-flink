package com.dorr.flink.function.process.operators

import com.dorr.flink.common.entity.StreamRecord
import com.dorr.flink.function.process.{ProcessContext, ProcessFunction, UDFOperator}

class Map extends UDFOperator{
  override def processElement[In, Out](streamRecord: StreamRecord[In], ctx: ProcessContext[Out]): Unit = ???

  override def open(): Unit = ???

  override def close(): Unit = ???
}
