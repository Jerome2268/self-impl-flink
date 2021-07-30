package com.dorr.flink.function.process.operators

import com.dorr.flink.common.entity.StreamRecord
import com.dorr.flink.function.process.{ProcessContext, ProcessFunction}

class Sum extends ProcessFunction{
  override def processElement[In, Out](streamRecord: StreamRecord[In], ctx: ProcessContext[Out]): Unit = ???
}
