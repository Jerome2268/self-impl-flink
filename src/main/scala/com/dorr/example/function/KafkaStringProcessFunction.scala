package com.dorr.example.function

import com.dorr.flink.common.entity.StreamRecord
import com.dorr.flink.function.process.{ProcessContext, ProcessFunction}

class KafkaStringProcessFunction extends ProcessFunction {
  override def processElement[In, Out](streamRecord: StreamRecord[In], ctx: ProcessContext[Out]): Unit = {
    if (streamRecord != null &&
      streamRecord.data != null &&
      streamRecord.data.isInstanceOf[String] &&
      !streamRecord.data.asInstanceOf[String].stripLineEnd.equals("")) {
      val str = (streamRecord.data.asInstanceOf[String].stripLineEnd + "---- and dorr use processFunction handled this ---").asInstanceOf[Out]
      ctx.collect(str)
    }
  }


}
