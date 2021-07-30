package com.dorr.example.wordcount

import com.dorr.flink.common.entity.StreamRecord
import com.dorr.flink.function.process.{Operator, ProcessContext}

class WordCountProcessFunction extends Operator{

  override def open(): Unit = ???

  override def close(): Unit = ???

  override def processElement[In, Out](streamRecord: StreamRecord[In], ctx: ProcessContext[Out]): Unit = ???
}
