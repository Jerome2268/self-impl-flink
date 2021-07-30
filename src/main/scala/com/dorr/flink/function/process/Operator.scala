package com.dorr.flink.function.process

import com.dorr.flink.common.entity.StreamRecord

trait Operator {
  def open(): Unit

  def processElement[In, Out](streamRecord: StreamRecord[In], ctx: ProcessContext[Out])

  def close(): Unit
}

trait UDFOperator extends Operator

trait UDTFOperator extends Operator

trait UDAFOperator extends Operator
