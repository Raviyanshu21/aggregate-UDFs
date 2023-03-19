package com.knoldus.sparkUDF

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator


object MeanExample extends Aggregator[Double, (Double, Long), Double]{

  override def zero: (Double, Long) = (0.0, 0L)

  override def reduce(buffer: (Double, Long), input: Double): (Double, Long) = {
    (buffer._1 + input, buffer._2 + 1L)
  }

  override def merge(buffer1: (Double, Long), buffer2: (Double, Long)): (Double, Long) = {
    (buffer1._1 + buffer2._1, buffer1._2 + buffer2._2)
  }

  override def finish(buffer: (Double, Long)): Double = {
    buffer._1 / buffer._2.toDouble
  }

  override def bufferEncoder: Encoder[(Double, Long)] = Encoders.product[(Double, Long)]

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

}
