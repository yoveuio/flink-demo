package com.hismalltree.demo.flink
package state

import entity.SensorReading

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.util.Collector

object StateDemo:

  def main(args: Array[String]): Unit =
    val sensorData: DataStream[SensorReading] = ???


class TemperatureAlertFunction(val threshold: Double)
  extends RichFlatMapFunction[SensorReading, (String, Double, Double)]:

  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit =
    val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
    lastTempState = getRuntimeContext.getState(lastTempDescriptor)

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit =
    val lastTemp = lastTempState.value()
    val tempDiff = (value.temperature - lastTemp).abs
    if (tempDiff > threshold)
      out.collect(value.id, value.temperature, tempDiff)
    this.lastTempState.update(value.temperature)
