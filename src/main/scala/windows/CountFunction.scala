package com.hismalltree.demo.flink
package windows

import entity.SensorReading

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang

class CountFunction
  extends ProcessWindowFunction[SensorReading, (String, Long, Long, Int), String, TimeWindow] {

  override def process
  (
    key: String,
    context: ProcessWindowFunction[SensorReading, (String, Long, Long, Int), String, TimeWindow]#Context,
    elements: lang.Iterable[SensorReading],
    out: Collector[(String, Long, Long, Int)]
  ): Unit = {
    // count readings
    val cnt = key.count(_ => true)
    // get current watermark
    val evalTime = context.currentWatermark
    // emit result
    out.collect((key, context.window.getEnd, evalTime, cnt))
  }
}
