package com.hismalltree.demo.flink
package windows

import entity.SensorReading

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.{Collector, OutputTag}

import java.lang
import java.sql.Timestamp
import java.time.Instant

class LateDataDemo {


}

object LateDataDemo {

  private val lateReadingsOutput: OutputTag[SensorReading] =
    new OutputTag[SensorReading]("late-readings", TypeInformation.of(classOf[SensorReading]))

  def redirectLateReadingsOutput(): Unit = {
    val readings: DataStream[SensorReading] = StreamExecutionEnvironment.getExecutionEnvironment().fromElements(
      SensorReading("001", Instant.now().getEpochSecond, 24)
    )
    val countPer10Sec: SingleOutputStreamOperator[(String, Long, Long, Int)] = readings
      .keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .sideOutputLateData(lateReadingsOutput)
      .process(new CountFunction())

    val lateStream: DataStream[SensorReading] = countPer10Sec
      .getSideOutput(lateReadingsOutput)
  }

  def postLateReadingsOutput(): Unit = {
    val readings: DataStream[SensorReading] = ???
    val countPer10Sec = readings
      .keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .allowedLateness(Time.seconds(5))
      .process(new UpdatingWindowCountFunction)
  }


  def main(args: Array[String]): Unit = {

  }

}

class UpdatingWindowCountFunction
  extends ProcessWindowFunction[SensorReading, (String, Long, Int, String), String, TimeWindow] {

  override def process
  (
    key: String,
    context: ProcessWindowFunction[SensorReading, (String, Long, Int, String), String, TimeWindow]#Context,
    elements: lang.Iterable[SensorReading],
    out: Collector[(String, Long, Int, String)]
  ): Unit = {
    val cnt = key.count(_ => true)
    val isUpdate = context.windowState().getState(
      new ValueStateDescriptor[Boolean]("isUpdate", TypeInformation.of(classOf[Boolean]))
    )

    if (!isUpdate.value()) {
      out.collect((key, context.window.getEnd, cnt, "first"))
      isUpdate.update(true)
    } else {
      // 非首次计算，发出更新
      out.collect((key, context.window.getEnd, cnt, "update"))
    }
  }

}






