package com.hismalltree.demo.flink

import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.guava31.com.google.common.collect.Lists
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.util
import scala.annotation.varargs

class CollectionSink[IN] extends RichSinkFunction[IN]:

  override def invoke(value: IN, context: SinkFunction.Context): Unit =
    CollectionSink.collection.add(value)

  @varargs def containsAll(values: IN*): Boolean =
    CollectionSink.collection.containsAll(Lists.newArrayList(values:_*))

object CollectionSink:

  val collection: util.Collection[Any] = new util.ArrayList[Any]()
