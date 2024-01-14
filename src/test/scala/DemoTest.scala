package com.hismalltree.demo.flink

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{ClassRule, Rule, Test}
import org.testcontainers.containers.MySQLContainer

class DemoTest extends BaseTestService:

  @Test
  def testIncrement(): Unit =
    val function = new IncrementMapFunction
    assertEquals(3L, function.map(2L))


  @Test
  def testIncrementPipeline(): Unit =
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sink = new CollectionSink[Long]
    env.setParallelism(2)
    env.fromElements(1L, 2L, 3L)
      .map(new IncrementMapFunction)
      .returns(TypeInformation.of(classOf[Long]))
      .addSink(sink)
    env.execute()

    assertTrue(sink.containsAll(2L, 3L, 4L))

object DemoTest:

  @ClassRule
  def mysql: MySQLContainer[_] = new MySQLContainer()

  @ClassRule
  def flinkCluster: MiniClusterWithClientResource =
    new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
        .setNumberSlotsPerTaskManager(2)
        .setNumberTaskManagers(1)
        .build()
    )

class IncrementMapFunction extends MapFunction[Long, Long] {

  override def map(value: Long): Long = value + 1

}
