package com.sc.flink.sensor

import java.io.File
import java.text.SimpleDateFormat
import java.util

import com.sc.flink.mysql2flink.mysql2flink.jdbcRead
import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.CloseableRegistry
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.runtime.execution.Environment
import org.apache.flink.runtime.query.TaskKvStateRegistry
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.ttl.TtlTimeProvider
import org.apache.flink.runtime.state.{AbstractKeyedStateBackend, CheckpointStorage, CompletedCheckpointStorageLocation, KeyGroupRange, KeyedStateHandle, OperatorStateBackend, OperatorStateHandle}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.Clock
import org.apache.flink.streaming.connectors.fs.bucketing.{Bucketer, BucketingSink}
import org.apache.flink.types.Row
import org.apache.hadoop.fs.Path

import scala.util.Random


object SourceTest {
  val hdfs_path = "hdfs://centos01:9000/data_test"
  val file_path = "F:\\data_test"

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val env = ExecutionEnvironment.getExecutionEnvironment


    streamEnv.setParallelism(1)

    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.enableCheckpointing(1000)
    streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    streamEnv.setStateBackend(new FsStateBackend("hdfs://centos01:9000/flink/checkpoints"))
    streamEnv.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)
    val list = new util.ArrayList[String]()

    val stdata = streamEnv.addSource(new MySensorSource())


        val filesink = new BucketingSink[String](file_path)


        val hadoopSink = new BucketingSink[SensorReading](hdfs_path)
        //val hadoopSink = new BucketingSink[String]("E:\\vir_env")
        // 使用东八区时间格式"yyyy-MM-dd--HH"命名存储区
        //hadoopSink.setBucketer(new DateTimeBucketer[String]("yyyyMMdd", ZoneId.of("Asia/Shanghai")))
        hadoopSink.setBucketer(new Bucketer[SensorReading] {
          override def getBucketPath(clock: Clock, basePath: Path, element: SensorReading): Path = {
            // yyyyMMdd
            val fm = new SimpleDateFormat("yyyyMMdd")
            val day = fm.format(element.timestamp)

            print(basePath + "/" + "dt=" + day)
            new Path(basePath + "/dt=" + day)
          }
        })
          .setBatchRolloverInterval(1 * 60 * 1000L)
          .setPartSuffix(".log")


        stdata.addSink(hadoopSink)

    streamEnv.execute("chuanganqi")
  }

  def jdbcRead(env: ExecutionEnvironment) = {
    val inputMysql: DataSet[Row] = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
      //    .指定驱动名称
      .setDrivername("com.mysql.jdbc.Driver")
      //      url
      .setDBUrl("jdbc:mysql://localhost:3306/world")
      .setUsername("root")
      .setPassword("123456")
      .setQuery("select id,name from city")
      .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
      .finish()
    )
    inputMysql
  }

  class MySensorSource extends SourceFunction[SensorReading] {

    var running = true

    override def cancel(): Unit = running = false

    override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
      val rand = new Random()

      //初始化传感器温度10个
      var curTemps = 1.to(10).map(
        i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
      )
      //无线循环，生成变化温度
      while (running) {
        curTemps = curTemps.map(
          data => (data._1, data._2 + rand.nextGaussian())
        )
        val curTs = System.currentTimeMillis()
        curTemps.foreach(
          data => ctx.collect(SensorReading(data._1, curTs, data._2))
        )
        Thread.sleep(2000L)
      }


    }


  }


}
