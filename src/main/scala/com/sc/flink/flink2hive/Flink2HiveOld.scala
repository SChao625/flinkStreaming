package com.sc.flink.flink2hive

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.sc.flink.sensor.SensorReading
import com.sc.flink.sensor.SourceTest.MySensorSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.fs.Clock
import org.apache.flink.streaming.connectors.fs.bucketing.{Bucketer, BucketingSink}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector
import org.apache.hadoop.fs.Path

object Flink2HiveOld {
  val KAFKA_BROKERS = "centos01:9092,centos01:9093,centos01:9094"
  val TRANSACTION_GROUP = "sc"
  val TOPIC_NAME = "AdRealTimeLog1"
  val HDFS_URL1 = "hdfs://centos01:9000/data_test01"
  val HDFS_URL2 = "hdfs://centos01:9000/data_test02"
  val HDFS_CHECKPOINT = "hdfs://centos01:9000//flink/checkpoints"

  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(2)
    env.enableCheckpointing(10 * 1000)
    env.setStateBackend(new FsStateBackend(HDFS_CHECKPOINT))
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //kafka source
    //    val kafkaProps = new Properties()
    //    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKERS)
    //    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
    //    val dataStreamSource: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String](TOPIC_NAME,
    //      new SimpleStringSchema(), kafkaProps)).setParallelism(1)
    val tag: OutputTag[SensorReading] = new OutputTag[SensorReading]("freezing")
    //自定义source
    val dataStreamSource = env
      .addSource(new MySensorSource())
      //分流
      .process(new HotAlarm(tag))


    val dataStreamSource01 = dataStreamSource.map(i => i.id + "," + i.temperature + "," + i.timestamp)


    // 获得侧流数据
    val dataStreamSource02 = dataStreamSource.getSideOutput(tag).map(i => i.id + "," + i.temperature + "," + i.timestamp)

    //        dataStreamSource.print();
    dataStreamSource01.print("主流")
    dataStreamSource02.print("侧流")


    val hadoopSink = new BucketingSink[String](HDFS_URL1)
      .setBucketer(new Bucketer[String] {
        override def getBucketPath(clock: Clock, path: Path, t: String): Path = {
          val str = tranTimeToString(t.split(",")(2))

          new Path(path + "/" + "nc_date=" + str)
        }
      })

      //val hadoopSink = new BucketingSink[String]("E:\\vir_env")
      // 使用东八区时间格式"yyyy-MM-dd--HH"命名存储区
      //hadoopSink.setBucketer(new DateTimeBucketer[String]("yyyyMMdd", ZoneId.of("Asia/Shanghai")))
      .setBatchRolloverInterval(5 * 60 * 1000)
      .setPendingPrefix("pre")
      .setInProgressPrefix("doing")

    val hadoopSink02 = new BucketingSink[String](HDFS_URL2)
      .setBucketer(new Bucketer[String] {
        override def getBucketPath(clock: Clock, path: Path, t: String): Path = {
          val str = tranTimeToString(t.split(",")(2))

          new Path(path + "/" + "nc_date=" + str)
        }
      })

      //val hadoopSink = new BucketingSink[String]("E:\\vir_env")
      // 使用东八区时间格式"yyyy-MM-dd--HH"命名存储区
      //hadoopSink.setBucketer(new DateTimeBucketer[String]("yyyyMMdd", ZoneId.of("Asia/Shanghai")))
      .setBatchRolloverInterval(5 * 60 * 1000)
      .setPendingPrefix("pre")
      .setInProgressPrefix("doing")
      .setPartSuffix("doing")

    dataStreamSource01.addSink(hadoopSink)
    dataStreamSource02.addSink(hadoopSink02)

    env.execute("Flink add kafka data source")

  }

  //转换时间戳
  def tranTimeToString(tm: String): String = {
    val fm = new SimpleDateFormat("yyyyMMdd")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }

  //分流操作
  class HotAlarm(alarmOutPutStream: OutputTag[SensorReading]) extends ProcessFunction[SensorReading, SensorReading] {
    override def processElement(sensor: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
      if (sensor.temperature > 60.5) {
        context.output(alarmOutPutStream, sensor)
      } else {
        collector.collect(sensor)
      }
    }
  }

}
