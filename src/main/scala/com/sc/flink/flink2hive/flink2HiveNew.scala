package com.sc.flink.flink2hive

import java.util.Properties

import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object flink2HiveNew {
  //通过StreamingFileSink把数据导入到对应hive分区中
  val KAFKA_BROKERS = "centos01:9092,centos01:9093,centos01:9094"
  val TRANSACTION_GROUP = "sc"
  val TOPIC_NAME = "AdRealTimeLog1"
  val HDFS_URL = "hdfs://centos01:8020/user/hive/warehouse/part_test"
  def main(args: Array[String]): Unit = {

//    val HDFS_URL = "E:\\vir_env"

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //定义kafka配置，生成对应Source
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKERS)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
    val dataStreamSource: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String](TOPIC_NAME,
      new SimpleStringSchema(), kafkaProps)).setParallelism(1)

    //sink到对应hdfs指定文件中
    val hdfsSink = StreamingFileSink
      .forRowFormat(new Path(HDFS_URL), new SimpleStringEncoder[String]("UTF-8"))
      .withBucketAssigner(new EventTimeBucketAssigner)
      .build


    dataStreamSource.addSink(hdfsSink)
    env.execute("flink2hive")
  }

}
