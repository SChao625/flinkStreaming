package com.sc.flink.flink2hive

import java.util.Properties

import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.types.Row

object kafka2FlinkTable {
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
    val taEnv = StreamTableEnvironment.create(env)
    val kafkaProps = new Properties()

    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKERS)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
    kafkaProps.setProperty("zookeeper.connect", "centos01:2181")
    //连接kafka并转化为flinkTable
    taEnv.connect(
      new Kafka()
        .version("0.10")
        .topic(TOPIC_NAME)
        .properties(kafkaProps)
    ).withFormat(
      new Csv()
        .fieldDelimiter(' ') // optional: field delimiter character (',' by default)
        .lineDelimiter("\r\n") // optional: line delimiter ("\n" by default;
        //   otherwise "\r", "\r\n", or "" are allowed)
        .quoteCharacter('\'') // optional: quote character for enclosing field values ('"' by default)
    ).withSchema(
      new Schema()
        .field("timestamp", DataTypes.STRING())
        .field("record1", DataTypes.STRING())
        .field("record2",DataTypes.STRING())
        .field("record3", DataTypes.STRING())
        .field("record4", DataTypes.STRING())
    )
      .inAppendMode()
      .createTemporaryTable("time_records")

    val table = taEnv.sqlQuery("select * from time_records  ")
    table.printSchema()
    taEnv.toAppendStream[Row](table).print()


    val sink = new CsvTableSink("data/users.csv", ",", 1, FileSystem.WriteMode.NO_OVERWRITE);

//    tableEnv.registerTableSink("Result",
//      new String[] {
//        "userId"
//        , "name"
//        , "age"
//        , "sex"
//        , "createTime"
//      },
//      new TypeInformation[] {
//        Types.STRING
//        , Types.STRING
//        , Types.STRING
//        , Types.STRING
//        , Types.BIG_DEC
//      },
//      sink);
//
//    tableEnv.insertInto(table, "Result", new StreamQueryConfig());








    //    val hdfsSink = StreamingFileSink
    //      .forRowFormat(new Path(HDFS_URL), new SimpleStringEncoder[String]("UTF-8"))
    //      .withBucketAssigner(new EventTimeBucketAssigner)
    //      .build


    //
    env.execute("flink2hive")
  }

}
