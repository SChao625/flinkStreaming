package com.sc.flink.mysql2flink

import java.sql.DriverManager
import java.util
import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import com.sc.flink.sensor.SensorReading
import com.sc.flink.sensor.SourceTest.MySensorSource
import org.apache.flink.api.common.functions.{RichFilterFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig

object preloaddata {

  class SideFilterFunction extends RichFilterFunction[SensorReading] {


    private val Set = new util.HashSet[String]()

    override def open(parameters: Configuration): Unit = {

      super.open(parameters)

      val executors = Executors.newSingleThreadScheduledExecutor()

      executors.scheduleAtFixedRate(new Runnable {

        override def run(): Unit = loadData()

      }, 0, 1, TimeUnit.DAYS)

    }

    //初始化加载mysql数据
    def loadData() = {

      Class.forName("com.mysql.jdbc.Driver")

      val con = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink_test", "root", "123456")

      val sql = "select id from filter_id"

      val statement = con.prepareStatement(sql)

      val rs = statement.executeQuery()

      while (rs.next()) {

        val id = rs.getString("id")
        Set.add(id)

      }

      con.close()

    }

    override def filter(s: SensorReading): Boolean = {
      Set.contains(s.id)
    }

  }


  case class AdData(aId: Int, tId: Int, clientId: String, actionType: Int, time: Long)


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaConfig = new Properties();

    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "sc");

    val consumer = new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), kafkaConfig);

    val datast = env.addSource(new MySensorSource()).filter(new SideFilterFunction)

    datast.print()

    datast.addSink(new mysqlsink)

    env.execute()

  }

}
