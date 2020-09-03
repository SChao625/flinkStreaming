package com.sc.flink.mysql2flink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.sc.flink.sensor.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
//自定义mysql_sink
class mysqlsink extends RichSinkFunction[SensorReading]{
  // 连接
  var conn: Connection = _
  // 插入语句
  var insertStmt: PreparedStatement = _
  // 更新语句
  var updateStmt: PreparedStatement = _

  // 生命周期开始，建立连接
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection(
      "jdbc:mysql://localhost:3306/flink_test", "root", "123456"
    )

    insertStmt = conn.prepareStatement(
      "INSERT INTO filter_id (id, time_name) VALUES (?, ?)"
    )

    updateStmt = conn.prepareStatement(
      "UPDATE temperatures SET temp = ? WHERE sensor = ?"
    )
  }

  // 执行sql语句
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()

    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  // 生命周期结束，清理工作
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }


}
