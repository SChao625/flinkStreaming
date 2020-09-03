package com.sc.flink.mysql2flink

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object mysql2flink {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputMysql: DataSet[Row] = jdbcRead(env)

    inputMysql.print()

  }
  def jdbcRead(env: ExecutionEnvironment) ={
    val inputMysql = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
      //    .指定驱动名称
      .setDrivername("com.mysql.jdbc.Driver")
      //      url
      .setDBUrl("jdbc:mysql://localhost:3306/flink_test")
      .setUsername("root")
      .setPassword("123456")
      .setQuery("select id,time_name from filter_id")
      .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
      .finish()
    )
    inputMysql
  }
}
