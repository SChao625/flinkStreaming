package com.sc.flink.flink2hive

import java.io.File
import java.text.SimpleDateFormat

import org.apache.flink.streaming.connectors.fs.Clock
import org.apache.flink.streaming.connectors.fs.bucketing.BasePathBucketer
import org.apache.hadoop.fs.Path



class DayBasePathBucketer  extends BasePathBucketer[String]{

  /**
   * 返回路径
   * @param clock
   * @param basePath
   * @param element
   * @return
   */
  override def getBucketPath(clock: Clock, basePath: Path, element: String): Path = {
    // yyyyMMdd
    val fm = new SimpleDateFormat("yyyyMMdd")
    val day = fm.format(element.split(" ")(1))
    print(basePath + File.separator + "dt=" + day)

    new Path(basePath + File.separator + "dt=" + day)

  }

}
