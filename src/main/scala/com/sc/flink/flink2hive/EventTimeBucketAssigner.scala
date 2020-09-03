package com.sc.flink.flink2hive

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat

import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner

class EventTimeBucketAssigner extends BucketAssigner[String, String] {
  /**
   * bucketId is the output path
   *
   * @param element
   * @param context
   * @return
   */
  override def getBucketId(element: String, context: BucketAssigner.Context): String = {
    val fm = new SimpleDateFormat("yyyyMMdd")
    val day = fm.format(element.split(" ")(0).toLong)
    val dt = "dt=" + day
    dt
  }

  override def getSerializer: SimpleVersionedSerializer[String] = {
    StringSerializer
  }
  object StringSerializer extends SimpleVersionedSerializer[String] {
    val VERSION = 77

    override def getVersion = 77

    @throws[IOException]
    override def serialize(checkpointData: String): Array[Byte] = checkpointData.getBytes(StandardCharsets.UTF_8)

    @throws[IOException]
    override def deserialize(version: Int, serialized: Array[Byte]): String = if (version != 77) throw new IOException("version mismatch")
    else new String(serialized, StandardCharsets.UTF_8)
  }

}