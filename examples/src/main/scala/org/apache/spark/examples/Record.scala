package org.apache.spark.examples

import java.util.Date
import java.text.SimpleDateFormat

class Record(input: Array[String]) extends Serializable{
  val esid = input(0)
  val itemid = input(1)
  val timestamp = input(2)

  def secElapse = timestamp.toLong
  def dateStr = new SimpleDateFormat("yyyyMMdd").format(new Date((timestamp.toLong + 28800) * 1000))

  override def toString() = input.mkString(",")
}
