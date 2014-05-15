package org.apache.spark.examples
/*** SimpleApp.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._

object wc {
  def main(args: Array[String]) {
    val logFile = args(1)
    val conf = new SparkConf().setMaster(args(0)).setAppName("Simple App").set("spark.executor.memory", "6g").setJars( SparkContext.jarOfClass(this.getClass) ).setSparkHome( System.getenv("SPARK_HOME") )
    val sc = new SparkContext(conf)
    val file = sc.textFile(logFile)
    val counts = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    println( counts.count() ) ;

  }
}
