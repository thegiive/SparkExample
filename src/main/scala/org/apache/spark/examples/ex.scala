package org.apache.spark.examples
/*** SimpleApp.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object ex {
  def main(args: Array[String]) {
    val logFile = "/tmp/README.md" // Should be some file on your system
    val sc = new SparkContext("spark://a-grid-s2.corp.sg3.yahoo.com:5050", "Simple App", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
