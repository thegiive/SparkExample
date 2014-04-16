package org.apache.spark.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import scala.math._

object Main {
  def main(args: Array[String]) {
    //val sc = new SparkContext("local","Spark Testing", "/opt/spark-0.7.3",
    //val sc = new SparkContext("spark://glarehair.corp.gq1.yahoo.com:7077", "CF on Spark", "/opt/spark-0.7.3", List("target/scala-2.9.3/cf-on-spark_2.9.3-0.1.jar"), Map(("SPARK_HOME", "/opt/spark-0.7.3")))
    val conf = new SparkConf().setMaster(args(0))
                    .setAppName("Simple App")
//                    .set("spark.executor.memory", "90g")
//                    .set("spark.default.parallelism", "20")
                    .setJars( SparkContext.jarOfClass(this.getClass) )
                    .setSparkHome( System.getenv("SPARK_HOME") )
    val sc = new SparkContext(conf)

    //val file = sc.textFile(args(0))
    //val file = sc.textFile("hdfs://quicktrick.corp.sg3.yahoo.com:8020/users/pishen/records-0804-0810-5M", 40)
    val file = sc.textFile(args(1))

    val rdd1 = file.map(line => new Record(line.split(",")))
    val rdd2 = rdd1.filter(record =>
      /*record.dateStr == "20130808" &&*/
      record.esid != "" &&
      record.itemid != ""
    )

    //println( rdd2.first ) ;

//  同一個 item ，被多少 user( esid ) 看過，當大於 10 才計算
    val count = rdd2.groupBy(_.itemid).mapValues(_.map(_.esid).distinct.length).filter(_._2 > 10)
    val lengths = count.mapValues(v => sqrt(v))
    lengths.cache()
    val rdd3 = rdd2.keyBy(_.itemid).join(lengths).map(_._2._1)

    val rdd4 = rdd3.groupBy(_.esid).values.flatMap(records => {
      records.map(_.itemid).distinct.combinations(2).flatMap(_.permutations).map(s => (s.head, s.last))
    })


    val rdd5 = rdd4.groupBy(p => p).mapValues(_.length).map(p => (p._1._1, p._1._2, p._2))

    val rdd6 = rdd5.keyBy(_._2).join(lengths).values

    val rdd7 = rdd6.map(p => (p._1._1, p._1._2, p._1._3 / p._2))

    val rdd8 = rdd7.groupBy(_._1).mapValues(_.sortWith((x, y) => x._3 > y._3).take(10).map(_._2))

    //println("top = "+ rdd8.count() )
    println("result = "+ rdd8.collect.mkString("\n"))
    println("result = "+ rdd8.count() )
  }
}
