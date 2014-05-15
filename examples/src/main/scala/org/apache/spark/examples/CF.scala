package org.apache.spark.examples

//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext._
//import scala.math._
//import org.apache.spark.mllib.recommendation.ALS
//import org.apache.spark.mllib.recommendation.Rating


object CF {
  def main(args: Array[String]) {

//
//  val conf = new SparkConf().setMaster(args(0))
//              .setAppName("Simple App").set("spark.executor.memory", "6g")
//              .setJars( SparkContext.jarOfClass(this.getClass) ).setSparkHome( System.getenv("SPARK_HOME") )
//  val sc = new SparkContext(conf)
//
//    // Load and parse the data
//    val data = sc.textFile(args(1))
//    val ratings = data.map(_.split(',') match {
//      case Array(user, item, rate) =>  Rating(user.toInt, item.toInt, rate.toDouble)
//    })
//
//    // Build the recommendation model using ALS
//    val numIterations = 20
//    val model = ALS.train(ratings, 1, 20, 0.01)
//
//    // Evaluate the model on rating data
//    val usersProducts = ratings.map{ case Rating(user, product, rate)  => (user, product)}
//    val predictions = model.predict(usersProducts).map{
//      case Rating(user, product, rate) => ((user, product), rate)
//    }
//    val ratesAndPreds = ratings.map{
//      case Rating(user, product, rate) => ((user, product), rate)
//    }.join(predictions)
//    val MSE = ratesAndPreds.map{
//      case ((user, product), (r1, r2)) =>  math.pow((r1- r2), 2)
//    }.reduce(_ + _)/ratesAndPreds.count
//    println("Mean Squared Error = " + MSE)
  }
}
