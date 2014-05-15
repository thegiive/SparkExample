package org.apache.spark.examples

//#based on https://github.com/amplab/training/blob/ampcamp4/machine-learning/scala/solution/MovieLensALS.scala

import java.util.Random

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
//import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
//import org.apache.spark.mllib.recommendation.MatrixFactorizationModel



object CF1 {

  def main(args: Array[String]) {
      // .setAppName("ALS").set("spark.executor.memory", "90g")

    val conf = new SparkConf().setMaster(args(0))
      .setAppName("ALS")
      .set("spark.executor.memory", "90g")
      .set("spark.storage.memoryFraction" , "0.2")
      .set("spark.shuffle.memoryFraction" , "0.3")      
      .set("spark.default.parallelism" , args(4))
      .setJars( SparkContext.jarOfClass(this.getClass) ).setSparkHome( System.getenv("SPARK_HOME") )
    val sc = new SparkContext(conf)

    val ratings = sc.textFile(args(1)).map { line =>
      val fields = line.split(",")
      // format: (timestamp % 10, Rating(userId, movieId, rating))
      (fields(3).toLong % 10, Rating(fields(0).toInt , fields(1).toInt, fields(2).toDouble))
    }


    // val movies = sc.textFile(args(2)).map { line =>
    //   val fields = line.split(",")
    //   // format: (movieId, movieName)
    //   (fields(0).toInt, fields(2))
    // }.collect.toMap

    val numPartitions = 50
    val training = ratings.filter(x => x._1 < 6)
      .values
      .repartition(numPartitions)
      .persist
    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8)
      .values
      .repartition(numPartitions)
      .persist
    val test = ratings.filter(x => x._1 >= 8).values.persist

    // train models and evaluate them on the validation set
//    val ranks = List(100, 200)
//    val lambdas = List(0.1, 10.0)
//    val numIters = List(10, 20)
        val ranks = List(args(2).toInt)
        val lambdas = List(10.0)
        val numIters = List(args(3).toInt)

    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      println("Ranks is "+rank+" and inter is "+numIter)
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation, validation.count())
      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }
    val myRatings = ratings.filter{ case( t , r ) =>  r.user == 823519 }.map{ case ( k , v ) => (v.user , v.product) }
    val recommendations = bestModel.get
      .predict(myRatings)
      .collect
      .sortBy(- _.rating)
      .take(50)

    // var i = 1
    // println("Movies recommended for you:")
    // recommendations.foreach { r =>
    //   println("%2d".format(i) + ": " + movies(r.product))
    //   i += 1
    // }

    // clean up

    sc.stop();
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long) = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }


}