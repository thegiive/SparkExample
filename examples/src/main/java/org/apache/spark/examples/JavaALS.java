package org.apache.spark.examples;

import breeze.linalg.product;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.mllib.recommendation.*;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.tools.nsc.transform.patmat.MatchAnalysis;

import java.util.ArrayList;
import java.util.List;

public class JavaALS {

    /** Compute RMSE (Root Mean Squared Error). */
    public static double computeRmse(MatrixFactorizationModel model , JavaRDD<Rating> data, Long n){

        RDD userProduct = data.mapToPair(x -> new Tuple2(x.user(), x.product()) ).rdd();
        JavaRDD<Rating> predictions  = model.predict( userProduct ).toJavaRDD() ;

        JavaRDD<Tuple2<Double,Double>> predictionsAndRatings =
            predictions.mapToPair( x -> new Tuple2( new Tuple2(x.user() , x.product()) , x.rating() ) )
            .join(data.mapToPair(x -> new Tuple2( new Tuple2(x.user() , x.product()) , x.rating() ) ))
            .values();

        Double msr = predictionsAndRatings.mapToDouble(
                        x -> { double predict = x._1() , real = x._2() ;
                               return (( predict - real ) * ( predict - real)) ;
                        }).sum() / n;

        return Math.sqrt(msr);

    }


    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: JavaALS <file> <rank> <iters>");
            System.exit(1);
        }

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("JavaALS"));
        JavaRDD<String> lines = sc.textFile(args[0]);

        JavaPairRDD<Long, Rating> ratings = lines.mapToPair(line -> {
            String[] arr = line.split(",");
            String user = arr[0] , product = arr[1] , rating = arr[2] , time = arr[3];
            Rating tmprat = new Rating(Integer.parseInt(user), Integer.parseInt(product), Double.parseDouble(rating));
            return new Tuple2( Long.parseLong(time) % 10, tmprat);
        });


        int numPartitions = 50;

        JavaRDD<Rating> training = ratings.filter( x -> x._1() < 6).values().repartition(numPartitions).cache();
        JavaRDD<Rating> validation = ratings.filter( x -> x._1() >= 6 && x._1() < 8).values().repartition(numPartitions).cache();
        JavaRDD<Rating> test = ratings.filter( x -> x._1() >= 8).values().cache();

        int rank = Integer.parseInt(args[1]) ;
        double lambda = 10.0 ;
        int numIter = Integer.parseInt( args[2] ) ;

        System.out.println("Ranks is " + rank + " and inter is " + numIter);
        MatrixFactorizationModel model = ALS.train(training.rdd(), rank, numIter, lambda);

        Double validationRmse = computeRmse(model, validation, validation.count());
        System.out.println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
                + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".");


    }
}
