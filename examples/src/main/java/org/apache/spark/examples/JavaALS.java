package org.apache.spark.examples;

import breeze.linalg.product;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
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

    public static List findTop10(MatrixFactorizationModel model, JavaSparkContext sc , int user_id , int total_product){
        List<Tuple2<Integer , Integer>> arr = new ArrayList<>();
        for(int i = 1 ; i <= total_product ; i++){
            arr.add( new Tuple2( user_id, i));
        }
        JavaRDD testdata = sc.parallelize(arr);
        JavaRDD<Rating> prediction = model.predict(testdata.rdd()).toJavaRDD();
        return prediction.mapToPair(p -> new Tuple2(p.rating() , p.product() )).sortByKey(false).take(10) ;
    }


    public static void main(String[] args) {

        if (args.length < 4) {
            System.err.println("Usage: JavaALS <file> <rank> <lambda> <numpartintion>");
            System.exit(1);
        }

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("JavaALS"));

        JavaRDD<String> lines = sc.textFile(args[0]);
        int rank = Integer.parseInt(args[1]) ;
        double lambda = Double.parseDouble(args[2]) ;
        int numPartitions = Integer.parseInt(args[3]) ;


        JavaPairRDD<Long, Rating> ratings = lines.mapToPair(line -> {
            String[] arr = line.split(",");
            String user = arr[0] , product = arr[1] , rating = arr[2] , time = arr[3];
            Rating tmprat = new Rating(Integer.parseInt(user), Integer.parseInt(product), Double.parseDouble(rating));
            return new Tuple2( Long.parseLong(time) % 10, tmprat);
        });



        // Seperate the data
        JavaRDD<Rating> training = ratings.filter( x -> x._1() < 6).values().repartition(numPartitions).cache();
        JavaRDD<Rating> validation = ratings.filter( x -> x._1() >= 6 && x._1() < 8).values().repartition(numPartitions).cache();
        JavaRDD<Rating> test = ratings.filter( x -> x._1() >= 8).values().cache();

        System.out.println("Start Iteration");

        Double minRmse = 100.0;
        int bestNumIter = 0 ;

        for( int numIter = 3 ; numIter <= 30 ; numIter ++ ) {
            MatrixFactorizationModel model = ALS.train(training.rdd(), rank, numIter, lambda);
            Double validationRmse = computeRmse(model, validation, validation.count());
            System.out.println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
                    + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".");
            if( minRmse < validationRmse ){
                System.out.println("Stop!!!");
                System.out.println("RMSE (validation) = " + validationRmse + " previous RMSE is "+minRmse+"for the model trained with rank = "
                        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".");
                bestNumIter = numIter - 1 ;

                break ;
            }else{
                minRmse = validationRmse;
            }
        }

        System.out.println("Best Setting is iter="+bestNumIter+" lambda="+lambda);

        sc.stop();

    }
}
