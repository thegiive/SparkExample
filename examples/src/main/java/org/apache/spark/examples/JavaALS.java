package org.apache.spark.examples;

import breeze.linalg.product;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.recommendation.*;
import org.apache.spark.rdd.RDD;
import org.jblas.DoubleMatrix;
import scala.Array;
import scala.Tuple2;
import scala.tools.nsc.transform.patmat.MatchAnalysis;

import java.util.*;

public class JavaALS {

    public static RDD getModel(JavaSparkContext sc , String model_file_name){
        JavaRDD<Tuple2<Integer,String>> rdd1 = sc.objectFile(model_file_name);
        JavaPairRDD<Object , double[]> rdd2 = rdd1.mapToPair(s -> {
            Integer user_id = s._1();
            String[] arr = s._2().split(",");
            double[] result = new double[arr.length];
            for( int i = 0 ; i < arr.length ; i++ ) {
                result[i] = Double.parseDouble(arr[i]) ;
            }
            return new Tuple2(user_id , result);
        });
        return rdd2.rdd() ;
    }

    public static void writeRDDtoFile(JavaRDD<Tuple2<Object, double[]>> rdd , String file_name){
        rdd.mapToPair(s -> {
            Integer user_id = (Integer) s._1();
            double[] vector = s._2();
            String result = "";
            for (double d : vector) result += d + ",";
            return new Tuple2(user_id, result);
        }).saveAsObjectFile(file_name);
    }

    public static Rating[] sort(Rating[] arr , Rating input){

        double min_value = arr[0].rating() ; int min_index = 0 ;
        for( int i = 1 ; i < arr.length ; i++){
            if(arr[i].rating() < min_value){
                min_value = arr[i].rating() ;
                min_index = i ;
            }
        }
        if( min_value <= input.rating()){
            arr[min_index] = input;
        }
        return arr;
    }

    public static void main(String[] args) {

        if (args.length < 6) {
            System.err.println("Usage: JavaALS <InputFile> <Rank> <Iter> <Lambda> <NumPartintion> <OutputFileLocation>");
            System.exit(1);
        }

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("JavaALS"));

        JavaRDD<String> lines = sc.textFile(args[0]);
        int rank = Integer.parseInt(args[1]) ;
        int iter = Integer.parseInt(args[2]) ;
        double lambda = Double.parseDouble(args[3]) ;
        int numPartitions = Integer.parseInt(args[4]) ;
        String outputFilePrefix = args[5];

        MatrixFactorizationModel model =
           new MatrixFactorizationModel(rank , getModel(sc,outputFilePrefix+"user.txt") , getModel(sc,outputFilePrefix+"product.txt")) ;

        ArrayList<Integer> product_arr = (ArrayList) lines.map(l ->{
            String[] arr = l.split(",");
            return Integer.parseInt(arr[1]);
        }).distinct().collect();

        Broadcast bb = sc.broadcast(product_arr) ;

        JavaPairRDD<Integer, double[]> product_feature = model.productFeatures().toJavaRDD().mapToPair(t -> new Tuple2(t._1(), t._2())).cache();
        JavaPairRDD<Integer, double[]> user_feature = model.userFeatures().toJavaRDD().mapToPair(t -> new Tuple2(t._1(), t._2())).cache();

        HashMap<Integer , double[]> hm = new HashMap();
        product_feature.collect().forEach(p -> hm.put(p._1() , p._2() ));
        Broadcast bbb = sc.broadcast(hm);
        for(int i = 0 ; i < 1 ; i ++ ) {
            final int start = 500000 + (i*1000 );
            final int end = 500000 + ((i+1)*1000 );

            JavaPairRDD<Integer, double[]> users_part_rdd = user_feature.filter(t -> t._1() >= start && t._1() < end).repartition(numPartitions);

            JavaPairRDD<Integer , String> predict = users_part_rdd.mapToPair(t -> {
                Integer user = t._1();
                double[] uFeatures = t._2();
                HashMap<Integer, double[]> tmp_hm = (HashMap) bbb.value();

                Rating[] tarr = new Rating[5];
                for(int j = 0 ; j <5 ; j++) tarr[j] = new Rating(user , 0 , 0) ;


                for (Integer product : (ArrayList<Integer>) bb.value()) {
                    double r = 0.0;
                    try {
                        double[] pFeatures = tmp_hm.get(product);
                        DoubleMatrix userVector = new DoubleMatrix(uFeatures);
                        DoubleMatrix productVector = new DoubleMatrix(pFeatures);
                        r = userVector.dot(productVector);

                        tarr = sort(tarr, new Rating(user , product , r));

                    } catch (Exception e) {
//                    System.err.println("###"+product);
                    }
                }


                ArrayList<Rating> tarr_list = new ArrayList(Arrays.asList(tarr));
                tarr_list.sort((r1,r2) -> r1.rating() < r2.rating() ? 1 : -1 );
                return new Tuple2(user , tarr_list.toString() );
            });

            System.out.println(predict.take(10));
//            System.out.println(predict.count());
        }





        // Load model from text file
//        MatrixFactorizationModel model =
//           new MatrixFactorizationModel(rank , getModel(sc,outputFilePrefix+"user.txt") , getModel(sc,outputFilePrefix+"product.txt")) ;
//        System.out.println(model.predict(1 ,5072402));
//        System.out.println(model.predict(2,4129220));


//        // Save mode as text file
//        JavaPairRDD<Long, Rating> ratings = lines.mapToPair(line -> {
//            String[] arr = line.split(",");
//            String user = arr[0] , product = arr[1] , rating = arr[2] , time = arr[3];
//            Rating tmprat = new Rating(Integer.parseInt(user), Integer.parseInt(product), Double.parseDouble(rating));
//            return new Tuple2( Long.parseLong(time) % 10, tmprat);
//        });
//
//
//        JavaRDD<Rating> training = ratings.filter( x -> x._1() < 8).values().repartition(numPartitions).cache();
//
//        MatrixFactorizationModel model = ALS.train(training.rdd(), rank, iter , lambda);
//        writeRDDtoFile(model.userFeatures().toJavaRDD() , outputFilePrefix+"user.txt");
//        writeRDDtoFile(model.productFeatures().toJavaRDD() , outputFilePrefix+"product.txt");
//        // Save mode as text file END


        sc.stop();

    }

}
