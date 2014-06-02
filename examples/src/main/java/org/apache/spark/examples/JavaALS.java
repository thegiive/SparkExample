package org.apache.spark.examples;

import breeze.linalg.product;
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

//        JavaRDD<Integer> users_rdd  = lines.map(l ->{
//            String[] arr = l.split(",");
//            return Integer.parseInt(arr[0]);
//        }).distinct().cache();

        JavaPairRDD<Integer, double[]> product_feature = model.productFeatures().toJavaRDD().mapToPair(t -> new Tuple2(t._1(), t._2())).cache();
        JavaPairRDD<Integer, double[]> user_feature = model.userFeatures().toJavaRDD().mapToPair(t -> new Tuple2(t._1(), t._2())).cache();

        HashMap<Integer , double[]> hm = new HashMap();
        product_feature.collect().forEach(p -> hm.put(p._1() , p._2() ));
        Broadcast bbb = sc.broadcast(hm);
        for(int i = 0 ; i < 10 ; i ++ ) {
            final int start = 500000 + (i*10000 );
            final int end = 500000 + ((i+1)*10000 );

            JavaPairRDD<Integer, double[]> users_part_rdd = user_feature.filter(t -> t._1() >= start && t._1() < end).repartition(numPartitions);

            JavaRDD<Rating> predict = users_part_rdd.map(t -> {
                Integer user = t._1();
                double[] uFeatures = t._2();
                HashMap<Integer, double[]> tmp_hm = (HashMap) bbb.value();
                double max_r = 0;
                Integer max_p = 0;

                for (Integer product : (ArrayList<Integer>) bb.value()) {
                    double r = 0.0;
                    try {
                        double[] pFeatures = tmp_hm.get(product);
                        DoubleMatrix userVector = new DoubleMatrix(uFeatures);
                        DoubleMatrix productVector = new DoubleMatrix(pFeatures);
                        r = userVector.dot(productVector);
                        if (r > max_r) {
                            max_r = r;
                            max_p = product;
                        }
                    } catch (Exception e) {
//                    System.err.println("###"+product);
                    }
                }
                return new Rating(user, max_p, max_r);
            });

            System.out.println(predict.count());
//            System.out.println(predict.take(10));

        }

//            JavaPairRDD<Integer, Object> user_product = users_part_rdd.flatMapToPair(user -> {
//                ArrayList<Tuple2<Integer, Object>> result = new ArrayList();
//                ArrayList<Integer> cc = (ArrayList<Integer>) bb.value();
//                for (Integer p : cc) {
//                    result.add(new Tuple2(user, p));
//                }
//                return result;
//            });


//        val users = userFeatures.join(usersProducts).map{
//            case (user, (uFeatures, product)) => (product, (user, uFeatures))
//        }
//        users.join(productFeatures).map {
//            case (product, ((user, uFeatures), pFeatures)) =>
//                val userVector = new DoubleMatrix(uFeatures)
//                val productVector = new DoubleMatrix(pFeatures)
//                Rating(user, product, userVector.dot(productVector))
//        }


//        JavaRDD<Rating> predict = user_product.join(user_feature).map(t -> {
//           Integer user = t._1() ;
//            double[] uFeatures = t._2()._2();
//            Integer product = (Integer)t._2()._1();
//            HashMap<Integer, double[]> tmp_hm = (HashMap) bbb.value() ;
//            DoubleMatrix userVector = new DoubleMatrix(uFeatures);
//            double r = 0.0;
//            try {
//                DoubleMatrix productVector = new DoubleMatrix(tmp_hm.get(product));
//                r = userVector.dot(productVector);
//            }catch(Exception e){
//                System.err.println("###"+product);
//            }
//            return new Rating(user, product, r);
//
////            return new Rating(user, product, (double)tmp_hm.get(product)[0] );
//        });

//        System.out.println("#####"+Arrays.toString(product_feature.lookup(5072402).get(0)));


//            JavaPairRDD<Integer, Tuple2<Integer, double[]>> tmp_users_rdd = user_product.join(user_feature).mapToPair(t -> {
//                Integer user = t._1();
//                double[] uFeatures = t._2()._2();
//                Integer product = (Integer) t._2()._1();
//                return new Tuple2(product, new Tuple2(user, uFeatures));
//            });
//
//            JavaRDD<Rating> predict = tmp_users_rdd.join(product_feature).map(t -> {
//                Integer product = t._1();
//                Integer user = t._2()._1()._1();
//                double[] uFeatures = t._2()._1()._2();
//                double[] pFeatures = t._2()._2();
//                DoubleMatrix userVector = new DoubleMatrix(uFeatures);
//                DoubleMatrix productVector = new DoubleMatrix(pFeatures);
//                return new Rating(user, product, userVector.dot(productVector));
//            });
//
//        JavaRDD<Rating> result = model.predict(user_product.rdd()).toJavaRDD();
//




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
