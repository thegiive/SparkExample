package org.apache.spark.examples;

import breeze.linalg.product;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.*;
import org.apache.spark.mllib.recommendation.*;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.tools.nsc.transform.patmat.MatchAnalysis;

import java.util.*;

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


        JavaPairRDD<Long, Rating> ratings = lines.mapToPair(line -> {
            String[] arr = line.split(",");
            String user = arr[0] , product = arr[1] , rating = arr[2] , time = arr[3];
            Rating tmprat = new Rating(Integer.parseInt(user), Integer.parseInt(product), Double.parseDouble(rating));
            return new Tuple2( Long.parseLong(time) % 10, tmprat);
        });



        // Seperate the data
        JavaRDD<Rating> training = ratings.filter( x -> x._1() < 6).values().repartition(numPartitions).cache();
        JavaRDD<Rating> validation = ratings.filter( x -> x._1() >= 6 && x._1() < 8).values().repartition(numPartitions).cache();
        JavaRDD<Rating> test = ratings.filter( x -> x._1() >= 8).values();
        JavaPairRDD<Integer,ArrayList<Integer>> testr = test.mapToPair( r -> new Tuple2(r.user() , r.product() )).groupByKey().cache();

//        System.out.println("Start Iteration");
//
//        Double minRmse = 100.0;
//        int bestNumIter = 0 ;
//
//        for( int numIter = 3 ; numIter <= 30 ; numIter ++ ) {
//            MatrixFactorizationModel model = ALS.train(training.rdd(), rank, numIter, lambda);
//            Double validationRmse = computeRmse(model, validation, validation.count());
//            System.out.println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
//                    + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".");
//            if( minRmse < validationRmse ){
//                System.out.println("Stop!!!");
//                System.out.println("RMSE (validation) = " + validationRmse + " previous RMSE is "+minRmse+"for the model trained with rank = "
//                        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".");
//                bestNumIter = numIter - 1 ;
//
//                break ;
//            }else{
//                minRmse = validationRmse;
//            }
//        }

//        System.out.println("Best Setting is iter="+bestNumIter+" lambda="+lambda);


        MatrixFactorizationModel bestModel = ALS.train(training.rdd(), rank, iter , lambda);
//        bestModel.userFeatures().toJavaRDD().mapToPair(s -> {
//            Integer user_id = (Integer)s._1(); double[] feature_array = s._2(); int i = 1 ;
//            ArrayList<Tuple2<Integer,Double>> result = new ArrayList();
//            for( double f : feature_array) result.add(new Tuple2(i++,f));
//            result.sort( (a, b) -> a._2() >= b._2() ? -1 : 1 );
//            return new Tuple2( user_id , result.subList(0,4));
//        }).saveAsTextFile(outputFilePrefix+"/user_feature.txt");

        JavaPairRDD<Integer, Tuple2<Double,Integer>> pf = bestModel.productFeatures().toJavaRDD().flatMapToPair(s->{
            Integer product_id = (Integer)s._1(); double[] feature_array = s._2(); int feature_id = 1 ;
            List result = new ArrayList();
            for( double f : feature_array) {
                result.add(new Tuple2( feature_id++ , new Tuple2(f, product_id) ));
            }
            return result ;
        });//.saveAsTextFile(outputFilePrefix+"/product_feature.txt");;
//        pf.groupByKey().
        JavaPairRDD<Integer , ArrayList<Tuple2>> ppf =  pf.groupByKey().mapValues(v -> {
            ArrayList<Tuple2<Double, Integer>> result = new ArrayList();
            v.forEach(t -> result.add(t));
            result.sort((a,b) -> a._1() >= b._1() ? -1 : 1 );
            return new ArrayList<Tuple2>(result.subList(0, 4));
        });//.saveAsTextFile(outputFilePrefix+"/product_feature.txt");

        HashMap<Integer, String> hm = new HashMap() ;
        List<Tuple2<Integer , ArrayList<Tuple2>>> ppf_list = ppf.collect();
        for( Tuple2 t : ppf_list ){
            String result ="";
              for( Tuple2 tt : (ArrayList<Tuple2>) t._2()){
                result += tt._1()+","+tt._2()+"#";
              }
            hm.put((Integer)t._1() , result );
        }

        JavaPairRDD<Integer, ArrayList<Tuple2>> uf = bestModel.userFeatures().toJavaRDD().mapToPair(s -> {
            Integer user_id = (Integer)s._1(); double[] feature_array = s._2(); int i = 1 ;
            ArrayList<Tuple2<Integer,Double>> result = new ArrayList();
            for( double f : feature_array) result.add(new Tuple2(i++,f));
            result.sort( (a, b) -> a._2() >= b._2() ? -1 : 1 );
            return new Tuple2( user_id , new ArrayList(result.subList(0,4)));
        });//.saveAsTextFile(outputFilePrefix+"/user_feature.txt");

        JavaPairRDD<Integer, ArrayList<Integer>> up = uf.mapValues( s -> {
            ArrayList<Tuple2<Integer,Double>> tresult = new ArrayList();
            s.forEach( t -> {
                int feature_id = (int)t._1() ;
                double feature_params = (double)t._2();

                String r = hm.get( feature_id );
                for( String r1 : r.split("#")) {
                    String[] tarr = r1.split(",") ;
                    double product_feature_params = Double.parseDouble(tarr[0]);
                    int product_id = Integer.parseInt(tarr[1]);
                    tresult.add( new Tuple2(product_id, product_feature_params*feature_params));
                }
            });
            tresult.sort((a,b) -> a._2() >= b._2() ? -1 : 1 );
            ArrayList<Integer> result = new ArrayList();
            for( Tuple2<Integer,Double> t : tresult){
                result.add(t._1());
            }
            return result ;
        });

        up.saveAsTextFile(outputFilePrefix+"/model.txt");

        JavaRDD<Integer> final_predict_result = testr.join(up).flatMap(t -> {
            Integer user_id = t._1();
            ArrayList<Integer> tmp_result = new ArrayList(t._2()._1()) ;
            ArrayList<Integer> tmp_predict = new ArrayList(t._2()._2()) ;
            ArrayList<Integer> result = new ArrayList();
            for (Integer r : tmp_result) {
                if( tmp_predict.contains(r) ){
                    result.add(1);
                }else{
                    result.add(0);
                }
            }
            return result ;
        });


        System.out.println(final_predict_result.filter(i -> i == 1).count());
        System.out.println(final_predict_result.count());

//        System.out.println(testr.join(up).take(5));

//        bestModel.productFeatures().toJavaRDD().mapToPair(s -> new Tuple2( s._1() , Arrays.toString(s._2()) ) ).saveAsTextFile(outputFilePrefix+"/product_feature.txt");

//        for( Tuple2<Object , double[]> t : bestModel.userFeatures().toJavaRDD().collect() ){
//            if(((Integer)t._1()) == 5000) {
//                System.out.println(t._1());
//                Arrays.sort(t._2());
//                System.out.println(Arrays.toString( t._2() ));
//            }
//        }
//        List result = findTop10(bestModel , sc , 6040 , 3952 );

//        System.out.println(result);
//        System.out.println(findTop10(bestModel , sc , 6039 , 3952 ));
//        List<Tuple2<Integer , Integer>> arr = new ArrayList<>();
//        for(int i = 1 ; i <= 3952 ; i++){
//            arr.add( new Tuple2(6040 , i));
//        }
//        JavaRDD testdata = sc.parallelize(arr);
//        JavaRDD<Rating> prediction = bestModel.predict(testdata.rdd()).toJavaRDD();
//        System.out.println(prediction.mapToPair(p -> new Tuple2(p.rating() , p.product() )).sortByKey(false).take(10));
//        System.out.println(prediction.take(10));
        sc.stop();

    }
        ;
    }
