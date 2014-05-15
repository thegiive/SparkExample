/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

/**
 * Logistic regression based classification.
 */
public class JavaHDFSLR {


    private static final int D = 10;   // Number of dimensions
    private static final Random rand = new Random(42);

    static class DataPoint implements Serializable {
        DataPoint(double[] x, double y) {
            this.x = x;
            this.y = y;
        }
        double[] x;
        double y;
    }

    public static double dot(double[] a, double[] b) {
        double x = 0;
        for (int i = 0; i < D; i++) {
            x += a[i] * b[i];
        }
        return x;
    }

    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: JavaHdfsLR <file> <iters>");
            System.exit(1);
        }

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("JavaHDFSLR"));
        JavaRDD<String> lines = sc.textFile(args[0]);
        int ITERATIONS = Integer.parseInt(args[1]);


        JavaRDD<DataPoint> points = lines.map(
           line -> {
                String[] tok = line.split(" ");
                double y = Double.parseDouble(tok[0]);
                double[] x = new double[D];
                for (int i = 0; i < D; i++) {
                    x[i] = Double.parseDouble(tok[i + 1]);
                }
                return new DataPoint(x,y);
            }
        ).cache();

        // Initialize w to a random value
        double[] w = new double[D];
        for (int i = 0; i < D; i++) {
            w[i] = 2 * rand.nextDouble() - 1;
        }
        System.out.print("Initial w: ");
        System.out.println(Arrays.toString(w));
        for (int i = 1; i <= ITERATIONS; i++) {
            System.out.println("On iteration " + i);

            double[] gradient = points.map(
                p -> {
                    double[] gra = new double[D];
                    for (int j = 0; j < D; j++) {
                        double dot = dot(w, p.x);
                        gra[j] = (1 / (1 + Math.exp(-p.y * dot)) - 1) * p.y * p.x[j];
                    }
                    return gra;
                }
            ).reduce(
                (a,b) -> {
                    double[] result = new double[D];
                    for (int j = 0; j < D; j++) {
                        result[j] = a[j] + b[j];
                    }
                    return result;
                }
            );

            for (int j = 0; j < D; j++) {
                w[j] -= gradient[j];
            }

        }

        System.out.print("Final w: ");
        System.out.println(Arrays.toString(w));
        sc.stop();
    }

}
