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
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class JavaWordCount {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("JavaWordCount"));

        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaPairRDD<String, Integer> counts = lines.flatMap(line -> Arrays.asList(line.split(" ")))
                                                    .mapToPair(w -> new Tuple2<String,Integer>(w, 1))
                                                    .reduceByKey((x, y) -> x + y);

        System.out.println("Word count is " + counts.count() + " .");
        sc.stop();

    }
}
