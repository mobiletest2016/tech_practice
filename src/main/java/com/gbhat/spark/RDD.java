package com.gbhat.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

/*
    For Java >= 17 and Spark >= 3.3.0 Modify Run configurations:
    Click the top right drop down with Class Name
    Click Edit Configurations -> Modify options -> Add VM Options
    Add it to VM Options:
    --add-opens=java.base/java.lang=ALL-UNNAMED
    --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
    --add-opens=java.base/java.io=ALL-UNNAMED
    --add-opens=java.base/java.net=ALL-UNNAMED
    --add-opens=java.base/java.nio=ALL-UNNAMED
    --add-opens=java.base/java.util=ALL-UNNAMED
    --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
    --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
    --add-opens=java.base/sun.security.action=ALL-UNNAMED
    --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
    --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
 */

public class RDD {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("RDD Programming");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sparkContext.textFile("src/main/java/com/gbhat/spark/*.java");
//        System.out.println(rdd.collect());

        rdd = rdd.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());

        rdd = rdd.filter((Function<String, Boolean>) v1 -> !v1.isBlank() && v1.matches("[A-Za-z0-9]+"));

        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair((PairFunction<String, String, Integer>) s -> Tuple2.apply(s, 1));

        JavaPairRDD<String, Integer> wordCountRDD = pairRDD.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        wordCountRDD.collectAsMap().forEach((s, i) -> System.out.println(s + "=" + i));

        System.out.println("Top 3 words: ");
        wordCountRDD
                .map((Function<Tuple2<String, Integer>, Tuple2<Integer, String>>) v1 -> Tuple2.apply(v1._2, v1._1))
                .takeOrdered(3, new CustomComparator())
                .forEach(t -> {
                    System.out.println(t._2 + "=" + t._1);
                });

    }

    public static class CustomComparator implements Comparator<Tuple2<Integer, String>>, Serializable {

        @Override
        public int compare(Tuple2<Integer, String> o1, Tuple2<Integer, String> o2) {
            return o2._1 - o1._1;
        }
    }
}
