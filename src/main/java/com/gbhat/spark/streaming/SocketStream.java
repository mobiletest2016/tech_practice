package com.gbhat.spark.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

/*
    Run a socket server:
        nc -l 7777

    For Java >= 17 and Spark >= 3.3.0 Modify Run configurations:
    Click the top right drop down with Class Name
    Click Edit Configurations -> Modify options -> Add VM Options
    Add it to VM Options: --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
 */
public class SocketStream {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession session = SparkSession.builder().master("local[4,4]").getOrCreate();
        session.sparkContext().setLogLevel("WARN");
        Dataset<Row> lines = session.readStream().format("socket").option("host", "localhost").option("port", 7777).load();
        lines.printSchema();
        Dataset<String> words = lines.as(Encoders.STRING()).flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
        Dataset<Row> count = words.groupBy("value").count();

        StreamingQuery query = count.writeStream()
                .outputMode("update")   //.outputMode("complete") to see all previous
                .format("console")
                .option("truncate", "false")
                .start();

        query.awaitTermination();
    }
}
