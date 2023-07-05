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
    sudo docker-compose -f kafka_flink.yml up
    sudo bash add_docker_container_to_hosts.sh
    sudo docker exec -it kafka-0 /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic updates --partitions 10 --bootstrap-server kafka-0:19092,kafka-1:29092,kafka-2:39092
    sudo docker exec -it kafka-0 /opt/bitnami/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-0:19092,kafka-1:29092,kafka-2:39092 --topic updates
    > (TYPE MESSAGES)

    For Java >= 17 and Spark >= 3.3.0 Modify Run configurations:
    Click the top right drop down with Class Name
    Click Edit Configurations -> Modify options -> Add VM Options
    Add it to VM Options: --add-opens=java.base/sun.nio.ch=ALL-UNNAMED

 */
public class KafkaStream2 {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession session = SparkSession.builder().master("local[4,4]").getOrCreate();
        session.sparkContext().setLogLevel("WARN");
        Dataset<Row> lines = session
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-0:19092,kafka-1:29092,kafka-2:39092")
                .option("startingOffsets","earliest")
                .option("subscribe", "updates")
                .load();

        lines.printSchema();

        Dataset<Row> lines2 = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        Dataset<String> words = lines2.flatMap((FlatMapFunction<Row, String>) row -> Arrays.asList(((String) row.getAs("value")).split(" ")).iterator(), Encoders.STRING());
        Dataset<Row> count = words.groupBy("value").count();

        StreamingQuery query = count.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .start();

        query.awaitTermination();
    }
}
