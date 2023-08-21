package com.gbhat.spark.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/*

    For Java >= 17 and Spark >= 3.3.0 Modify Run configurations:
    Click the top right drop down with Class Name
    Click Edit Configurations -> Modify options -> Add VM Options
    Add it to VM Options: --add-opens=java.base/sun.nio.ch=ALL-UNNAMED

    sudo docker-compose -f kafka_flink.yml up
    sudo bash add_docker_container_to_hosts.sh
    sudo docker exec -it kafka-0 /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic updates --partitions 10 --bootstrap-server kafka-0:19092,kafka-1:29092,kafka-2:39092
    sudo docker exec -it kafka-0 /opt/bitnami/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-0:19092,kafka-1:29092,kafka-2:39092 --topic updates
    > (TYPE MESSAGES)

Open clock in another display.
Sample input: (Enter at different time)
>a
>a
>a
>a


Sample output:
-------------------------------------------
Batch: 8
-------------------------------------------
+------------------------------------------+-----+-----+
|window                                    |value|count|
+------------------------------------------+-----+-----+
|{2023-08-21 17:08:00, 2023-08-21 17:09:00}|a    |2    |
|{2023-08-21 17:07:30, 2023-08-21 17:08:30}|a    |3    |
|{2023-08-21 17:07:00, 2023-08-21 17:08:00}|a    |2    |
|{2023-08-21 17:06:30, 2023-08-21 17:07:30}|a    |1    |
|{2023-08-21 17:06:00, 2023-08-21 17:07:00}|a    |2    |
|{2023-08-21 17:05:30, 2023-08-21 17:06:30}|a    |4    |
|{2023-08-21 17:05:00, 2023-08-21 17:06:00}|a    |2    |
+------------------------------------------+-----+-----+

 */
public class WindowOp {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession session = SparkSession.builder().master("local[4,4]").getOrCreate();
        session.sparkContext().setLogLevel("WARN");
        Dataset<Row> lines = session
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-0:19092,kafka-1:29092,kafka-2:39092")
                .option("startingOffsets","latest")
                .option("subscribe", "updates")
                .load();

        Dataset<Row> lines2 = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp");

        Dataset<Row> words = lines2.flatMap((FlatMapFunction<Row, Row>) row -> {
            String value = row.getAs("value");
            List<Row> result = new LinkedList<>();
            for(String split : value.split(" "))
                    result.add(new GenericRow(new Object[]{row.getAs("key"), split, row.getAs("timestamp")}));
            return result.iterator();
        }, RowEncoder.apply(lines2.schema()));

        Dataset<Row> windowedCounts = words.
                withWatermark("timestamp", "2 minutes").
                groupBy(
                functions.window(words.col("timestamp"), "1 minute", "30 seconds"),
                words.col("value")
        ).count();

        StreamingQuery query = windowedCounts.orderBy(windowedCounts.col("window").desc()).writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .start();

        query.awaitTermination();
    }
}
