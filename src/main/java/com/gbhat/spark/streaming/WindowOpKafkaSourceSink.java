package com.gbhat.spark.streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

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


    sudo docker-compose -f kafka_flink.yml up
    sudo bash add_docker_container_to_hosts.sh **IMPORTANT STEP**
    sudo docker exec -it kafka-0 /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic update_src --partitions 10 --bootstrap-server kafka-0:19092,kafka-1:29092,kafka-2:39092
    sudo docker exec -it kafka-0 /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic update_sink --partitions 10 --bootstrap-server kafka-0:19092,kafka-1:29092,kafka-2:39092

    sudo docker exec -it kafka-0 /opt/bitnami/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-0:19092,kafka-1:29092,kafka-2:39092 --topic update_src
    sudo docker exec -it kafka-0 /opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-0:19092,kafka-1:29092,kafka-2:39092 --topic update_sink --from-beginning

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
public class WindowOpKafkaSourceSink {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession session = SparkSession.builder().master("local[4,4]").getOrCreate();
        session.sparkContext().setLogLevel("WARN");
        session.conf().set("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/");
        Dataset<Row> lines = session
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-0:19092,kafka-1:29092,kafka-2:39092")
                .option("startingOffsets","latest")
                .option("subscribe", "update_src")
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

        StreamingQuery query = windowedCounts.writeStream()
                .outputMode("update")
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-0:19092,kafka-1:29092,kafka-2:39092")
                .option("topic", "update_sink")
                .start();

        query.awaitTermination();
    }
}
