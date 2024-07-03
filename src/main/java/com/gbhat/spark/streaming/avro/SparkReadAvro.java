package com.gbhat.spark.streaming.avro;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.avro.functions.from_avro;

/*
    This read Avro data written by SparkReadPlainWriteAvro.
    Run SparkReadPlainWriteAvro before

    sudo docker-compose -f kafka_flink.yml up
    sudo bash add_docker_container_to_hosts.sh **IMPORTANT STEP**

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
public class SparkReadAvro {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException, IOException {
        SparkSession session = SparkSession.builder().master("local[4,4]").getOrCreate();
        session.sparkContext().setLogLevel("WARN");
        Dataset<Row> ds = session
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-0:19092,kafka-1:29092,kafka-2:39092")
                .option("startingOffsets","earliest")
                .option("subscribe", "avro_topic")
                .load();

        ds.printSchema();

        String jsonFormatSchema = new String(Files.readAllBytes(Paths.get("src/main/resources/user_basic.avsc")));

        Dataset<Row> origDs = ds.select(from_avro(ds.col("value"), jsonFormatSchema).as("data"));

        origDs = origDs.select("data.name", "data.id", "data.age");

        StreamingQuery query = origDs.writeStream()
                .outputMode("append")
                .format("console")
                .option("truncate", "false")
                .start();

        query.awaitTermination();
    }
}
