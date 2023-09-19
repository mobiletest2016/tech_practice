package com.gbhat.spark.streaming.avro;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.avro.functions.to_avro;
import static org.apache.spark.sql.functions.struct;


/*
    This reads plain text written by WriteKafkaPlainText and converts to avro
    Run WriteKafkaPlainText application

    sudo docker-compose -f kafka_flink.yml up
    sudo bash add_docker_container_to_hosts.sh
    sudo docker exec -it kafka-0 /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic avro_topic --partitions 10 --bootstrap-server kafka-0:19092,kafka-1:29092,kafka-2:39092


    For Java >= 17 and Spark >= 3.3.0 Modify Run configurations:
    Click the top right drop down with Class Name
    Click Edit Configurations -> Modify options -> Add VM Options
    Add it to VM Options: --add-opens=java.base/sun.nio.ch=ALL-UNNAMED

 */
public class SparkReadPlainWriteAvro {
    public static void main(String[] args) throws IOException, TimeoutException, StreamingQueryException {
        SparkSession session = SparkSession.builder().master("local[4,4]").getOrCreate();
        session.sparkContext().setLogLevel("WARN");

        String jsonFormatSchema = new String(Files.readAllBytes(Paths.get("src/main/resources/user_basic.avsc")));

        Dataset<Row> ds = session
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-0:19092,kafka-1:29092,kafka-2:39092")
                .option("startingOffsets","earliest")
                .option("subscribe", "plain_text_topic")
                .load();

        ds.printSchema();

        ds = ds.selectExpr("*", "CAST(value AS STRING) as data");

        ds = ds.withColumn("split", functions.split(ds.col("data"), ","));

        ds = ds.selectExpr("split[0] as name", "CAST(split[1] as LONG) as id", "cast(split[2] as INT) as age");

        Dataset<Row> avroDs = ds.select(to_avro(struct(ds.col("name"), ds.col("id"), ds.col("age")), jsonFormatSchema).as("value"));

        avroDs.printSchema();

        avroDs.writeStream()
                .format("kafka")
                .outputMode("append")
                .option("kafka.bootstrap.servers", "kafka-0:19092,kafka-1:29092,kafka-2:39092")
                .option("topic", "avro_topic")
                .option("checkpointLocation","/tmp")
                .start()
                .awaitTermination();

    }
}
