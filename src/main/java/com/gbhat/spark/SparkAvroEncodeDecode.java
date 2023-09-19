package com.gbhat.spark;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.avro.functions.to_avro;
import static org.apache.spark.sql.functions.struct;

/*
    sudo docker-compose -f kafka_flink.yml up
    sudo bash add_docker_container_to_hosts.sh
    sudo docker exec -it kafka-0 /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic avro_topic --partitions 10 --bootstrap-server kafka-0:19092,kafka-1:29092,kafka-2:39092

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
public class SparkAvroEncodeDecode {
    public static void main(String[] args) throws IOException {
        SparkSession session = SparkSession.builder().master("local[4,4]").getOrCreate();
        session.sparkContext().setLogLevel("WARN");

        String jsonFormatSchema = new String(Files.readAllBytes(Paths.get("src/main/resources/user_basic.avsc")));

        StructType schema = new StructType()
                .add("name", DataTypes.StringType)
                .add("id", DataTypes.LongType)
                .add("age", DataTypes.IntegerType);

        for(int i = 0; i < 1; i++) {
            List<Row> list = List.of(RowFactory.create("User_" + i, (long) i, 10 + i));
            Dataset<Row> ds = session.createDataFrame(list, schema);
            ds.show();
            Dataset<Row> avroDs = ds.select(to_avro(struct(ds.col("name"), ds.col("id"), ds.col("age")), jsonFormatSchema).as("value"));

            avroDs.show();
            Dataset<Row> origDs = avroDs.select(from_avro(avroDs.col("value"), jsonFormatSchema).as("data"))
                            .select("data.name", "data.id", "data.age");
            origDs.show();


        }

    }
}
