package com.gbhat.hudi;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.LinkedList;
import java.util.List;

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
public class WriteUpsertKeepOld {
    public static void main(String[] args) throws Exception {
        SparkSession session = SparkSession.builder()
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .master("local[4,4]")
                .getOrCreate();
        session.sparkContext().setLogLevel("WARN");

        StructType schema = new StructType()
                .add("name", DataTypes.StringType)
                .add("date", DataTypes.StringType);

        List<Row> list = new LinkedList<>();
        for(int i = 0; i < 10; i++) {
            list.add(RowFactory.create("User_" + i, "2023-02-" + (i + 1)));
        }
        Dataset<Row> ds = session.createDataFrame(list, schema);
        long millis = System.currentTimeMillis();
        ds = ds.withColumn("ts", functions.lit(millis));
        ds.show();
        ds.write()
                .format("hudi")
//                .option("hoodie.datasource.write.partitionpath.field", "date")
                .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL())
                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "name")
                .option( "hoodie.index.type", "GLOBAL_BLOOM")
                .option("hoodie.datasource.write.precombine.field", "name")
                .option("hoodie.datasource.write.payload.class", "com.gbhat.hudi.OverwriteWithOldestAvroPayload")
                .option(HoodieWriteConfig.TBL_NAME.key(), "hudiTable")
                .mode(SaveMode.Overwrite)
                .save("/tmp/hudi_data/");


        System.out.println("First write completed...");
        Thread.sleep(3000);

        list = new LinkedList<>();
        for(int i = 0; i < 10; i++) {
            list.add(RowFactory.create("User_" + i, "2024-10-" + (i + 10 + 1)));
        }

        ds = session.createDataFrame(list, schema);
        millis = System.currentTimeMillis();
        ds = ds.withColumn("ts", functions.lit(millis));

        ds.show();
        ds.write()
                .format("hudi")
//                .option("hoodie.datasource.write.partitionpath.field", "date")
                .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL())
                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "name")
                .option("hoodie.datasource.write.precombine.field", "name")
                .option(HoodieWriteConfig.TBL_NAME.key(), "hudiTable")
                .option("hoodie.datasource.write.payload.class", "com.gbhat.hudi.OverwriteWithOldestAvroPayload")
                .mode(SaveMode.Append)
                .option("hoodie.datasource.write.operation", "upsert")
                .save("/tmp/hudi_data/");


        Thread.sleep(3000);
        System.out.println("Reading data back...");
        session.read().format("hudi").load("/tmp/hudi_data").show(1000, false);
    }
}