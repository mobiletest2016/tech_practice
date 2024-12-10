package com.gbhat.hudi;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.avro.functions.to_avro;
import static org.apache.spark.sql.functions.struct;

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
public class Write {
    public static void main(String[] args) throws Exception {
        SparkSession session = SparkSession.builder()
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .master("local[4,4]")
                .getOrCreate();
        session.sparkContext().setLogLevel("WARN");

        StructType schema = new StructType()
                .add("name", DataTypes.StringType)
                .add("amt", DataTypes.LongType)
                .add("date", DataTypes.StringType);

        List<Row> list = new LinkedList<>();
        for(int i = 0; i < 100; i++) {
            list.add(RowFactory.create("User_" + i, (long) (i + 10) * 10, "2024-11-" + ((i + 1) / 10)));
        }
        Dataset<Row> ds = session.createDataFrame(list, schema);
        long millis = System.currentTimeMillis();
        ds = ds.withColumn("ts", functions.lit(millis));
        ds.show();
        ds.write()
                .format("hudi")
                .option("hoodie.datasource.write.partitionpath.field", "date")
                .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL())
                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "name")
                .option("hoodie.datasource.write.precombine.field", "ts")
                .option(HoodieWriteConfig.TBL_NAME.key(), "hudiTable")
                .mode(SaveMode.Overwrite)
                .save("/tmp/hudi_data/");


        System.out.println("First write completed...");
        Thread.sleep(3000);

        ds = ds.withColumn("amt", ds.col("amt").multiply(10));

        ds.write()
                .format("hudi")
                .option("hoodie.datasource.write.partitionpath.field", "date")
                .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL())
                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "name")
                .option("hoodie.datasource.write.precombine.field", "ts")
                .option(HoodieWriteConfig.TBL_NAME.key(), "hudiTable")
                .mode(SaveMode.Append)
                .option("hoodie.datasource.write.operation", "upsert")
                .save("/tmp/hudi_data/");


        Thread.sleep(3000);
        System.out.println("Reading data back...");
        session.read().format("hudi").load("/tmp/hudi_data").show(1000, false);
    }
}
