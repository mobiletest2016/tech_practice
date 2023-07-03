package com.gbhat.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

public class WindowEx {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local[8]").getOrCreate();
        session.sparkContext().setLogLevel("WARN");

        Dataset<Row> ds = session.read().option("header", "true")
                .option("inferSchema", "true")
                .option("header", true)
                .option("delimiter", "|")
                .csv("data.csv");

        ds = ds.withColumn("Rank", functions.row_number().over(Window.partitionBy(ds.col("department")).orderBy("salary")));
        ds.show();

        System.out.println("Top 3 salaries:");
        ds.filter(ds.col("Rank").leq(3)).show();
    }
}
