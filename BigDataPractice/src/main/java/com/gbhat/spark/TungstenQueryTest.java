package com.gbhat.spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;

public class TungstenQueryTest {
    public static void main(String[] args) throws IOException {
        SparkSession session = SparkSession.builder().master("local[8]").getOrCreate();
        Dataset<Row> ds = session.read().option("header", "true").option("inferSchema", "true").option("samplingRation", "0.01").csv("/home/guru/Desktop/Dataset/DelayedFlights.csv");

        ds = ds.persist(StorageLevel.MEMORY_AND_DISK());

        System.out.println("Original count: " + ds.count());

        long before1 = System.currentTimeMillis();
        Dataset<Row> result1 = ds.filter(ds.col("FlightNum").equalTo(335).and(ds.col("Year").notEqual(2008)));
        System.out.println("Count: " + result1.count());
        long after1 = System.currentTimeMillis();
        System.out.println("Time taken: " + (after1 - before1));

        long before = System.currentTimeMillis();
        Dataset<Row> result = ds.filter((FilterFunction<Row>) row -> (int) row.getAs("FlightNum") == 335)
                .filter((FilterFunction<Row>) row -> (int) row.getAs("Year") != 2008);
        System.out.println("Count: " + result.count());
        long after = System.currentTimeMillis();
        System.out.println("Time taken: " + (after - before));

        System.in.read();
    }
}
