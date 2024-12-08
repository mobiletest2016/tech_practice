package com.gbhat.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class UDFEx {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local[4]").getOrCreate();
        session.udf().register("DoubleRankUDF", (UDF1<Integer, Integer>) in -> in * 2, DataTypes.IntegerType);

        Dataset<Row> ds = getDs(session);
        ds.withColumn("DoubleRank", functions.callUDF("DoubleRankUDF", ds.col("Rank"))).show();
    }

    private static Dataset<Row> getDs(SparkSession session) {
        String schemaStr = "`Name` STRING, `Rank` INT";
        StructType schema = StructType.fromDDL(schemaStr);
        List<Row> rowList = Arrays.asList(
                RowFactory.create("A", 1),
                RowFactory.create("B", 2),
                RowFactory.create("C", 3),
                RowFactory.create("D", 4),
                RowFactory.create("E", 5),
                RowFactory.create("F", 6));
        Dataset<Row> ds = session.createDataFrame(rowList, schema);
        return ds;
    }
}
