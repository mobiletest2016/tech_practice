package com.gbhat.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;

public class HBaseScan {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "zookeeper");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        Connection conn = ConnectionFactory.createConnection(config);

        Table table = conn.getTable(TableName.valueOf("people"));

        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        showResult(scanner);

        Scan scan2 = new Scan();
        scan2.addFamily(Bytes.toBytes("PersonalDetails"));
        showResult(table.getScanner(scan2));

    }

    private static void showResult(ResultScanner scanner) {
        for(Result result : scanner) {
            System.out.println("----");
            result.getNoVersionMap().forEach((key, value) -> System.out.println(new String(key) + ":" + new String(value.firstEntry().getKey()) + " -> " + new String(value.firstEntry().getValue())));
        }
        System.out.println("--------------");
        System.out.println("--------------");
        System.out.println("--------------");
    }
}
