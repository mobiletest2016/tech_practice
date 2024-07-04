package com.gbhat.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

// For setup run:
//  sudo docker-compose -f hadoop_hive_hbase_spark.yml up
//  sudo bash add_docker_container_to_hosts.sh **IMPORTANT STEP**

public class HBasePutLarge {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "zookeeper");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        Connection conn = ConnectionFactory.createConnection(config);

        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("cid"))   // consistent id hash to integer map
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("id")).build())
                .build();
        conn.getAdmin().createTable(tableDescriptor);

        TableDescriptor tableDescriptor2 = TableDescriptorBuilder.newBuilder(TableName.valueOf("cid_rev"))   // reverse map
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("hash")).build())
                .build();
        conn.getAdmin().createTable(tableDescriptor2);

        Table table = conn.getTable(TableName.valueOf("cid"));
        Table table2 = conn.getTable(TableName.valueOf("cid_rev"));
        for(int i = 0; i < 100_000; i++) {
            if (i % 1000 == 0)
                System.out.println("i: " + i);
            String uuid = UUID.randomUUID().toString();
            Put put = new Put(Bytes.toBytes(uuid))
                    .addColumn(Bytes.toBytes("id"), Bytes.toBytes("id"), Bytes.toBytes(i));
            table.put(put);

            Put put2 = new Put(Bytes.toBytes(String.valueOf(i)))                                    //Store int as string for easy fetch
                    .addColumn(Bytes.toBytes("hash"), Bytes.toBytes("hash"), Bytes.toBytes(uuid));
            table2.put(put2);
        }

        table.close();
        conn.close();
    }
}
