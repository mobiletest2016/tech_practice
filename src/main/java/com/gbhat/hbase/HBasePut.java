package com.gbhat.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

// For setup run:
//  sudo docker-compose -f hadoop_hive_hbase_spark.yml up
//  sudo bash add_docker_container_to_hosts.sh

public class HBasePut {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "zookeeper");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        Connection conn = ConnectionFactory.createConnection(config);

        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("people"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("PersonalDetails")).build())
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("ProfessionalDetails")).build())
                .build();
        conn.getAdmin().createTable(tableDescriptor);

        Table table = conn.getTable(TableName.valueOf("people"));

        Put put = new Put(Bytes.toBytes("Person1"))
                .addColumn(Bytes.toBytes("PersonalDetails"), Bytes.toBytes("name"), Bytes.toBytes("Guru"))
                .addColumn(Bytes.toBytes("ProfessionalDetails"), Bytes.toBytes("job"), Bytes.toBytes("Engineer"));
        table.put(put);

        List<Put> puts = new LinkedList<>();
        puts.add(new Put(Bytes.toBytes("Person2"))
                .addColumn(Bytes.toBytes("PersonalDetails"), Bytes.toBytes("name"), Bytes.toBytes("Subbi"))
                .addColumn(Bytes.toBytes("ProfessionalDetails"), Bytes.toBytes("job"), Bytes.toBytes("Engineer")));
        puts.add(new Put(Bytes.toBytes("Person3"))
                .addColumn(Bytes.toBytes("PersonalDetails"), Bytes.toBytes("name"), Bytes.toBytes("Samarth"))
                .addColumn(Bytes.toBytes("ProfessionalDetails"), Bytes.toBytes("job"), Bytes.toBytes("Baby")));
        table.put(puts);

        table.close();
        conn.close();
    }
}
