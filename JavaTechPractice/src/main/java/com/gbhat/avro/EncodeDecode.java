package com.gbhat.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;

public class EncodeDecode {
    public static void main(String[] args) throws IOException {
        Schema schema = new Schema.Parser().parse(new File("src/main/resources/user.avsc"));
        encode(schema);

        decode(schema);
    }

    private static void decode(Schema schema) throws IOException {
        File file = new File("user1.avro");
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println("Decoded: " + user.get("id") + " " + user);
        }
    }

    private static void encode(Schema schema) throws IOException {
        GenericRecord record1 = getGenericRecord(schema, "Guru", 37, 1, "Bangalore", 560087);
        GenericRecord record2 = getGenericRecord(schema, "Pratibha", 36, 2, "Bangalore", 560087);
        GenericRecord record3 = getGenericRecord(schema, "Samarth", 3, 3, "Bangalore", 560087);

        File file = new File("user1.avro");
        DatumWriter<GenericRecord> dwriter = new GenericDatumWriter<>();
        DataFileWriter<GenericRecord> writer = new DataFileWriter<>(dwriter);
        writer.create(schema, file);
        writer.append(record1);
        writer.append(record2);
        writer.append(record3);
        writer.flush();
        writer.close();
    }

    @NotNull
    private static GenericRecord getGenericRecord(Schema schema, String name, int age, int id, String city, int pin) {
        GenericRecord record1 = new GenericData.Record(schema);
        record1.put("name", name);
        record1.put("age", age);
        record1.put("id", id);
        GenericRecord address = new GenericData.Record(schema.getField("user_address").schema());
        address.put("city", city);
        address.put("pin", pin);
        record1.put("user_address", address);
        return record1;
    }
}
