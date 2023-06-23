package com.gbhat.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

public class EncodeJson {
    public static void main(String[] args) throws IOException {
        Schema schema = new Schema.Parser().parse(new File("src/main/resources/user.avsc"));
        encode(schema);
    }

    private static void encode(Schema schema) throws IOException {
        GenericRecord record1 = getGenericRecord(schema, "Guru", 37, 1, "Bangalore", 560087);
        GenericRecord record2 = getGenericRecord(schema, "Pratibha", 36, 2, "Bangalore", 560087);
        GenericRecord record3 = getGenericRecord(schema, "Samarth", 3, 3, "Bangalore", 560087);

        File file = new File("user1.avro.json");
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, System.out, true);
        writer.write(record1, jsonEncoder);
        writer.write(record2, jsonEncoder);
        writer.write(record3, jsonEncoder);
        jsonEncoder.flush();
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
