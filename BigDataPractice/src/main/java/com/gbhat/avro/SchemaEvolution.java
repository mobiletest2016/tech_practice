package com.gbhat.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class SchemaEvolution {
    public static void main(String[] args) throws IOException {
        encode1Decode2();
        encode2Decode1();
    }

    //Encode with user.avsc and decode with user2.avsc
    private static void encode1Decode2() throws IOException {
        Schema schema1 = new Schema.Parser().parse(new File("src/main/resources/user.avsc"));
        GenericRecord record1 = getGenericRecord1(schema1, "Guru", 37, 1, "Bangalore", 560087);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> dwriter = new GenericDatumWriter<>(schema1);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        dwriter.write(record1, encoder);
        encoder.flush();
        out.close();

        byte[] data = out.toByteArray();
        System.out.println("Encoded with schema 1 size: " + data.length);

        Schema schema2 = new Schema.Parser().parse(new File("src/main/resources/user2.avsc"));
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema1, schema2);                     //IMP: this takes both schema inputs
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        GenericRecord result = reader.read(null, decoder);
        System.out.println("Decoded with schema 2 : " + result);
    }

    //Encode with user2.avsc and decode with user.avsc
    private static void encode2Decode1() throws IOException {
        Schema schema2 = new Schema.Parser().parse(new File("src/main/resources/user2.avsc"));
        GenericRecord record1 = getGenericRecord2(schema2, "Guru", 37,  UUID.randomUUID().toString(),1, "Bangalore", 560087, 47083);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> dwriter = new GenericDatumWriter<>(schema2);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        dwriter.write(record1, encoder);
        encoder.flush();
        out.close();

        byte[] data = out.toByteArray();
        System.out.println("Encoded with schema 2 size: " + data.length);

        Schema schema1 = new Schema.Parser().parse(new File("src/main/resources/user.avsc"));
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema2, schema1);                     //IMP: this takes both schema inputs
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        GenericRecord result = reader.read(null, decoder);
        System.out.println("Decoded with schema 1 : " + result);
    }

    private static GenericRecord getGenericRecord1(Schema schema, String name, int age, int id, String city, int pin) {
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

    private static GenericRecord getGenericRecord2(Schema schema, String name, int age, String uuid, int id, String city, int pin, int houseNo) {
        GenericRecord record1 = new GenericData.Record(schema);
        record1.put("name", name);
        record1.put("age", age);
        record1.put("id", id);
        record1.put("uuid", uuid);
        GenericRecord address = new GenericData.Record(schema.getField("user_address").schema());
        address.put("city", city);
        address.put("pin", pin);
        address.put("house_no", houseNo);
        record1.put("user_address", address);
        return record1;
    }
}
