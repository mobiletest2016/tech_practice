package com.gbhat.hudi;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.BaseAvroPayload;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.Option;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Objects;

public class OverwriteWithOldestAvroPayload extends OverwriteWithLatestAvroPayload {

    public OverwriteWithOldestAvroPayload(GenericRecord record, Comparable orderingVal) {
        super(record, orderingVal);
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
        return Option.of(currentValue);
    }
}