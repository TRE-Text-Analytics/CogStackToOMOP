/*

This script prepares an AVRO record (one row from the DB) for TIKA.

The binary column becomes the FlowFile content that is sent to TIKA.
Every other column is kept as a FlowFile attribute named "src.<column>",
so the later scripts can rebuild the full record around the OCR text.
Timestamps are kept as epoch milliseconds.

Dynamic property:
  binary_field   name of the column holding the document bytes (e.g. file_data)

*/


@Grab('org.apache.avro:avro:1.8.1')
import org.apache.avro.*
import org.apache.avro.file.*
import org.apache.avro.generic.*

import org.apache.nifi.processor.io.StreamCallback
import java.nio.*
import java.nio.channels.*


def flowFile = session.get()
if (flowFile == null) {
    return
}

String binaryField = binary_field.value
String SRC_PREFIX = 'src.'

Map<String, String> sourceAttributes = [:]
String problem = null

try {
    flowFile = session.write(flowFile, { inputStream, outputStream ->

        DataFileStream<GenericRecord> reader = new DataFileStream<>(inputStream, new GenericDatumReader<GenericRecord>())

        // there should be exactly one record (ExecuteSQLRecord: Max Rows Per Flow File = 1)
        if (!reader.hasNext()) {
            problem = 'the FlowFile contains no record'
            return
        }
        GenericRecord currRecord = reader.next()

        if (currRecord.getSchema().getField(binaryField) == null) {
            problem = "the record has no column '${binaryField}'"
            return
        }
        Object rawContent = currRecord.get(binaryField)
        if (rawContent == null) {
            problem = "column '${binaryField}' is empty"
            return
        }

        // keep every other column as an attribute
        currRecord.getSchema().getFields().each { field ->
            if (field.name() != binaryField) {
                Object value = currRecord.get(field.name())
                if (value != null) {
                    sourceAttributes[SRC_PREFIX + field.name()] = value.toString()
                }
            }
        }

        // write the document bytes as the FlowFile content
        ByteBuffer rawBytes = (ByteBuffer) rawContent
        WritableByteChannel channel = Channels.newChannel(outputStream)
        channel.write(rawBytes)

    } as StreamCallback)
} catch (Exception e) {
    log.error('Failed to read the DB record: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
    return
}

if (problem != null || !sourceAttributes.containsKey(SRC_PREFIX + 'note_id')) {
    log.error('Cannot prepare the document for TIKA: ' + (problem ?: 'the record has no note_id'))
    session.transfer(flowFile, REL_FAILURE)
    return
}

flowFile = session.putAllAttributes(flowFile, sourceAttributes)
session.transfer(flowFile, REL_SUCCESS)
