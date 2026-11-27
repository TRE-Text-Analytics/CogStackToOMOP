/*

This script converts the parsed TIKA result (JSON) to an AVRO record
in the same shape as the discharge_summaries documents:

  note_id, subject_id, hadm_id, note_type, note_seq, charttime, storetime, text
  + processing_timestamp and metadata_* fields from TIKA

note_type and note_seq are taken from the source columns when present,
otherwise from note_id ("<subject_id>-DS-11" -> "DS", 11).

Dynamic property:
  avro_schema_path   path to document-mimic.avsc

*/

@Grab('org.apache.avro:avro:1.11.0')
import org.apache.avro.*
import org.apache.avro.file.*
import org.apache.avro.generic.*

import java.time.ZonedDateTime

import groovy.json.JsonSlurper
import org.apache.commons.io.IOUtils
import org.apache.nifi.processor.io.StreamCallback
import java.nio.charset.StandardCharsets


// avro schema instance, loaded when the processor starts
//
class SchemaInstance {
    static Schema avroSchema = null

    static loadSchema(text) {
        avroSchema = new Schema.Parser().parse(text)
    }
}

static onStart(ProcessContext context) {
    def filePath = context.getProperty('avro_schema_path') as String
    SchemaInstance.loadSchema(new File(filePath).text)
}


def flowFile = session.get()
if (flowFile == null) {
    return
}


// helpers
//
Integer toInt(value) {
    if (value == null || value.toString().trim().isEmpty()) return null
    return Integer.valueOf(value.toString().trim())
}

Long toLong(value) {
    if (value == null || value.toString().trim().isEmpty()) return null
    return Long.valueOf(value.toString().trim())
}

Double toDouble(value) {
    if (value == null || value.toString().trim().isEmpty()) return null
    return Double.valueOf(value.toString().trim())
}

// TIKA timestamps look like "2026-11-24 21:30:00.123456" or ISO 8601
Long timestampToMillis(value) {
    if (value == null) return null
    String ts = value.toString().trim().replace(' ', 'T')
    if (!(ts.endsWith('Z') || ts ==~ /.*[+-]\d\d:\d\d$/)) {
        ts += 'Z'
    }
    try {
        return ZonedDateTime.parse(ts).toInstant().toEpochMilli()
    } catch (Exception e) {
        return null
    }
}

// only set fields that exist in the schema
void setField(GenericRecord record, String name, value) {
    if (record.getSchema().getField(name) != null && value != null) {
        record.put(name, value)
    }
}

GenericRecord parseJsonToAvro(Map inJson, Schema avroSchema) {
    GenericRecord docRecord = new GenericData.Record(avroSchema)

    // required
    String noteId = inJson['note_id']
    if (!noteId) throw new IllegalArgumentException('note_id is missing')
    Integer subjectId = toInt(inJson['subject_id'])
    if (subjectId == null) throw new IllegalArgumentException("subject_id is missing for note ${noteId}")

    docRecord.put('note_id', noteId)
    docRecord.put('subject_id', subjectId)
    docRecord.put('text', (inJson['text'] ?: '') as String)

    // note_type / note_seq: source columns first, otherwise from note_id
    String noteType = inJson['note_type']
    Integer noteSeq = toInt(inJson['note_seq'])
    def parts = noteId.split('-')
    if (parts.length >= 3) {
        if (!noteType) noteType = parts[parts.length - 2]
        if (noteSeq == null && parts[parts.length - 1].isInteger()) noteSeq = parts[parts.length - 1] as Integer
    }

    setField(docRecord, 'hadm_id', toInt(inJson['hadm_id']))
    setField(docRecord, 'note_type', noteType)
    setField(docRecord, 'note_seq', noteSeq)
    setField(docRecord, 'charttime', toLong(inJson['charttime']))   // epoch millis from the DB record
    setField(docRecord, 'storetime', toLong(inJson['storetime']))

    // TIKA output
    setField(docRecord, 'processing_timestamp', timestampToMillis(inJson['timestamp']))

    Map metadata = (inJson['metadata'] ?: [:]) as Map
    setField(docRecord, 'metadata_content_type', metadata['content-type'] != null ? metadata['content-type'].toString() : null)
    setField(docRecord, 'metadata_page_count', toInt(metadata['pages']))
    if (metadata['X-OCR-Applied'] != null) {
        setField(docRecord, 'metadata_x_ocr_applied', Boolean.valueOf(metadata['X-OCR-Applied'].toString()))
    }
    if (metadata['X-TIKA:Parsed-By'] != null) {
        def parsedBy = metadata['X-TIKA:Parsed-By']
        setField(docRecord, 'metadata_x_parsed_by', parsedBy instanceof List ? parsedBy.join(';') : parsedBy.toString())
    }
    setField(docRecord, 'metadata_confidence', toDouble(metadata['confidence']))
    setField(docRecord, 'metadata_elapsed_time', toDouble(metadata['elapsed_time']))

    return docRecord
}


// process the flow file
//
try {
    flowFile = session.write(flowFile, { inputStream, outputStream ->
        def content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        Map inJson = new JsonSlurper().parseText(content) as Map

        DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(SchemaInstance.avroSchema))
        writer.create(SchemaInstance.avroSchema, outputStream)
        writer.append(parseJsonToAvro(inJson, SchemaInstance.avroSchema))
        writer.close()
    } as StreamCallback)
} catch (Exception e) {
    log.error('Failed to convert the TIKA result to Avro: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
    return
}

flowFile = session.putAttribute(flowFile, 'mime.type', 'application/avro-binary')
session.transfer(flowFile, REL_SUCCESS)
