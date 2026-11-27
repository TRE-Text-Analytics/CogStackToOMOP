/*

This script parses the TIKA response and combines the extracted text
with the source columns kept by prepare-db-record-for-tika-request-single-mimic.groovy
(FlowFile attributes named "src.<column>").

Output JSON:
  { <every source column>, "text": ..., "metadata": {...}, "timestamp": ... }

*/

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.commons.io.IOUtils
import org.apache.nifi.processor.io.StreamCallback
import java.nio.charset.StandardCharsets


def flowFile = session.get()
if (flowFile == null) {
    return
}

String SRC_PREFIX = 'src.'

// source columns, without the prefix
Map<String, String> sourceColumns = flowFile.getAttributes()
        .findAll { k, v -> k.startsWith(SRC_PREFIX) }
        .collectEntries { k, v -> [(k.substring(SRC_PREFIX.length())): v] }

if (!sourceColumns.note_id) {
    log.error('No src.note_id attribute: was the FlowFile prepared by prepare-db-record-for-tika-request-single-mimic.groovy?')
    session.transfer(flowFile, REL_FAILURE)
    return
}

try {
    flowFile = session.write(flowFile, { inputStream, outputStream ->

        def content = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        def inJson = new JsonSlurper().parseText(content)
        def result = inJson?.result ?: [:]

        def outJson = [:]
        outJson.putAll(sourceColumns)
        outJson.text = result.text ?: ''
        outJson.metadata = result.metadata ?: [:]
        outJson.timestamp = result.timestamp

        outputStream.write(JsonOutput.toJson(outJson).getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)
} catch (Exception e) {
    log.error("Failed to parse the TIKA response for note ${sourceColumns.note_id}: " + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
    return
}

flowFile = session.putAttribute(flowFile, 'mime.type', 'application/json')
session.transfer(flowFile, REL_SUCCESS)
