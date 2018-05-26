package org.backstamp.datapump.xml;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;

import org.backstamp.datapump.table.TableDefinition.Table;
import org.xml.sax.SAXException;

import junit.framework.TestCase;

public class XMLStreamCursorTest extends TestCase {

	public void testMasterTable() throws XMLStreamException,
		ParserConfigurationException,
		SAXException,
		IOException {

		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		InputStream inputStream = cl.getResourceAsStream(
			"org/backstamp/datapump/xml/SYS_EXPORT_SCHEMA_01.xml");

		Optional<Table> table = new XMLStreamCursor.DocumentCursor().apply(inputStream);
		assertTrue(table.isPresent());

		table.ifPresent(t -> {
			String columnNames = t.columns().stream().map(i -> i.toString()).collect(
				Collectors.joining(", "));
			assertEquals("SYS_EXPORT_SCHEMA_01", t.name());
			assertEquals(
				Arrays.asList(
					"PROCESS_ORDER",
					"DUPLICATE",
					"DUMP_FILEID",
					"DUMP_POSITION",
					"DUMP_LENGTH",
					"DUMP_ALLOCATION",
					"COMPLETED_ROWS",
					"ERROR_COUNT",
					"ELAPSED_TIME",
					"OBJECT_TYPE_PATH",
					"OBJECT_PATH_SEQNO",
					"OBJECT_TYPE",
					"IN_PROGRESS",
					"OBJECT_NAME",
					"OBJECT_LONG_NAME",
					"OBJECT_SCHEMA",
					"ORIGINAL_OBJECT_SCHEMA",
					"PARTITION_NAME",
					"SUBPARTITION_NAME",
					"FLAGS",
					"PROPERTY",
					"COMPLETION_TIME",
					"OBJECT_TABLESPACE",
					"SIZE_ESTIMATE",
					"OBJECT_ROW",
					"PROCESSING_STATE",
					"PROCESSING_STATUS",
					"BASE_PROCESS_ORDER",
					"BASE_OBJECT_TYPE",
					"BASE_OBJECT_NAME",
					"BASE_OBJECT_SCHEMA",
					"ANCESTOR_PROCESS_ORDER",
					"DOMAIN_PROCESS_ORDER",
					"PARALLELIZATION",
					"UNLOAD_METHOD",
					"GRANULES",
					"SCN",
					"GRANTOR",
					"XML_CLOB",
					"NAME",
					"VALUE_T",
					"VALUE_N",
					"IS_DEFAULT",
					"FILE_TYPE",
					"USER_DIRECTORY",
					"USER_FILE_NAME",
					"FILE_NAME",
					"EXTEND_SIZE",
					"FILE_MAX_SIZE",
					"PROCESS_NAME",
					"LAST_UPDATE",
					"WORK_ITEM",
					"OBJECT_NUMBER",
					"COMPLETED_BYTES",
					"TOTAL_BYTES",
					"METADATA_IO",
					"DATA_IO",
					"CUMULATIVE_TIME",
					"PACKET_NUMBER",
					"OLD_VALUE",
					"SEED",
					"LAST_FILE",
					"USER_NAME",
					"OPERATION",
					"JOB_MODE",
					"CONTROL_QUEUE",
					"STATUS_QUEUE",
					"REMOTE_LINK",
					"VERSION",
					"DB_VERSION",
					"TIMEZONE",
					"STATE",
					"PHASE",
					"GUID",
					"START_TIME",
					"BLOCK_SIZE",
					"METADATA_BUFFER_SIZE",
					"DATA_BUFFER_SIZE",
					"DEGREE",
					"PLATFORM",
					"ABORT_STEP",
					"INSTANCE")
					.stream()
					.collect(Collectors.joining(", ")),
				columnNames);
		});
	}
}
