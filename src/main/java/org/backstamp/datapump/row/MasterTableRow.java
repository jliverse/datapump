package org.backstamp.datapump.row;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.stream.Collectors;

import org.backstamp.datapump.table.ColumnDefinition;
import org.backstamp.datapump.table.TableDefinition.Table;

public class MasterTableRow extends TableRow {

	@SuppressWarnings("unused")
	private int PROCESS_ORDER, DUPLICATE, DUMP_FILEID, DUMP_POSITION, DUMP_LENGTH, DUMP_ALLOCATION,
		COMPLETED_ROWS, ERROR_COUNT, ELAPSED_TIME, OBJECT_TYPE_PATH, OBJECT_PATH_SEQNO, OBJECT_TYPE,
		IN_PROGRESS, OBJECT_NAME, OBJECT_LONG_NAME, OBJECT_SCHEMA, ORIGINAL_OBJECT_SCHEMA,
		PARTITION_NAME, SUBPARTITION_NAME, FLAGS, PROPERTY, COMPLETION_TIME, OBJECT_TABLESPACE,
		SIZE_ESTIMATE, OBJECT_ROW, PROCESSING_STATE, PROCESSING_STATUS, BASE_PROCESS_ORDER,
		BASE_OBJECT_TYPE, BASE_OBJECT_NAME, BASE_OBJECT_SCHEMA, ANCESTOR_PROCESS_ORDER,
		DOMAIN_PROCESS_ORDER, PARALLELIZATION, UNLOAD_METHOD, GRANULES, SCN, GRANTOR, XML_CLOB,
		NAME, VALUE_T, VALUE_N, IS_DEFAULT, FILE_TYPE, USER_DIRECTORY, USER_FILE_NAME, FILE_NAME,
		EXTEND_SIZE, FILE_MAX_SIZE, PROCESS_NAME, LAST_UPDATE, WORK_ITEM, OBJECT_NUMBER,
		COMPLETED_BYTES, TOTAL_BYTES, METADATA_IO, DATA_IO, CUMULATIVE_TIME, PACKET_NUMBER,
		OLD_VALUE, SEED, LAST_FILE, USER_NAME, OPERATION, JOB_MODE, CONTROL_QUEUE, STATUS_QUEUE,
		REMOTE_LINK, VERSION, DB_VERSION, TIMEZONE, STATE, PHASE, GUID, START_TIME, BLOCK_SIZE,
		METADATA_BUFFER_SIZE, DATA_BUFFER_SIZE, DEGREE, PLATFORM, ABORT_STEP, INSTANCE,
		TARGET_XML_CLOB;
	
	public MasterTableRow(Object[] row, Table tableDefinition) {
		super(row, tableDefinition);

		// TODO: Convert to a single-pass assignment scheme.
		List<String> names = tableDefinition.columns().stream()
			.map(ColumnDefinition::toString)
			.collect(Collectors.toList());

		PROCESS_ORDER = names.indexOf("PROCESS_ORDER");
		VALUE_T = names.indexOf("VALUE_T");
		XML_CLOB = names.indexOf("XML_CLOB");
		NAME = names.indexOf("NAME");
		PLATFORM = names.indexOf("PLATFORM");
		SIZE_ESTIMATE = names.indexOf("SIZE_ESTIMATE");
		OBJECT_TYPE = names.indexOf("OBJECT_TYPE");
		OBJECT_SCHEMA = names.indexOf("OBJECT_SCHEMA");
		OBJECT_NAME = names.indexOf("OBJECT_NAME");
		COMPLETED_ROWS = names.indexOf("COMPLETED_ROWS");
		DUMP_POSITION = names.indexOf("DUMP_POSITION");
		OBJECT_TYPE_PATH = names.indexOf("OBJECT_TYPE_PATH");
		DUMP_ALLOCATION = names.indexOf("DUMP_ALLOCATION");
		DUPLICATE = names.indexOf("DUPLICATE");
		PROCESSING_STATUS = names.indexOf("PROCESSING_STATUS");
		PROCESSING_STATE = names.indexOf("PROCESSING_STATE");
		GRANTOR = names.indexOf("GRANTOR");
		TARGET_XML_CLOB = names.indexOf("TARGET_XML_CLOB");
	}

	public int processOrder() {
		return asInt(PROCESS_ORDER, Integer.MIN_VALUE); 
	}
	
	public String name() {
		return asString(NAME);
	}
	
	public String value_t() {
		return asString(VALUE_T);
	}

	public String platform() {
		return asString(PLATFORM);
	}

	public long sizeEstimate() {
		return asLong(SIZE_ESTIMATE, 0L);
	}

	public String objectName() {
		return asString(OBJECT_NAME);
	}

	public String objectType() {
		return asString(OBJECT_TYPE);
	}

	public String objectTypePath() {
		return asString(OBJECT_TYPE_PATH);
	}

	public String objectSchema() {
		return asString(OBJECT_SCHEMA);
	}

	public int completedRows() {
		return asInt(COMPLETED_ROWS, 0);
	}

	public long dumpPosition() {
		return asLong(DUMP_POSITION, 0L);
	}

	public long dumpAllocation() {
		return asLong(DUMP_ALLOCATION, 1L);
	}

	public InputStream xmlInputStream() {
		return new ByteArrayInputStream(asBytes(XML_CLOB));
	}

	public String xmlString() {
		byte[] bytes = asBytes(XML_CLOB);
		return new XMLString(bytes).toString();
	}

	public String targetXMLString() {
		byte[] bytes = asBytes(TARGET_XML_CLOB);
		return new XMLString(bytes).toString();
	}

	public int duplicate() {
		return asInt(DUPLICATE, Integer.MIN_VALUE); 
	}

	public String processingState() {
		return asString(PROCESSING_STATE);
	}

	public String processingStatus() {
		return asString(PROCESSING_STATUS);
	}

	public String grantor() {
		return asString(GRANTOR);
	}

	public class XMLString {
		private final byte[] bytes;

		XMLString(byte[] bytes) {
			this.bytes = bytes;
		}
		
		public String toString() {
			// These encoded XML strings will typically start with <?xml version="1.0"...
			if (bytes == null || bytes.length < 4) {
				return null;
			}
			try {
				switch (signature()) {
				case 0x003c003f:
					return new String(bytes, "UTF-16");
				default:
					return new String(bytes);
				}
			} catch (UnsupportedEncodingException e) {
				return new String(bytes);
			}
		}

		private int signature() {
			return (bytes[0] << 24
				| bytes[1] << 16
				| bytes[2] << 8
				| bytes[3]) & 0xffffffff;
		}
	}
}
