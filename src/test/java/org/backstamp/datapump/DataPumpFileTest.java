package org.backstamp.datapump;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.backstamp.datapump.io.TemporaryFile;
import org.backstamp.datapump.row.MasterTableRow;

import junit.framework.TestCase;

public class DataPumpFileTest extends TestCase {

	/**
	 * Test that we can extract the version and other details about the export.
	 * @throws Exception
	 */
	public void testScottMetadata() throws Exception {
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		try (TemporaryFile file = new TemporaryFile(cl.getResourceAsStream("scott.dmp"));) {
			DataPumpFile dumpFile = new DataPumpFile(file.toFile());
			assertEquals("Oracle 12c Release 1: 12.1.0", dumpFile.versionName());
			assertEquals("Wed May 23 14:34:07 EDT 2018", dumpFile.date().toString());
			assertEquals("AL32UTF8", dumpFile.characterSet());
			assertEquals(4096L, dumpFile.blockSize());
			assertEquals(true, dumpFile.master());
		}
	}

	public void testScottMasterTableClientCommand() throws Exception {
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		try (TemporaryFile file = new TemporaryFile(cl.getResourceAsStream("scott.dmp"));) {
			// Find CLIENT_COMMAND rows in the master table.
			Predicate<MasterTableRow> withClientCommand = row -> "CLIENT_COMMAND".equals(row.name());

			DataPumpFile dumpFile = new DataPumpFile(file.toFile());
			String clientCommand = dumpFile.masterTable().rowsMatching(withClientCommand)
					.map(MasterTableRow::value_t)
					.collect(Collectors.joining());
			assertEquals(
				"\"sys/******** AS SYSDBA\" directory=impdp"
					+ " schemas=scott logfile=scott.log dumpfile=scott.dmp ",
				clientCommand);
		}
	}

	public void testScottMasterTableObjects() throws Exception {
		
		Predicate<MasterTableRow> predicate = row -> row.processOrder() > 0
				&& row.duplicate() == 0
				&& "C".equals(row.processingStatus())
				&& "R".equals(row.processingState());
		
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		try (TemporaryFile file = new TemporaryFile(cl.getResourceAsStream("scott.dmp"));) {
			DataPumpFile dumpFile = new DataPumpFile(file.toFile());
			List<String> processingTypes = dumpFile.masterTable().rowsMatching(predicate)
					.map(i -> String.format("Processing object type %s %s", i.objectTypePath(),
							Objects.toString(i.objectName(), "")))
					.collect(Collectors.toList());
			assertEquals(26, processingTypes.size());
		}
	}

	public void testScottMasterTableXML() throws Exception {
		
		Predicate<MasterTableRow> predicate = row -> !Objects.isNull(row.xmlString());

		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		try (TemporaryFile file = new TemporaryFile(cl.getResourceAsStream("scott.dmp"));) {
			DataPumpFile dumpFile = new DataPumpFile(file.toFile());
			DataPumpTable<MasterTableRow> table = dumpFile.masterTable();

			Object[] lobLengths = table.rowsMatching(predicate)
					.map(i -> Objects.toString(i.xmlString()).length())
					.toArray();
			assertEquals(
				Arrays.toString(new Object[] { 7935, 12628, 4287, 3728 }),
				Arrays.toString(lobLengths));
		}
	}

	public void testScottMasterTableRows() throws Exception {
		
		Predicate<MasterTableRow> predicate = row -> row.processOrder() > 0
				&& row.duplicate() == 0
				&& "C".equals(row.processingStatus());
		
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		try (TemporaryFile file = new TemporaryFile(cl.getResourceAsStream("scott.dmp"));) {
			DataPumpFile dumpFile = new DataPumpFile(file.toFile());
			DataPumpTable<MasterTableRow> table = dumpFile.masterTable();

			List<String> columns = table.get().columns().stream()
				.map(i -> i.name())
				.collect(Collectors.toList());
			
			List<String> rows = table.rowsMatching(predicate)
					.map(i -> i.toString())
					.collect(Collectors.toList());

			assertEquals(100, columns.size());
			assertEquals(26, rows.size());
		}
	}

	/**
	 * Test the processing of tables using Java 8 Stream API methods. 
	 * @throws Exception
	 */
	public void testScottTables() throws Exception {
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		try (TemporaryFile file = new TemporaryFile(cl.getResourceAsStream("scott.dmp"));) {

			DataPumpFile dumpFile = new DataPumpFile(file.toFile());
			String names = dumpFile.tables()
					.map(t -> t.get().name())
					.sorted()
					.collect(Collectors.joining(", "));
			assertEquals(names, "DEPT, EMP, SALGRADE");
		}
	}
}
