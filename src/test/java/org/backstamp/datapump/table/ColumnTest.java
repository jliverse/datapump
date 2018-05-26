package org.backstamp.datapump.table;

import java.util.Date;

import org.backstamp.datapump.table.ColumnDefinition.NumberColumn;
import org.backstamp.datapump.table.ColumnDefinition.StringColumn;
import org.backstamp.datapump.table.ColumnDefinition.TimestampColumn;

import junit.framework.TestCase;

public class ColumnTest extends TestCase {

	public void testDecodeWithNumbers() {
		NumberColumn column = new ColumnDefinition.NumberColumn("ID", ColumnDataType.NUMBER);
		assertTrue(0L == column.decode(new byte[] {
				(byte) 0x80 }));
		assertTrue(100L == column.decode(new byte[] {
				(byte) 0xc2, 0x02 }));
		assertTrue(101L == column.decode(new byte[] {
				(byte) 0xc2, 0x02, 0x02 }));
		assertTrue(867L == column.decode(new byte[] {
				(byte) 0xc2, 0x09, 0x44 }));
	}

	public void testDecodeWithTimestamps() {
		TimestampColumn column = new ColumnDefinition.TimestampColumn("TSTAMP", null);
		assertEquals(new Date(1456108080000L), column.decode(new byte[] {
				0x78, 0x74, 0x02, 0x15, 0x16, 0x1d, 0x01 }));
	}

	public void testDecodeWithStrings() {
		StringColumn column = new ColumnDefinition.StringColumn("DESCRIPTION", ColumnDataType.VARCHAR2);
		assertEquals(
			"Employee works from home",
			column.decode(new byte[] {
					0x45, 0x6d, 0x70, 0x6c, 0x6f, 0x79, 0x65, 0x65, 0x20, 0x77, 0x6f, 0x72, 0x6b,
					0x73, 0x20, 0x66, 0x72, 0x6f, 0x6d, 0x20, 0x68, 0x6f, 0x6d, 0x65 }));
	}
}
