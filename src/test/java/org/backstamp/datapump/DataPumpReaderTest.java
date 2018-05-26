package org.backstamp.datapump;

import org.backstamp.datapump.DataPumpFileReader.FileVersion;
import org.backstamp.datapump.util.Reader;

import junit.framework.TestCase;

public class DataPumpReaderTest extends TestCase {

	public void testFileVersionFromBytes() {
		FileVersion version = new FileVersion(new Reader.StaticReader(0x401));
		assertEquals("Oracle 12c Release 1: 12.1.0", version.description());
		version.value().ifPresent(i -> assertEquals(new Integer(1025), i));
	}
}
