package org.backstamp.datapump.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public interface Reader {
	int read1();

	int read2();

	long read4();

	long read8();

	byte[] read(long numberOfBytes);

	byte[] read(int offset, int numberOfBytes);

	public void skip(long charsetLength);

	public void seek(long l);

	public class StaticReader implements Reader {
		long value;

		public StaticReader(int value) {
			this.value = value;
		}

		@Override
		public int read1() {
			return new Long(value).intValue();
		}

		@Override
		public int read2() {
			return new Long(value).intValue();
		}

		@Override
		public long read4() {
			return value;
		}

		@Override
		public long read8() {
			return value;
		}

		@Override
		public void skip(long numberOfBytes) {
		}

		@Override
		public byte[] read(long numberOfBytes) {
			return new byte[] { new Long(value).byteValue() };
		}

		@Override
		public byte[] read(int offset, int numberOfBytes) {
			return new byte[] { new Long(value).byteValue() };
		}

		@Override
		public void seek(long numberOfBytes) {
		}
	}

	public class RandomAccessFileReader implements Reader, AutoCloseable {
		private RandomAccessFile file;
		
		public RandomAccessFileReader(File file) {
			try {
				this.file = new RandomAccessFile(file, "r");
			} catch (FileNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
		
		public void skip(long numberOfBytes) {
			try {
				file.skipBytes((int) numberOfBytes);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public int read1() {
			try {
				return file.readUnsignedByte();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public int read2() {
			try {
				return file.readUnsignedShort();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public long read4() {
			return (read2() << 16 | read2()) & 0xFFFFFFFFL;
		}

		@Override
		public long read8() {
			return (read4() << 32) + read4() & 0xFFFFFFFFFFFFFFFFL;
		}

		@Override
		public byte[] read(int offset, int numberOfBytes) {
			try {
				byte[] bytes = new byte[numberOfBytes];
				file.seek(offset);
				file.read(bytes);
				return bytes;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public byte[] read(long numberOfBytes) {
			try {
				byte[] bytes = new byte[(int) numberOfBytes];
				file.read(bytes);
				return bytes;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void seek(long numberOfBytes) {
			try {
				file.seek(numberOfBytes);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void close() throws Exception {
			this.file.close();
		}
	}
}
