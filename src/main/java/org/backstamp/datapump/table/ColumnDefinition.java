package org.backstamp.datapump.table;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public interface ColumnDefinition {

	public String name();

	public String toSQL();

	public boolean isLob();

	public Object decode(byte[] field);

	public class Column<T> implements ColumnDefinition {

		private String name;
		private ColumnDataType<T> dataType;

		public Column(String name, ColumnDataType<T> dataType) {
			this.name = name;
			this.dataType = dataType;
		}
		
		public String name() {
			return name;
		}

		protected ColumnDataType<T> dataType() {
			return dataType;
		}

		public String toString() {
			return name;
		}

		@Override
		public T decode(byte[] field) {
			List<String> p = IntStream.range(0, field.length)
					.mapToObj(j -> Integer.toHexString(Byte.toUnsignedInt(field[j])))
					.map(j-> "0".concat(j).substring(j.length() - 1))
					.collect(Collectors.toList());
			
			throw new IllegalStateException(String.format(
				"The column %s cannot be decoded for [%s].",
				name,
				p));
		}

		@Override
		public String toSQL() {
			return String.format("%s %s", name, dataType.toTypeSyntax());
		}

		@Override
		public boolean isLob() {
			return dataType.isLob();
		}
	}

	public class NumberColumn extends Column<Long> {
		public NumberColumn(String name, ColumnDataType<Long> type) {
			super(name, type);
		}

		@Override
		public Long decode(byte[] encoding) {
			return new NumericBytes(encoding).decode();
		}
	}

	public class StringColumn extends Column<String> {
		public StringColumn(String name, ColumnDataType<String> type) {
			super(name, type);
		}

		@Override
		public String decode(byte[] field) {
			return new String(field, 0, field.length);
		}
	}

	public class RawColumn extends Column<byte[]> {
		public RawColumn(String name, ColumnDataType<byte[]> type) {
			super(name, type);
		}

		@Override
		public byte[] decode(byte[] bytes) {
			return bytes;
		}
	}


	public class TimestampColumn extends Column<Date> {

		public TimestampColumn(String name, ColumnDataType<Date> type) {
			super(name, type);
		}

		@Override
		public Date decode(byte[] encoding) {
			return new TimestampBytes(encoding).decode();
		}
	}

	public class NumericBytes {
		byte[] encoding;
		
		public NumericBytes(byte[] encoding) {
			this.encoding = encoding;
		}

		public long decode() {
			final boolean isPositive = (encoding[0]
				& 0xffffff80) != 0;
			final byte a = (byte) (((isPositive ? encoding[0]
				: ~encoding[0])
				& 0xffffff7f)
				- 64);
			final byte b = (byte) (encoding.length
				- 1 - (isPositive
					|| (encoding.length == 21
						&& encoding[20] != 0x66) ? 0
							: 1));
	
			long value = 0L;
	
			// Same as Math.min(a, b); b <= a ? b : a;
			final int minimum = b ^ ((a ^ b) & -(a < b ? 1 : 0)); 
			if (isPositive) {
				switch (minimum) {
					case 9: throw new IllegalStateException();
					case 8: throw new IllegalStateException();
					case 7: value = value * 100L + encoding[minimum - 6] - 1;
					case 6: value = value * 100L + encoding[minimum - 5] - 1;
					case 5: value = value * 100L + encoding[minimum - 4] - 1;
					case 4: value = value * 100L + encoding[minimum - 3] - 1;
					case 3: value = value * 100L + encoding[minimum - 2] - 1;
					case 2: value = value * 100L + encoding[minimum - 1] - 1;
					case 1: value = value * 100L + encoding[minimum - 0] - 1;
					default: break;
				}
			} else {
				switch (minimum) {
					case 9: throw new IllegalStateException();
					case 8: throw new IllegalStateException();
					case 7: value = value * 100L - encoding[minimum - 6] + 101;
					case 6: value = value * 100L - encoding[minimum - 5] + 101;
					case 5: value = value * 100L - encoding[minimum - 4] + 101;
					case 4: value = value * 100L - encoding[minimum - 3] + 101;
					case 3: value = value * 100L - encoding[minimum - 2] + 101;
					case 2: value = value * 100L - encoding[minimum - 1] + 101;
					case 1: value = value * 100L - encoding[minimum - 0] + 101;
					default: break;
				}
			}
			switch (a - b) {
				case 10: throw new IllegalStateException();
				case 9: value *= 100L; 
				case 8: value *= 100L; 
				case 7: value *= 100L; 
				case 6: value *= 100L; 
				case 5: value *= 100L; 
				case 4: value *= 100L; 
				case 3: value *= 100L; 
				case 2: value *= 100L; 
				case 1: value *= 100L; 
				default: break;
			}
			return isPositive ? value : -value;
		}
	}

	public class TimestampBytes {
		byte[] encoding;
		
		public TimestampBytes(byte[] encoding) {
			this.encoding = encoding;
		}

		public Date decode() {
			Calendar calendar = Calendar.getInstance();
			calendar.clear();
			calendar.set(1, (0xff & encoding[0] - 100) * 100 + (0xff & encoding[1] - 100));
			calendar.set(2,  0xff & encoding[2] - 1);
			calendar.set(5,  0xff & encoding[3]);
			calendar.set(11, 0xff & encoding[4] - 1);
			calendar.set(12, 0xff & encoding[5] - 1);
			calendar.set(13, 0xff & encoding[6] - 1);
			calendar.set(14, 0);
			return calendar.getTime();
		}
	}
}
