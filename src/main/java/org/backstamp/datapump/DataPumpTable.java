package org.backstamp.datapump;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.xml.stream.XMLStreamException;

import org.backstamp.datapump.row.TableRow;
import org.backstamp.datapump.table.ColumnDefinition;
import org.backstamp.datapump.table.TableDefinition.Table;
import org.backstamp.datapump.util.ArrayCombination.ByteArrays;
import org.backstamp.datapump.util.Reader;
import org.backstamp.datapump.util.Value.Once;
import org.backstamp.datapump.xml.XMLStreamCursor;

public class DataPumpTable<T extends TableRow> extends Once<Table> implements Iterable<T> {
	private Reader reader;
	private Class<T> type;
	
	public DataPumpTable(Reader reader, long offset, Class<T> type) {
		super(() -> {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			reader.seek(offset - 4096);

			// FFFF24240A00[XML LENGTH][BLOCK SIZE][DATA LENGTH] ...
			// [CHARSET ID][XML][DATA]
			
			reader.skip(6); // 0xffff24240a00
			long xmlLength = reader.read8();
			
			try {
				// TODO: Character sets other than the platform default.
				reader.seek(offset + 2); // Skip two bytes containing character set ID.
				baos.write(reader.read(xmlLength - 2));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}

			// Align to the next eight-byte boundary; same as (8 - (offset + xmlLength % 8)) % 8.
			long byteAlignment = -(offset + xmlLength) & (8 - 1); 
			reader.skip(byteAlignment);

			byte[] xml = baos.toByteArray();
			try (InputStream inputStream = new ByteArrayInputStream(xml)) {
				Optional<Table> document = new XMLStreamCursor.DocumentCursor().apply(inputStream);
				return document.get();
			} catch (IOException | XMLStreamException e) {
				throw new RuntimeException(e);
			}
		});
		this.reader = reader;
		this.type = type;
	}

	/* (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	public Iterator<T> iterator() {
		final Class<T> clazz = this.type;
		
		Optional<Table> document = value();
		if (!document.isPresent()) {
			return new Iterator<T>() {
				public boolean hasNext() { return false; }
				public T next() { throw new IllegalStateException(); }
			};
		}
		final Table table = document.get();

		final ColumnDefinition[] columns = table.columns().toArray(new ColumnDefinition[] {});
		int[] lobColumns = IntStream.range(0, columns.length).filter(i -> columns[i].isLob()).toArray();
		boolean hasLob = lobColumns.length > 0;
		final Object[] row = new Object[columns.length];

		AtomicInteger rowFlag = new AtomicInteger(0);
		rowFlag.set(reader.read1());
		if (rowFlag.get() == 0x3c) {
			reader.read2(); // Row length.
		}

		final AtomicInteger numberOfColumns = new AtomicInteger(reader.read1());

		return new Iterator<T>() {
			boolean hasNext = document.isPresent();

			@Override
			public boolean hasNext() {
				return hasNext;
			}

			@Override
			public T next() {
				Arrays.fill(row, null);

				for (int column = 0; column < Math.min(row.length, numberOfColumns.get()); column++) {
					int size = reader.read1();

					// LOBs often extend into one or more adjoining rows
					// unless indicated as inline with the 0xfe header.
					if (columns[column].isLob()
						&& size != 0xfe) {
						column++;
						numberOfColumns.incrementAndGet();
					}
				
					switch (size) {
					case 0x00:
						assert false;
						break;
					case 0xff: // NULL column value
						break;
					case 0xfe:
						int length = reader.read2();
						byte[] lob = reader.read(length);
						row[column] = columns[column].decode(lob);
						break;
					default:
						byte[] bytes = reader.read(size);
						row[column] = columns[column].decode(bytes);
						break;
					}
				}
				rowFlag.set(reader.read1());

				switch (rowFlag.get()) {
				case 0x3c:
					reader.read2(); // Row length.
					break;
				default:
					break;
				}

				// Handle LOB (BLOB, CLOB) column rows.
				// TODO: This code has a lot of confusing temporal cohesion.
				// An improved version could consume the Reader and collect
				// encoded rows to coalesce into a single result.
				if (hasLob
					&& acceptLob(rowFlag.get())) {
					int[] rowLobColumns = lobColumns.clone();

					int numberOfColumns = reader.read1();
					if (numberOfColumns > rowLobColumns.length) {
						// HACK: Consume and discard row.
						// TODO: This should be consumed using normal rules.
						for (int column = 0; column < numberOfColumns; column++) {
							reader.read1();
						}
						rowFlag.set(reader.read1());
						numberOfColumns = reader.read1();
					}

					// Combine LOBs that span multiple rows into the
					// existing array containing its decoded data.
					int i = 0;
					boolean shouldContinue = numberOfColumns > 0;
					while (shouldContinue) {
						int column = rowLobColumns[i];
						int size = reader.read1();
						switch (size) {
							case 0xff:
								break;
							case 0xfe:
								int length = reader.read2();
								byte[] newLob = (byte[]) columns[column].decode(
									reader.read(length));
								if ((rowFlag.get() == 0x06
									|| rowFlag.get() == 0x03)
									&& row[column] != null) {
									ByteArrays arrays = new ByteArrays(
										(byte[]) row[column],
										newLob);
									row[column] = arrays.toArray();
								} else {
									row[column] = newLob;
								}

							default:
								break;
						}
						rowFlag.set(reader.read1());
						if (rowFlag.get() == 0x01
							|| rowFlag.get() == 0x04) {
							// Advance if we have the start of a new LOB or a new complete LOB.
							i++;
						} else if (!acceptLob(rowFlag.get())) {
							break;
						}
						if (i >= Math.min(numberOfColumns, rowLobColumns.length)) {
							if (acceptLob(rowFlag.get())) {
								if (rowLobColumns.length == 1) {
									break;
								} else {
									rowLobColumns = Arrays.copyOfRange(
										rowLobColumns,
										1,
										rowLobColumns.length);
									i = 0;
								}
							} else {
								break;
							}
						}
						numberOfColumns = reader.read1();
					}
				}

				numberOfColumns.set(reader.read1());
				hasNext = (numberOfColumns.get() != 0x00);

				T current;
				try {
					Constructor<T> ctor = clazz.getConstructor(row.getClass(), table.getClass());
					current = ctor.newInstance(Arrays.copyOf(row, row.length), table);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				return current;
			}

			private boolean acceptLob(int rowFlag) {
				switch (rowFlag) {
					case 0x04: // Complete LOB.
					case 0x01: // Start of a multi-row LOB.
					case 0x03: // Middle of a multi-row LOB.
					case 0x06: // Last of a multi-row LOB.
					case 0x00: // Empty
						return true;
					default:
						return false;
				}
			}
		};
	}

	public Stream<T> rows() {
		return rowsMatching(row -> true);
	}

	public Stream<T> rowsMatching(Predicate<T> predicate) {
		Spliterator<T> s = Spliterators.spliteratorUnknownSize(iterator(), 0);
		return StreamSupport.stream(s, false).filter(predicate);
	}
}
