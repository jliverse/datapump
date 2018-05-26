package org.backstamp.datapump;

import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.backstamp.datapump.row.MasterTableRow;
import org.backstamp.datapump.row.TableRow;
import org.backstamp.datapump.util.Reader;
import org.backstamp.datapump.util.Value;

public class DataPumpFile implements DataPumpFileReader {

	private Reader reader;
	private Optional<List<?>> values;
	private Map<MasterTableRow, Long> tableOffsets;

	public DataPumpFile(File file) {
		this.reader = new Reader.RandomAccessFileReader(file);
		this.values = Optional.empty();
	}

	public String versionName() {
		return FileVersion.class.cast(read().get(0)).description();
	}

	public String characterSet() {
		return CharacterSet.class.cast(read().get(6)).description();
	}

	public Date date() {
		return CreationDate.class.cast(read().get(7)).get();
	}

	public long blockSize() {
		return BlockSize.class.cast(read().get(5)).value().orElse(4096L);
	}

	public boolean master() {
		return MasterPresent.class.cast(read().get(2)).get();
	}

	public DataPumpTable<MasterTableRow> masterTable() {
		long blockSize = BlockSize.class.cast(read().get(5)).get();
		long offset = MasterOffset.class.cast(read().get(9)).get();
		return new DataPumpTable<>(reader, offset * blockSize, MasterTableRow.class);
	}

	public Stream<DataPumpTable<TableRow>> tables() {
		return masterTableOffsets().entrySet().stream()
				.mapToInt(i -> i.getValue().intValue())
				.mapToObj(i -> new DataPumpTable<>(reader, i, TableRow.class));
	}

	public DataPumpTable<TableRow> tableWithName(String name) {
		return tableMatching(i -> i.objectName().equalsIgnoreCase(name));
	}

	public DataPumpTable<TableRow> tableMatching(Predicate<MasterTableRow> predicate) {
		OptionalInt optionalOffset = masterTableOffsets().entrySet().stream()
				.filter(i -> predicate.test(i.getKey()))
				.mapToInt(i -> i.getValue().intValue())
				.findFirst();
		if (!optionalOffset.isPresent()) {
			throw new IllegalStateException(String.format("There is no table matching '%s'.", predicate));
		}
		return new DataPumpTable<>(reader, optionalOffset.getAsInt(), TableRow.class);
	}

	private List<?> read() {
		if (!values.isPresent()) {
			List<Value<?>> order = Arrays.asList(
					new FileVersion(reader),
					new Unknown(reader, 11),
					new MasterPresent(reader),
					new GUID(reader),
					new FileNumber(reader),
					new BlockSize(reader),
					new CharacterSet(reader),
					new CreationDate(reader),
					new Unknown(reader, 6),
					new MasterOffset(reader),
					new MasterSize(reader));

			values = Optional.of(order.stream()
					.peek(i -> i.get())
					.collect(Collectors.toList()));
		}
		return values.get();
	}

	private Map<MasterTableRow, Long> masterTableOffsets() {
		if (tableOffsets == null) {
			tableOffsets = masterTable().rows()
				.filter(row -> row.processOrder() > 0
					&& "SCHEMA_EXPORT/TABLE/TABLE_DATA".equals(row.objectTypePath()))
				.collect(Collectors.groupingBy(MasterTableRow::processOrder, Collectors.toList()))
				.entrySet()
				.stream()
				.filter(i -> i.getValue().size() == 2)
				.collect(Collectors.toMap(
						e -> e.getValue().get(0),
						e -> e.getValue().get(1).dumpPosition() * blockSize(),
						(u, v) -> { throw new IllegalStateException(); },
						LinkedHashMap::new));
		}
		return tableOffsets;
	}
}
