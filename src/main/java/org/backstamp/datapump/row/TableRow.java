package org.backstamp.datapump.row;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.backstamp.datapump.table.ColumnDefinition;
import org.backstamp.datapump.table.TableDefinition.Table;

public class TableRow {
	
	private final Object[] row;
	private Map<String, Integer> map;
	private final Table table;

	public TableRow(Object[] row, Table tableDefinition) {
		this.row = row;
		this.table = tableDefinition;
	}

	protected byte[] asBytes(int column) {
		return (byte[]) row[column];
	}

	protected String asString(int column) {
		return Objects.toString(row[column], null);
	}

	protected String asString(String column) {
		return asString(map(column));
	}

	protected int asInt(int column, int defaultValue) {
		return Objects.isNull(row[column]) ? defaultValue : Long.class.cast(row[column]).intValue();
	}

	protected int asInt(String column, int defaultValue) {
		return asInt(map(column), defaultValue);
	}

	protected long asLong(int column, long defaultValue) {
		return Objects.isNull(row[column]) ? defaultValue : Long.class.cast(row[column]).longValue();
	}

	@Override
	public int hashCode() {
		return Arrays.stream(row).mapToInt(i -> i != null ? i.hashCode() % 51 : 0).sum();
	}

	@Override
	public String toString() {
		final List<ColumnDefinition> columns = table.columns();
		return IntStream.range(0, columns.size())
			.mapToObj(i -> columns.get(0).isLob() ? new String((byte[]) row[i])
				: Objects.toString(row[i], ""))
			.collect(Collectors.joining(","));
	}

	public void populateStatement(PreparedStatement s) throws SQLException {
		for (int i = 0; i < row.length; i++) {
			s.setObject(i + 1, row[i]);
		}
	}

	private int map(String column) {
		if (map == null) {
			final List<ColumnDefinition> columns = table.columns();
			map = IntStream.range(0, columns.size())
				.mapToObj(
					i -> new AbstractMap.SimpleEntry<String, Integer>(columns.get(i).name()
						.toLowerCase(), i))
				.collect(Collectors.toMap(i -> i.getKey(), i -> i.getValue()));
		}
		return map.get(column.toLowerCase());
	}

	public String tableName() {
		return table.name();
	}

	public boolean isEmpty() {
		for (int i = 0; i < row.length; i++) {
			if (!Objects.isNull(row[i])) {
				return false;
			}
		}
		return true;
	}
}
