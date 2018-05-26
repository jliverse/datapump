package org.backstamp.datapump.table;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public interface TableDefinition {

	public String name();

	public Iterable<ColumnDefinition> columns();

	public class Table implements TableDefinition {
		private String ownerName;
		private String name;
		private List<ColumnDefinition> columns = Arrays.asList();

		public Table(String name) {
			this.name = name;
		}
		
		public TableBuilder builder() {
			return new TableBuilder(this);
		}

		public Table(String name, List<ColumnDefinition> columns) {
			this(name);
			this.columns = columns;
		}

		@Override
		public String name() {
			return name;
		}

		@Override
		public List<ColumnDefinition> columns() {
			return columns;
		}

		public Object ownerName() {
			return ownerName;
		}

		public String toSQL() {
			String x = columns.stream().map(c -> c.toSQL()).collect(Collectors.joining(",\n  "));
			return String.format("CREATE TABLE %s (\n  %s)", name, x);
		}
		
		public String toSQLInsertSyntax() {
			String names = columns.stream().map(c -> c.name()).collect(Collectors.joining(", "));
			String binds = columns.stream().map(c -> "?").collect(Collectors.joining(", "));
			return String.format("INSERT INTO %s (%s) VALUES (%s)", name, names, binds);
		}
		
		public class TableBuilder {
			Table table;
			
			public TableBuilder(Table table) {
				this.table = table;
			}
			
			public TableBuilder name(String name) {
				table.name = name;
				return this;
			}
			public TableBuilder ownerName(String ownerName) {
				table.ownerName = ownerName;
				return this;
			}
			public TableBuilder columns(List<ColumnDefinition> columns) {
				table.columns = columns;
				return this;
			}
			public Table table() {
				return table;
			}
		}
	}
}
