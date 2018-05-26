package org.backstamp.datapump.xml;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.backstamp.datapump.table.ColumnDataType;
import org.backstamp.datapump.table.ColumnDefinition;
import org.backstamp.datapump.table.ColumnDataType.DefaultDataType;
import org.backstamp.datapump.table.TableDefinition.Table;

public interface XMLStreamCursor<T> {

	public T apply(XMLStreamReader reader) throws XMLStreamException;

	public class DocumentCursor implements XMLStreamCursor<Optional<Table>> {

		@Override
		public Optional<Table> apply(XMLStreamReader reader) throws XMLStreamException {
			Optional<Table> result = Optional.empty();
			while (reader.hasNext()) {
				int code = reader.next();
				switch (code) {
				case XMLStreamReader.START_ELEMENT:
					if ("STRMTABLE_T".equals(reader.getLocalName())) {
						result = Optional.of(new TableCursor().apply(reader));
					}
					break;
				default:
					break;
				}
			}
			return result;
		}

		public Optional<Table> apply(InputStream inputStream) throws XMLStreamException {
			XMLInputFactory factory = XMLInputFactory.newInstance();
			XMLStreamReader reader = factory.createXMLStreamReader(inputStream);
			Optional<Table> optional = apply(reader);
			reader.close();
			return optional;
		}

	}

	public class TableCursor implements XMLStreamCursor<Table> {

		String charset;
		String name;
		String ownerName;

		public Consumer<String> withNodeName(String nodeName) {
			switch (nodeName) {
				case "CHARSET": return t -> charset = t;
				// case "ENDIANNESS": return t -> {};
				case "NAME": return t -> name = t;
				case "OWNER_NAME": return t -> ownerName = t;
				default: return t -> {};
			}
		}

		public Table apply(XMLStreamReader reader) throws XMLStreamException {
			Consumer<String> unassignedConsumer = t -> {};
			Consumer<String> consumer = unassignedConsumer;

			List<ColumnDefinition> columns = Arrays.asList();

			QName qualifiedName = reader.getName();

			boolean hasNext = true;
			while (reader.hasNext() && hasNext) {
				int code = reader.next();
				switch (code) {
				case XMLStreamReader.START_ELEMENT:
					consumer = withNodeName(reader.getLocalName());
					if ("COL_LIST".equals(reader.getLocalName())) {
						columns = new ColumnListCursor().apply(reader);
					}
					break;
				case XMLStreamReader.END_ELEMENT:
					consumer = unassignedConsumer;
					if (reader.getName().equals(qualifiedName)) {
						hasNext = false;
					}
					break;
				case XMLStreamReader.CHARACTERS:
				case XMLStreamReader.CDATA:
				case XMLStreamReader.SPACE:
					consumer.accept(reader.getText());
					break;
				default:
					break;
				}
			}
			return new Table(name).builder()
				.ownerName(ownerName)
				.columns(columns)
				.table();
		}
	}

	public class ColumnCursor implements XMLStreamCursor<ColumnDefinition> {

		String name;
		String type;
		String length;
		String precision;
		String scale;
		boolean nullable;

		public Consumer<String> withNodeName(String nodeName) {
			switch (nodeName) {
			case "NAME": return t -> name = t;
			case "TYPE_NUM": return t -> type = t;
			case "LENGTH": return t -> length = t;
			case "NOT_NULL": return t -> nullable = "0".equals(t); // 0, 1, 2?
			case "PRECISION_NUM": return t -> precision = t;
			case "SCALE": return t -> scale = t;
			default: return t -> {};
			}
		}

		public ColumnDefinition apply(XMLStreamReader reader) throws XMLStreamException {

			Consumer<String> unassignedConsumer = t -> {};
			Consumer<String> consumer = unassignedConsumer;

			QName qualifiedName = reader.getName();

			boolean hasNext = true;
			while (reader.hasNext() && hasNext) {
				int code = reader.next();
				switch (code) {
				case XMLStreamReader.START_ELEMENT:
					consumer = withNodeName(reader.getLocalName());
					break;
				case XMLStreamReader.END_ELEMENT:
					consumer = unassignedConsumer;
					if (reader.getName().equals(qualifiedName)) {
						hasNext = false;
					}
					break;
				case XMLStreamReader.CHARACTERS:
				case XMLStreamReader.CDATA:
				case XMLStreamReader.SPACE:
					consumer.accept(reader.getText());
					break;
				default:
					break;
				}
			}

			// Create matching column definitions from Oracle's Data Type Codes. 
			// https://docs.oracle.com/cd/E11882_01/appdev.112/e10646/oci03typ.htm#LNOCI030
			
			switch (type) {
			case "1": // VARCHAR
				return new ColumnDefinition.StringColumn(name, ColumnDataType.VARCHAR2
							.length(Integer.parseInt(length))
							.nullable(nullable));
			case "2": // NUMBER
				ColumnDataType<Long> numericType = new DefaultDataType<Long>("number", Long.class);
				if (!Objects.isNull(precision)
					&& !Objects.isNull(scale)) {
					numericType = numericType.precision(
						Integer.parseInt(precision),
						Integer.parseInt(scale));
				}
				return new ColumnDefinition.NumberColumn(name, numericType
						.nullable(nullable));
			case "12": // DATE
				return new ColumnDefinition.TimestampColumn(name, new DefaultDataType<Date>("date",
					Date.class)
						.nullable(nullable));
			case "23": // RAW
				return new ColumnDefinition.RawColumn(name, new DefaultDataType<byte[]>("raw",
					byte[].class)
						.nullable(nullable));
			case "96": // CHAR
				return new ColumnDefinition.StringColumn(name, ColumnDataType.VARCHAR
						.length(Integer.parseInt(length))
						.nullable(nullable));
			case "112": // CLOB
				return new ColumnDefinition.RawColumn(name, new DefaultDataType<byte[]>("clob",
					byte[].class)
						.nullable(nullable)
						.length(Integer.parseInt(length)));
			case "113": // BLOB
				return new ColumnDefinition.RawColumn(name, new DefaultDataType<byte[]>("blob",
					byte[].class)
						.nullable(nullable)
						.length(Integer.parseInt(length)));
			default:
				return new ColumnDefinition.RawColumn(name, new DefaultDataType<byte[]>("raw",
					byte[].class)
						.nullable(nullable)
						.length(Integer.parseInt(length)));
			}
		}
	}

	public class ColumnListCursor implements XMLStreamCursor<Iterable<ColumnDefinition>> {

		public List<ColumnDefinition> apply(XMLStreamReader reader) throws XMLStreamException {
			QName qualifiedName = reader.getName();

			List<ColumnDefinition> list = new ArrayList<>();
			boolean hasNext = true;
			while (reader.hasNext() && hasNext) {
				int code = reader.nextTag();
				switch (code) {
				case XMLStreamReader.START_ELEMENT:
					if ("COL_LIST_ITEM".equals(reader.getLocalName())) {
						list.add(new ColumnCursor().apply(reader));
					}
					break;
				case XMLStreamReader.END_ELEMENT:
					if (reader.getName().equals(qualifiedName)) {
						hasNext = false;
					}
					break;
				default:
					break;
				}
			}

			return list;
		}
	}
}
