package org.backstamp.datapump.table;

import java.util.Optional;

public interface ColumnDataType<T> {
	String name();

	boolean defaulted();

	boolean hasLength();

	boolean hasPrecision();

	boolean hasScale();

	boolean isLob();

	boolean nullable();

	int length();

	int precision();

	int scale();

	Optional<T> defaultValue();

	String toTypeSyntax();

	ColumnDataType<T> defaultValue(Optional<T> defaultValue);

	ColumnDataType<T> length(int length);

	ColumnDataType<T> nullable(boolean nullable);

	ColumnDataType<T> precision(int precision, int scale);

	ColumnDataType<T> precision(int precision);

	ColumnDataType<T> scale(int scale);

	public static final ColumnDataType<String> VARCHAR = new DefaultDataType<String>("varchar", String.class);
	public static final ColumnDataType<String> VARCHAR2 = new DefaultDataType<String>("varchar2", String.class);
	public static final ColumnDataType<Long> NUMBER = new DefaultDataType<Long>("number", Long.class);

	public class DefaultDataType<T> implements ColumnDataType<T> {
	
		private final String typeName;
		private final Class<T> type;
		private final Optional<T> defaultValue;
		private final boolean nullable;
		private final int precision;
		private final int scale;
		private final int length;
			
		public DefaultDataType(String typeName, Class<T> type, Optional<T> defaultValue,
			boolean nullable, int precision, int scale, int length) {
			this.typeName = typeName;
			this.type = type;
			this.defaultValue = defaultValue;
			this.nullable = nullable;
			this.precision = precision;
			this.scale = scale;
			this.length = length;
		}
	
		public DefaultDataType(String typeName, Class<T> type) {
			this(typeName, type, Optional.empty(), true, 0, 0, 0);
		}
	
		private DefaultDataType(DefaultDataType<T> t, Optional<T> defaultValue) {
			this(t.typeName, t.type, defaultValue, t.nullable, t.precision, t.scale, t.length);
		}
		
		@Override
		public boolean defaulted() {
			return defaultValue.isPresent();
		}
	
		@Override
		public boolean nullable() {
			return nullable;
		}
	
		@Override
		public Optional<T> defaultValue() {
			return defaultValue;
		}
	
		@Override
		public ColumnDataType<T> defaultValue(Optional<T> defaultValue) {
			return new DefaultDataType<T>(this, defaultValue);
		}
	
		@Override
		public ColumnDataType<T> nullable(boolean nullable) {
			if (this.nullable == nullable) {
				return this;
			}
			return new DefaultDataType<T>(
					this.typeName,
					this.type,
					this.defaultValue,
					nullable,
					this.precision,
					this.scale,
					this.length);
		}
		
		@Override
		public final ColumnDataType<T> precision(int precision, int scale) {
			if (this.precision == precision && this.scale == scale) {
				return this;
			}
			return new DefaultDataType<T>(
					this.typeName,
					this.type,
					this.defaultValue,
					this.nullable,
					precision,
					scale,
					this.length);
		}
	
		@Override
		public ColumnDataType<T> length(int length) {
			if (this.length == length) {
				return this;
			}
			return new DefaultDataType<T>(
					this.typeName,
					this.type,
					this.defaultValue,
					this.nullable,
					this.precision,
					this.scale,
					length);
		}
	
		@Override
		public ColumnDataType<T> precision(int precision) {
			return precision(precision, this.scale);
		}
	
		@Override
		public ColumnDataType<T> scale(int scale) {
			return precision(this.precision, scale);
		}
	
		@Override
		public int length() {
			return length;
		}
	
		@Override
		public int precision() {
			return precision;
		}
	
		@Override
		public int scale() {
			return scale;
		}
	
		@Override
		public String name() {
			return typeName;
		}
		
		public String toTypeSyntax() {
			StringBuilder sb = new StringBuilder();
			if (hasPrecision() && hasScale()) {
				if (scale != 0) {
					sb.append(String.format("%s(%d,%d)", typeName, precision, scale));
				} else {
					sb.append(String.format("%s(%d)", typeName, precision));
				}
			} else if (hasLength()) {
				sb.append(String.format("%s(%d)", typeName, length));
			} else {
				sb.append(typeName);
			}
			if (!nullable()) {
				sb.append(" not null");
			}
			if (defaulted()) {
				sb.append(" default ").append(defaultValue.get());
			}
			return sb.toString();
		}
	
		public boolean isLob() {
			return typeName.toLowerCase().endsWith("lob");		
		}
		
		@Override
		public boolean hasLength() {
			return (type == byte[].class || type == String.class) && !isLob();
		}
	
		@Override
		public boolean hasPrecision() {
			return Number.class.isAssignableFrom(type);
		}
	
		@Override
		public boolean hasScale() {
			return Number.class.isAssignableFrom(type);
		}
	}
}
