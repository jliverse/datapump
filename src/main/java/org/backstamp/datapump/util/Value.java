package org.backstamp.datapump.util;

import java.util.Optional;
import java.util.function.Supplier;

public interface Value<T> {
	T get();

	public abstract class Once<T> implements Value<T> {
		private Supplier<T> supplier;
		private Optional<T> value;

		public Once(Supplier<T> supplier) {
			this.supplier = supplier;
			this.value = Optional.empty();
		}

		public Optional<T> value() {
			if (!value.isPresent()) {
				value = Optional.ofNullable(supplier.get());
			}
			return value;
		}

		@Override
		public T get() {
			return value().orElseThrow(() -> new RuntimeException());
		}
	}
}
