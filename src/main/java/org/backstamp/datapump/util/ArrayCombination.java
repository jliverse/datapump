package org.backstamp.datapump.util;

import java.lang.reflect.Array;

public interface ArrayCombination<T> {

	public T toArray();
	
	public class ObjectArrays<V> implements ArrayCombination<V[]> {
		private final V[] a;
		private final V[] b;

		public ObjectArrays(V[] a, V[] b) {
			this.a = a;
			this.b = b;
		}
		
		public V[] toArray() {
			try {
				@SuppressWarnings("unchecked")
				V[] bytes = (V[]) Array.newInstance(
					a.getClass().getComponentType(),
					a.length + b.length);
				System.arraycopy(a, 0, bytes, 0, a.length);
				System.arraycopy(b, 0, bytes, a.length, b.length);
				return bytes;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	public class IntArrays implements ArrayCombination<int[]> {
		private final int[][] arrays;

		public IntArrays(int[]... arrays) {
			this.arrays = arrays;
		}
		
		public int[] toArray() {
			if (arrays.length == 1) {
				return arrays[0];
			}
			int total = 0;
			for (int i = 0; i < arrays.length; i++) {
				total += arrays[i].length;
			}
			int[] array = new int[total];
			int position = 0;
			for (int i = 0; i < arrays.length; i++) {
				int[] next = arrays[i];
				System.arraycopy(next, 0, array, position, next.length);
				position += next.length;
			}
			return array;
		}
	}

	public class ByteArrays implements ArrayCombination<byte[]> {
		private final byte[][] arrays;

		public ByteArrays(byte[]... arrays) {
			this.arrays = arrays;
		}

		public byte[] toArray() {
			if (arrays.length == 1) {
				return arrays[0];
			}
			int total = 0;
			for (int i = 0; i < arrays.length; i++) {
				total += arrays[i].length;
			}
			byte[] array = new byte[total];
			int position = 0;
			for (int i = 0; i < arrays.length; i++) {
				byte[] next = arrays[i];
				System.arraycopy(next, 0, array, position, next.length);
				position += next.length;
			}
			return array;
		}
	}
}
