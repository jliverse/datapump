package org.backstamp.datapump.util;

import java.io.IOException;
import java.util.Arrays;

import org.backstamp.datapump.util.ArrayCombination.ByteArrays;
import org.backstamp.datapump.util.ArrayCombination.IntArrays;
import org.backstamp.datapump.util.ArrayCombination.ObjectArrays;

import junit.framework.TestCase;

public class ArrayCombinationTest extends TestCase {

	public void testCombinedByteArray() throws IOException {

		byte[] a = new byte[] {
				1, 2, 3 };
		byte[] b = new byte[] {
				4, 5, 6 };

		ByteArrays array = new ArrayCombination.ByteArrays(a, b, b);
		assertEquals(Arrays.toString(new byte[] {
				1, 2, 3, 4, 5, 6, 4, 5, 6 }), Arrays.toString(array.toArray()));
	}

	public void testCombinedIntArray() throws IOException {

		int[] a = new int[] {
				1, 2, 3 };
		int[] b = new int[] {
				4, 5, 6 };

		IntArrays array = new ArrayCombination.IntArrays(a, b);
		assertEquals(Arrays.toString(new int[] {
				1, 2, 3, 4, 5, 6 }), Arrays.toString(array.toArray()));
	}

	public void testCombinedStringArray() throws IOException {

		String[] a = new String[] {
				"1", "2", "3" };
		String[] b = new String[] {
				"4", "5", "6" };

		ObjectArrays<String> array = new ArrayCombination.ObjectArrays<>(a, b);
		assertEquals(Arrays.toString(new String[] {
				"1", "2", "3", "4", "5", "6" }), Arrays.toString(array.toArray()));
	}
}
