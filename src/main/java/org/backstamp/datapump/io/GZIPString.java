package org.backstamp.datapump.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.Objects;
import java.util.zip.GZIPInputStream;

public class GZIPString {
	private InputStream inputStream;

	public GZIPString(InputStream inputStream) {
		this.inputStream = inputStream;
	}

	public String toString() {
		if (Objects.isNull(inputStream)) {
			return "".intern();
		}
		try (InputStream ungzippedResponse = new GZIPInputStream(inputStream);
				InputStreamReader reader = new InputStreamReader(ungzippedResponse, "UTF-8");
				StringWriter writer = new StringWriter();) {
			char[] buffer = new char[4096];
			for (int length = 0; (length = reader.read(buffer)) > 0;) {
				writer.write(buffer, 0, length);
			}
			return writer.toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
