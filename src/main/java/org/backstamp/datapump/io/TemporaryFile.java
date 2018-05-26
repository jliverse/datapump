package org.backstamp.datapump.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public class TemporaryFile implements AutoCloseable {
	private final InputStream inputStream;
	private Optional<File> temporaryFile = Optional.empty();
	
	public TemporaryFile(InputStream inputStream) {
		this.inputStream = inputStream;
	}
	
	public File toFile() throws IOException {
		if (!temporaryFile.isPresent()) {
			File file = File.createTempFile(String.valueOf(inputStream.hashCode()), null);
			try (FileOutputStream out = new FileOutputStream(file)) {
				byte[] buffer = new byte[4096];
				int numberOfBytes;
				while ((numberOfBytes = inputStream.read(buffer)) >= 0) {
					out.write(buffer, 0, numberOfBytes);
				}
			}
			file.deleteOnExit();
			temporaryFile = Optional.of(file);
		}
		return temporaryFile.get();
	}

	@Override
	public void close() throws Exception {
		temporaryFile.ifPresent(f -> f.delete());
	}
}
