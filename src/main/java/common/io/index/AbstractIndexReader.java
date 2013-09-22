/*
 * Copyright 2012 Thomas Meyer
 */

package common.io.index;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public abstract class AbstractIndexReader<T> implements IndexConstants, Closeable {

	protected final String indexName;
	protected final File baseFile;
	protected final File fullIndexName;

	public AbstractIndexReader(File baseFile, String indexName) {
		this.indexName = indexName;
		this.baseFile = baseFile;
		this.fullIndexName = new File(baseFile.toString() + '.' + indexName + '.' + filePostfix);
	}

	public abstract T readObject() throws IOException;

	public abstract void close() throws IOException;

	public abstract long getLength() throws IOException;

	public abstract void sync(long mid) throws IOException;

	public abstract long getPosition() throws IOException;

	public abstract void seek(long l) throws IOException;
}
