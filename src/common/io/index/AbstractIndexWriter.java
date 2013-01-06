package common.io.index;
/*
 * Copyright 2012 Thomas Meyer
 */

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;

public abstract class AbstractIndexWriter<E> implements IndexConstants, Closeable, Flushable {

	protected final File baseFile;
	protected final String indexName;
	protected final File fullIndexName;

	public AbstractIndexWriter(File baseFile, String indexName) throws IOException {
		this.baseFile = baseFile;
		this.indexName = indexName;
		this.fullIndexName = new File(baseFile.toString() + '.' + indexName + '.' + filePostfix);
	}

	public abstract void writeObject(E obj) throws IOException;

	@Override
	public abstract void flush() throws IOException;

	@Override
	public abstract void close() throws IOException;
}
