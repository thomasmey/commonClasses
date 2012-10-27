/*
 * Copyright 2012 Thomas Meyer
 */

package objectstream.index;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;


public class IndexReader<E> implements IndexConstants {

	private final RandomAccessFile raf;

	public IndexReader(File xmlDumpFile, String indexName) throws FileNotFoundException {
		raf = new RandomAccessFile(xmlDumpFile + filePostfix, "r");
	}

}
