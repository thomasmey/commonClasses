/*
 * Copyright 2012 Thomas Meyer
 */

package objectstream.index;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class IndexWriter<E> implements IndexConstants {

	private final File inputFile;
	private final Comparator<E> comparator;
	private final String indexName;
	private final List<E> list = new ArrayList<E>();
	private int indexCounter;
	
	private static final int MAX_ELEMENTS = 50000;

	public IndexWriter(File inputFile, String indexName, Comparator<E> comparator) {
		this.inputFile = inputFile;
		this.indexName = indexName;
		this.comparator = comparator;
	}

	public boolean write(E e) {

		boolean rc = false;
		rc = list.add(e);

		writeElements(MAX_ELEMENTS);

		return rc;
	}

	private void writeElements(int maxSize) {
		// pre sort elements
		if(list.size() > maxSize) {
			Collections.sort(list, comparator);
			ObjectOutputStream out = null;
			try {
				out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(inputFile.toString() + '.' + indexName + '.' + filePostfix + '.' + indexCounter++)));
				for(E el: list)
					out.writeObject(el);
			} catch (IOException ex) {
				System.out.println(ex);
			} finally {
				if(out != null)
					try {
						out.close();
					} catch (IOException e1) {}
			}
			list.clear();
		}
	}

	public void close() {
		writeElements(indexCounter);
	}
}
