package common.io.index.impl;
/*
 * Copyright 2012 Thomas Meyer
 */

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import common.io.index.IndexWriter;
import common.io.index.IndexWriterFactory;

public class SortingIndexWriter<E> implements Closeable {

	protected final int noMaxObjects;
	protected final List<E> list;
	protected final Comparator<E> comparator;

	protected int indexCounter;
	private IndexWriterFactory<E> indexWriterFactory;

	public SortingIndexWriter(IndexWriterFactory<E> indexWriterFactory, int noMaxObjects, Comparator<E> comparator) throws IOException {
		this.indexWriterFactory = indexWriterFactory;
		if(noMaxObjects < 0)
			throw new IllegalArgumentException();

		this.noMaxObjects = noMaxObjects;
		this.comparator = comparator;

		this.list = new ArrayList<E>();
	}

	public void writeObject(E obj) throws IOException {
		list.add((E) obj);
		checkListLimit(noMaxObjects);
	}

	private void checkListLimit(int maxSize) throws IOException {
		// pre sort elements
		if(list.size() > maxSize) {
			Collections.sort(list, comparator);
			writeElements(list);
			list.clear();
			indexCounter++;
		}
	}

	private void writeElements(List<E> list) throws IOException {
		try(IndexWriter<E> indexWriter = indexWriterFactory.createIndexWriter(indexCounter)) {
			for(E obj: list) {
				indexWriter.writeObject(obj);
			}
		}
	}

	@Override
	public void close() throws IOException {
		checkListLimit(0);
	}
}
