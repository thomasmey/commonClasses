package common.io.index;
/*
 * Copyright 2012 Thomas Meyer
 */

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public abstract class AbstractSortingIndexWriter<E> extends AbstractIndexWriter<E> {

	protected final int noMaxObjects;
	protected final List<E> list;
	protected final Comparator<E> comparator;

	protected int indexCounter;

	public AbstractSortingIndexWriter(File baseFile, String indexName, int noMaxObjects, Comparator<E> comparator) throws IOException {
		super(baseFile, indexName);
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

	protected abstract void writeElements(List<E> list) throws IOException;

	@Override
	public void flush() throws IOException {
		checkListLimit(0);
	}

	@Override
	public void close() throws IOException {
		flush();
	}
}
