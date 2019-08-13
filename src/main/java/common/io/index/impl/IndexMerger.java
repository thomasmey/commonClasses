/*
 * Copyright 2012 Thomas Meyer
 */

package common.io.index.impl;

import java.io.EOFException;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import common.io.index.IndexConstants;
import common.io.index.IndexReader;
import common.io.index.IndexWriter;

public class IndexMerger<T> implements Runnable, IndexConstants {

	protected final int noMaxObjects;
	protected final Comparator<T> comparator;

	protected IndexReader<T>[] indexReaders;
	protected IndexWriter<T> indexWriter;

	public IndexMerger(int noMaxObjects, Comparator<T> comparator, IndexReader<T>[] indexReaders, IndexWriter<T> indexWriter) {

		if(noMaxObjects <= 0)
			throw new IllegalArgumentException("noMaxArguments is <= 0");

		if(comparator == null)
			throw new IllegalArgumentException();

		this.noMaxObjects = noMaxObjects;
		this.comparator = comparator;

		this.indexReaders = indexReaders;
		this.indexWriter = indexWriter;
	}

	public void run() {

		Queue<T> q = new PriorityQueue<T>(noMaxObjects,comparator);
		T objr = null;
		T objw;
		List<T> objc = new ArrayList<T>(indexReaders.length); // current object value
		for (int i=0 ; i < indexReaders.length; i++)
			objc.add(objr);

		boolean[] eof = new boolean[indexReaders.length];
		boolean eofAll = false;

		T objcomp = null;
		T objprev = null;
		int j,i;

		try {
			while (!eofAll) {

				objprev = null;
				// find smallest obj
				for(i = 0, j = 0; j< indexReaders.length; j++) {

					if(!eof[j]) {
						objcomp = objc.get(j);
						if(objcomp == null) {
							i = j;
							break;
						} else {
							if(objprev == null) {
								objprev = objcomp;
								i = j;
							}

							if(comparator.compare(objcomp, objprev) < 0) {
								objprev = objcomp;
								i = j;
							}
						}
					}
				}

				// read object
				if(eof[i])
					continue;

				try {
					objr = (T) indexReaders[i].readObject();
				} catch(EOFException e) {
					// end of input
					objr = null;
				}

				if (objr != null) {
					objc.set(i, objr);
					q.offer(objr);
				} else {
					eof[i] = true;
					objc.set(i, null);
					eofAll = eof[i];
					for(boolean b: eof)
						eofAll &= b;
				}

				if (q.size() > noMaxObjects || eofAll) {
					objw = q.peek();
					for(; objw != null; ) {
						if(comparator.compare(objprev, objw) <= 0)
							break;
						q.remove();
						indexWriter.writeObject(objw);
						objw = q.peek();
					}
				}
			}
		} catch(IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				close();
			} catch (IOException e) {
			}
		}

		return;
	}

	protected void close() throws IOException {

		for(int i = 0; i < indexReaders.length;i++) {
			if(indexReaders[i] != null)
				try {
					indexReaders[i].close();
				} catch (IOException e) {}
		}

		try {
			if(indexWriter != null)
				indexWriter.close();
		} catch (IOException e) {}
	}
}
