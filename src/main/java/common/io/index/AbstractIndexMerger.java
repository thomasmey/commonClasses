/*
 * Copyright 2012 Thomas Meyer
 */

package common.io.index;

import java.io.EOFException;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

public abstract class AbstractIndexMerger<T> implements Runnable, IndexConstants {

	protected final int noMaxObjects;
	protected final boolean removeInputfiles;
	protected final File indexFile;
	protected final Comparator<T> comparator;
	protected final File[] inFiles;

	protected AbstractIndexReader<T>[] indexReader;
	protected AbstractIndexWriter<T> indexWriter;

	public AbstractIndexMerger(File baseFileName, String indexName, int noMaxObjects, Comparator<T> comparator,
			boolean removeInputfiles) throws IOException {

		if(noMaxObjects <= 0)
			throw new IllegalArgumentException("noMaxArguments is <= 0");

		if(baseFileName == null || indexName == null)
			throw new IllegalArgumentException("fileName is null");

		if(comparator == null)
			throw new IllegalArgumentException();

		this.indexFile = new File(baseFileName.toString() + '.' + indexName + '.'+ filePostfix);
		this.inFiles = getFiles(baseFileName,indexName);

		this.noMaxObjects = noMaxObjects;
		this.removeInputfiles = removeInputfiles;

		this.comparator = comparator;

		if(inFiles.length == 0)
			throw new IllegalArgumentException("No mergable index files found!");

		indexReader = new AbstractIndexReader[inFiles.length];
	}

	public void run() {

//		Comparator comparator = index
		Queue<T> q = new PriorityQueue<T>(noMaxObjects,comparator);
		T objr = null;
		T objw;
		List<T> objc = new ArrayList<T>(indexReader.length); // current object value
		for (int i=0 ; i < indexReader.length; i++)
			objc.add(objr);

		boolean[] eof = new boolean[indexReader.length];
		boolean eofAll = false;

		T objcomp = null;
		T objprev = null;
		int j,i;

		try {
			while (!eofAll) {

				objprev = null;
				// find smallest obj
				for(i = 0, j = 0; j< indexReader.length; j++) {

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
					objr = (T) indexReader[i].readObject();
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

		for(int i = 0; i < indexReader.length;i++) {
			if(indexReader[i] != null)
				try {
					indexReader[i].close();
				} catch (IOException e) {}
			if(removeInputfiles)
				inFiles[i].delete();
		}

		try {
			if(indexWriter != null)
				indexWriter.close();
		} catch (IOException e) {}
	}

	public File getIndexFile() {
		return indexFile;
	}

	private static File[] getFiles(final File baseFileName, final String indexName) {

		File dir = baseFileName.getAbsoluteFile();
		dir = dir.getParentFile();
		File[] files = dir.listFiles(new FilenameFilter() {

				@Override
				public boolean accept(File path, String name) {
					String fileName = baseFileName.getName() + '.' + indexName + '.'+ filePostfix + '.';
					return name.startsWith(fileName);
				}
			}
		);
		return files;
	}
}
