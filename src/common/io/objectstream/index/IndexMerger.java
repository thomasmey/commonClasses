/*
 * Copyright 2012 Thomas Meyer
 */

package common.io.objectstream.index;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.Callable;

public class IndexMerger<T> implements Callable<Void>, IndexConstants {

	private final int noMaxObjects;
	private final Comparator<? super T> comparator;
	private final boolean removeInputfiles;
	private final File indexFile;

	private ObjectInputStream[] ois;
	private File[] inFile;
	private final ObjectOutputStream oos;

	public IndexMerger(final File baseFileName, final String indexName,
			int noMaxObjects, Comparator<? super T> comparator,
			boolean removeInputfiles) throws FileNotFoundException, IOException {

		if(noMaxObjects <= 0)
			throw new IllegalArgumentException("noMaxArguments is <= 0");

		if(baseFileName == null)
			throw new IllegalArgumentException("fileName is null");

//		if(baseFileName)
//			throw new IllegalArgumentException("fileName is empty!");

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

		int noChunks = files.length;
		ois = new ObjectInputStream[noChunks];
		inFile = new File[noChunks];
		for(int i=0; i < noChunks;i++) {
			inFile[i] = files[i];
			ois[i] = new ObjectInputStream(new BufferedInputStream(new FileInputStream(inFile[i])));
		}
		indexFile = new File(baseFileName.toString() + '.' + indexName + '.'+ filePostfix);
		oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(getIndexFile())));
		this.noMaxObjects = noMaxObjects;
		this.comparator = comparator;
		this.removeInputfiles = removeInputfiles;
	}

	public Void call() throws ClassNotFoundException, IOException {
		assert oos != null;
		assert ois != null;
		for(int i=0; i < ois.length;i++)
			assert ois[i] != null;
		assert noMaxObjects > 0;

		if(ois.length == 0)
			return null;

		Queue<T> q = new PriorityQueue<T>(noMaxObjects,comparator);
		T objr = null;
		T objw;
		List<T> objc = new ArrayList<T>(ois.length); // current object value
		for (int i=0 ; i < ois.length; i++)
			objc.add(objr);

		boolean[] eof = new boolean[ois.length];
		boolean eofAll = false;

		T objcomp = null;
		T objprev = null;
		int j,i;

		try {
			while (!eofAll) {

				objprev=null;
				// find smallest obj
				for(i=0, j=0; j< ois.length; j++) {

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
					objr = (T) ois[i].readObject();
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
						oos.writeObject(objw);
						objw = q.peek();
					}
				}
			}
		} finally {
			close();
		}
		return null;
	}

	private void close() {
		for(int i=0; i < ois.length;i++) {
			if(ois[i] != null)
				try {
					ois[i].close();
				} catch (IOException e) {}
			if(removeInputfiles)
				inFile[i].delete();
		}
		try {
			if(oos != null)
				oos.close();
		} catch (IOException e) {}
	}

	public File getIndexFile() {
		return indexFile;
	}
}
