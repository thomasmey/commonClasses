/*
 * Copyright 2012 Thomas Meyer
 */

package objectstream.index;

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

public class IndexMerger<T> implements Runnable {

	private final int noMaxObjects;
	private final Comparator<? super T> comparator;

	private ObjectInputStream[] ois;
	private File[] inFile;
	private ObjectOutputStream oos;

	public IndexMerger(File dir, final String sortFileName, int noMaxObjects, Comparator<? super T> comparator) throws FileNotFoundException, IOException {

		if(noMaxObjects <=0)
			throw new IllegalArgumentException("noMaxArguments is <=0");

		if(sortFileName == null)
			throw new IllegalArgumentException("fileName is null");

		if(sortFileName != null && sortFileName.isEmpty())
			throw new IllegalArgumentException("fileName is empty!");

		File[] files = dir.listFiles(new FilenameFilter() {

				@Override
				public boolean accept(File path, String name) {
					return name.startsWith(sortFileName + ".");
				}
			}
		);

		int noChunks = files.length;
		ois = new ObjectInputStream[noChunks];
		inFile = new File[noChunks];
		for(int i=0; i < noChunks;i++) {
			inFile[i] = files[i];
			ois[i] = new ObjectInputStream(new FileInputStream(inFile[i]));
		}

		oos = new ObjectOutputStream(new FileOutputStream(sortFileName));
		this.noMaxObjects = noMaxObjects;
		this.comparator = comparator;
	}

	public void run() {
		assert(oos!=null);
		assert(ois!=null);
		for(int i=0; i < ois.length;i++)
			assert(ois[i]!=null);
		assert(noMaxObjects > 0);

		if(ois.length==0)
			return;

		Queue<T> q = new PriorityQueue<T>();
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
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
					try {
						oos.writeObject(objw);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					objw = q.peek();
				}
			}
		}
	}

	public void close(boolean removeInputfiles) throws IOException {

		for(int i=0; i < ois.length;i++) {
			ois[i].close();
			if(removeInputfiles)
				inFile[i].delete();
		}
		oos.close();
	}
}
