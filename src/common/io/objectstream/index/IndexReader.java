/*
 * Copyright 2012 Thomas Meyer
 */

package common.io.objectstream.index;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.Comparator;

public class IndexReader<T extends Serializable> implements IndexConstants {

	private final RandomObjectInput randomObjectInput;
	private final Comparator<? super T> comparator;

	public IndexReader(File baseFile, String indexName, Comparator<? super T> comparator) throws IOException, ClassNotFoundException {
		super();
		RandomAccessFile raf = new RandomAccessFile(baseFile.toString() + '.' + indexName + '.' + filePostfix, "r");
		long size = raf.length();
		if(size < 4 * 1024 * 1024) {
			byte[] ba = new byte[(int) size];
			raf.read(ba);
			this.randomObjectInput = new ByteArrayInputStreamObjectInputStream<T>(new ByteArrayInputStream(ba));
			raf.close();
		} else
			this.randomObjectInput = new RandomAccessFileObjectInputStream<T>(raf);
		this.comparator = comparator;
	}

	public T getNextObject() throws ClassNotFoundException, IOException {
		return (T) randomObjectInput.readObject();
	}

	public T getPreviousObjectAt(long pos) throws ClassNotFoundException, IOException {
		randomObjectInput.seekObjectPrevious(pos);
		return (T) randomObjectInput.readObject();
	}

	public T getNextObjectAt(long pos) throws ClassNotFoundException, IOException {
		randomObjectInput.seekObjectNext(pos);
		return (T) randomObjectInput.readObject();
	}

	public T getObjectAt(long pos) throws ClassNotFoundException, IOException {
		randomObjectInput.seek(pos);
		return (T) randomObjectInput.readObject();
	}

	public long binarySearch(T key) {
		long startTime = System.nanoTime();
		long ret = binarySearch(randomObjectInput, key, comparator);
		long diff = System.nanoTime() - startTime;
		System.out.println("search took " + diff/1000 + "µs");
		return ret;
	}

	public static <T extends Serializable> long binarySearch(File baseFile, String indexName, T key, Comparator<? super T> c) {

		RandomObjectInput randomObjectInput = null;
		Exception ex = null;
		try {
			randomObjectInput = new RandomAccessFileObjectInputStream(new RandomAccessFile(baseFile.toString() + '.' + indexName + '.' + filePostfix, "r"));
			return binarySearch(randomObjectInput, key, c);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			ex = e;
		} catch (IOException e) {
			e.printStackTrace();
			ex = e;
		} finally {
			if(randomObjectInput != null)
				try {
					randomObjectInput.close();
				} catch (IOException e) {}
		}
		throw new RuntimeException(ex);
	}

	private static <T extends Serializable> long binarySearch(RandomObjectInput randomObjectInput, T key, Comparator<? super T> c) {

		Exception ex = null;
		try {
			long fileSize = randomObjectInput.length();

			long low = 0;
			long high = fileSize;

			while(low <= high) {
				long mid = (low + high) / 2;
				long midSeek = randomObjectInput.seekObjectNext(mid);
				if(midSeek > high) {
					// object search went to far for the current borders
					midSeek = high;
					randomObjectInput.seek(midSeek);
				} else if (midSeek == high && midSeek == low) {
					// no exact match for this key
					break;
				} else if(midSeek < 0) {
					// EOF detected
					midSeek = randomObjectInput.seekObjectPrevious(mid);
				}
				T entry = (T) randomObjectInput.readObject();
				int comp = c.compare(entry, key);
				if(comp < 0) {
					low = randomObjectInput.seekObjectNext(midSeek);
				} else if (comp > 0) {
					high = randomObjectInput.seekObjectPrevious(midSeek);
				} else if (comp == 0){
					return mid;
				}
			}
			return -(randomObjectInput.seekObjectNext(low));
		} catch (EOFException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
			ex = e;
		}
		throw new RuntimeException(ex);
	}
}
