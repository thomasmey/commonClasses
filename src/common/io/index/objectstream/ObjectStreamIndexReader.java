/*
 * Copyright 2012 Thomas Meyer
 */

package common.io.index.objectstream;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;

import common.io.index.AbstractIndexReader;

public class ObjectStreamIndexReader<T extends Serializable> extends AbstractIndexReader<T> {

	private static final long IN_MEMORY_INDEX_SIZE = 4 * 1024 * 1024;

	private RandomObjectInput randomObjectInput;

	public ObjectStreamIndexReader(File baseFile, String indexName) throws IOException, ClassNotFoundException {

		super(baseFile, indexName);

		RandomAccessFile raf = new RandomAccessFile(fullIndexName, "r");

		//optimization for short index files
		long size = raf.length();
		if(size < IN_MEMORY_INDEX_SIZE) {
			byte[] ba = new byte[(int) size];
			raf.read(ba);
			this.randomObjectInput = new ByteArrayInputStreamObjectInputStream<T>(new ByteArrayInputStream(ba));
			raf.close();
		} else
			this.randomObjectInput = new RandomAccessFileObjectInputStream<T>(raf);
	}

	public T readObject() throws IOException {
		try {
			return (T) randomObjectInput.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	public T getPreviousObjectAt(long pos) throws IOException {
		randomObjectInput.seekObjectPrevious(pos);
		try {
			return (T) randomObjectInput.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);		}
	}

	public T getNextObjectAt(long pos) throws IOException {
		randomObjectInput.seekObjectNext(pos);
		try {
			return (T) randomObjectInput.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	public T getObjectAt(long pos) throws IOException {
		randomObjectInput.seek(pos);
		try {
			return (T) randomObjectInput.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

//	public long binarySearch(T key) {
//		long ret = binarySearch(randomObjectInput, key, comparator);
//		return ret;
//	}
//
//	public static <T> long binarySearch(File baseFile, String indexName, T key, Comparator<? super T> c) {
//
//		RandomObjectInput randomObjectInput = null;
//		Exception ex = null;
//		try {
//			randomObjectInput = new RandomAccessFileObjectInputStream(new RandomAccessFile(baseFile.toString() + '.' + indexName + '.' + filePostfix, "r"));
//			return binarySearch(randomObjectInput, key, c);
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//			ex = e;
//		} catch (IOException e) {
//			e.printStackTrace();
//			ex = e;
//		} finally {
//			if(randomObjectInput != null)
//				try {
//					randomObjectInput.close();
//				} catch (IOException e) {}
//		}
//		throw new RuntimeException(ex);
//	}
//
//	private static <T> long binarySearch(RandomObjectInput randomObjectInput, T key, Comparator<? super T> c) {
//
//		Exception ex = null;
//		try {
//			long low = 4;
//			long high = randomObjectInput.seekObjectPrevious(randomObjectInput.length());
//
//			while(low <= high) {
//				long mid = (low + high) / 2;
//				long midSeek = randomObjectInput.seekObjectNext(mid);
//				if(midSeek > high) {
//					// object search went to far for the current borders
//					midSeek = high;
//					randomObjectInput.seek(midSeek);
//				}
//				T entry = (T) randomObjectInput.readObject();
//				int comp = c.compare(entry, key);
//				if(comp < 0) {
//					low = randomObjectInput.seekObjectNext(midSeek);
//				} else if (comp > 0) {
//					high = randomObjectInput.seekObjectPrevious(midSeek);
//				} else if (comp == 0){
//					return midSeek;
//				}
//
//				// break loop when algorytm hangs
//				if (midSeek == high && midSeek == low) {
//					return -(midSeek);
//				}
//			}
//			return -(low);
//		} catch (EOFException e) {
//			e.printStackTrace();
//		} catch (Exception e) {
//			e.printStackTrace();
//			ex = e;
//		}
//		throw new RuntimeException(ex);
//	}

	@Override
	public void close() throws IOException {
		randomObjectInput.close();
		randomObjectInput = null;
	}
}
