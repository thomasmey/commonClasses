/*
 * Copyright 2012 Thomas Meyer
 */

package common.io.index;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public abstract class AbstractIndexReader<T> implements IndexConstants, Closeable {

	protected final String indexName;
	protected final File baseFile;
	protected final File fullIndexName;

	public AbstractIndexReader(File baseFile, String indexName) {
		this.indexName = indexName;
		this.baseFile = baseFile;
		this.fullIndexName = new File(baseFile.toString() + '.' + indexName + '.' + filePostfix);
	}

	public abstract T readObject() throws IOException;

//	public static <T extends Serializable> long binarySearch(File baseFile, String indexName, T key, Comparator<? super T> c) {
//
//		RandomObjectInput randomObjectInput = null;
//		Exception ex = null;
//		try {
//			randomObjectInput = new RandomAccessFileObjectInputStream(new RandomAccessFile( "r"));
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
//	private static <T extends Serializable> long binarySearch(RandomObjectInput randomObjectInput, T key, Comparator<? super T> c) {
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

	public abstract void close() throws IOException;
}
