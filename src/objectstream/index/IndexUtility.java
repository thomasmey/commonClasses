/*
 * Copyright 2012 Thomas Meyer
 */

package objectstream.index;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.Comparator;

public class IndexUtility implements IndexConstants {

//	private static final int MAX_SORT_BUFFER = 10000;

//	public static <T extends FileIndexAccessable<T>> void sort(File baseFile, String indexName, T key, Comparator<? super T> c) {
//		int tempFileNo = 0;
//		List<T> tempList= new ArrayList<T>(MAX_SORT_BUFFER);
//
//		// step 1 sort block and write them to temporary files
//		DataInputStream raf = null;
//		try {
//			raf = new DataInputStream(new BufferedInputStream(new FileInputStream(baseFile + "." + indexName + filePostfix)));
//			while(true) {
//				T entry = null;
//				if(key.isEntrySizeConstant()) {
//					entry = key.read(raf);
//				} else {
//					byte[] m1 = new byte[marker.length];
//					raf.read(m1);
//					if(Arrays.equals(m1, marker)) {
//						entry = key.read(raf);
//						int hashCode = raf.readInt();
//						if(hashCode != entry.hashCode()) {
//							//stream is corrupted
//							throw new IllegalArgumentException("index is corrupt!");
//						}
//					}
//				}
//				if(entry != null)
//					tempList.add(entry);
//				if(tempList.size() >= MAX_SORT_BUFFER) {
//					File outFile = new File(baseFile + "." + indexName + ".temp." + tempFileNo);
//					Collections.sort(tempList, c);
//					writeList(outFile, tempList);
//					tempList.clear();
//					tempFileNo++;
//				}
//			}
//		} catch (EOFException e) {
//			File outFile = new File(baseFile + "." + indexName + ".temp." + tempFileNo);
//			Collections.sort(tempList, c);
//			writeList(outFile, tempList);
//			tempList.clear();
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} finally {
//				try {
//					if(raf != null)
//						raf.close();
//				} catch (IOException e) {}
//		}
//
//		// step 2 open all temporary files and "readsort" them
//		DataInputStream[] dis = null;
//		DataOutputStream out = null;
//		try {
//			dis = new DataInputStream[tempFileNo];
//			for(int i = 0; i < dis.length; i++) {
//				dis[i] = new DataInputStream(new BufferedInputStream(new FileInputStream(baseFile + "." + indexName + ".temp." + i)));
//			}
//			out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(baseFile + "." + indexName)));
//
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} finally {
//			for(int i = 0; i < dis.length; i++) {
//				try {
//					if(dis[i] != null)
//						dis[i].close();
//				} catch (IOException e) {}
//			}
//		}
//	}

//	private static <T extends FileIndexAccessable<T>> void writeList(File outFile, List<T> list) {
//		
//		DataOutputStream out = null;
//		try {
//			out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outFile)));
//			for(T e: list) {
//				e.write(out);
//			}
//			out.flush();
//		} catch (IOException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		} finally {
//			try {
//				if(out != null)
//					out.close();
//			} catch (IOException e) {}
//		}
//	}

	public static <T extends Serializable> long binarySearch(File baseFile, String indexName, T key, Comparator<? super T> c) {

		RandomDataInputStream raf = null;
		ObjectInputStream in = null;
		try {
			raf = new RandomDataInputStream(new RandomAccessFile(baseFile.toString() + '.' + indexName + '.' + filePostfix, "r"));
			in = new ObjectInputStream(raf);
			// consume first object to create classdef handle...
			// stream must contain only object of same class!
			in.readObject();

			long fileSize = raf.length();

			long low = 0;
			long high = fileSize;

			while(low <= high) {
				long mid = (low + high) / 2;
				mid = raf.seekObjectNext(mid);
				raf.seek(mid);
				T entry = (T) in.readObject();
				int comp = c.compare(entry, key);
				if(comp < 0) {
					low = raf.seekObjectNext(mid);
				} else if (comp > 0) {
					high = raf.seekObjectPrevious(mid);
				} else if (comp == 0){
					return mid;
				}
			}
			return -(raf.seekObjectNext(low));
		} catch (EOFException e) {

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if(in != null)
				try {
					in.close();
				} catch (IOException e) {}
		}

		return 0;
	}
}
