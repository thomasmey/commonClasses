package objectstream.index;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;

public class RandomDataInputStream extends InputStream {

	private final RandomAccessFile raf;

	public RandomDataInputStream(RandomAccessFile raf) {
		this.raf = raf;
	}

	@Override
	public int read() throws IOException {
		return raf.read();
	}


	public long seekObjectNext(long pos) throws IOException {
		// seek to next object in object stream
		raf.seek(pos);

		byte b;
		while (true) {
			b = raf.readByte();
			if(b == ObjectInputStream.TC_OBJECT) {
				b = raf.readByte();
				switch (b) {
				case ObjectInputStream.TC_REFERENCE:
					int handle = raf.readInt();
					if(handle == ObjectInputStream.baseWireHandle)
						return raf.getFilePointer() - 6;
					else
						throw new IllegalArgumentException();
				case ObjectInputStream.TC_CLASSDESC:
					return raf.getFilePointer() - 2;
				}
			}
		}
	}

	public long seekObjectPrevious(long pos) throws IOException {
		// seek to next object in object stream
		raf.seek(pos);

		byte b;
		while (true) {
			b = raf.readByte();
			if(b == ObjectInputStream.TC_OBJECT) {
				b = raf.readByte();
				switch (b) {
				case ObjectInputStream.TC_REFERENCE:
					int handle = raf.readInt();
					if(handle == ObjectInputStream.baseWireHandle)
						return raf.getFilePointer() - 6;
					else
						throw new IllegalArgumentException();
				case ObjectInputStream.TC_CLASSDESC:
					return raf.getFilePointer() - 2;
				}
			}
			raf.seek(--pos);
		}
	}

	public long length() throws IOException {
		return raf.length();
	}

	public void seek(long mid) throws IOException {
		raf.seek(mid);
	}
}
