package common.io.objectstream.index;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;

class RandomObjectInputSeeker {

	private final RandomObjectInput in;

	public RandomObjectInputSeeker(RandomObjectInput in) {
		this.in = in;
	}

	public long seekObjectNext(long pos) {
		if(pos < 0)
			throw new IllegalArgumentException();
		long len = 0;

		try {
			len = in.length();

			// seek to next object in object stream
			in.seek(++pos);

			byte b;
			while (true) {
				b = in.readByte();
				if(b == ObjectInputStream.TC_OBJECT) {
					b = in.readByte();
					Long p1 = checkForObject(b);
					if(p1 != null) {
						in.seek(p1);
						return p1;
					}
					in.seek(++pos);
				}
				if(++pos >= len)
					break;
			}
		} catch(EOFException e) {
		} catch (IOException e) {
			e.printStackTrace();
		}
		return seekObjectPrevious(len);
	}

	public long seekObjectPrevious(long pos) {
		if(pos < 1)
			throw new IllegalArgumentException();

		try {
			// seek to next object in object stream
			in.seek(--pos);

			byte b;
			while (true) {
				b = in.readByte();
				if(b == ObjectInputStream.TC_OBJECT) {
					b = in.readByte();
					Long p1 = checkForObject(b);
					if(p1 != null) {
						in.seek(p1);
						return p1;
					}
				}
				if(pos > 0)
					in.seek(--pos);
				else
					break;
			}
		} catch(EOFException e) {
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 4;
	}

	private Long checkForObject(byte b) throws IOException {

		switch (b) {
		case ObjectInputStream.TC_REFERENCE:
			int handle = in.readInt();
			if(handle == ObjectInputStream.baseWireHandle)
				return in.getPosition() - 6;
		case ObjectInputStream.TC_CLASSDESC:
			long p1 = in.getPosition();
			if(p1 == 6)
				return (long)4;
		}

		return null;
	}

}
