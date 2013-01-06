package common.io.index.objectstream;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;

class RandomObjectInputSeeker {

	private final RandomObjectInput in;

	public RandomObjectInputSeeker(RandomObjectInput in) {
		this.in = in;
	}

	public long seekObjectNext(long pos) {

		long basePosition = pos + 1;
		try {
			long len = in.length();
			// seek to next object in object stream
			in.seek(basePosition);
			while (true) {
				byte b = in.readByte();
				basePosition = in.getPosition();
				if(b == ObjectInputStream.TC_OBJECT) {
					Long p1 = checkForObject();
					if(p1 != null) {
						in.seek(p1);
						return p1;
					}
					in.seek(basePosition);
				}
				if(in.getPosition() >= len)
					return seekObjectPrevious(len);
			}
		} catch(EOFException e) {
		} catch (IOException e) {
			e.printStackTrace();
		}

		return -1;
	}

	public long seekObjectPrevious(long pos) {

		try {

			long basePosition = pos - 1;

			// seek to next object in object stream
			in.seek(basePosition);

			while (true) {
				byte b = in.readByte();
				basePosition = in.getPosition();
				if(b == ObjectInputStream.TC_OBJECT) {
					Long p1 = checkForObject();
					if(p1 != null) {
						in.seek(p1);
						return p1;
					}
				}

				if(basePosition -2 <= 0)
					break;

				in.seek(basePosition - 2);
			}
		} catch(EOFException e) {
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 4;
	}

	private Long checkForObject() throws IOException {

		byte b = in.readByte();
		switch (b) {
		case ObjectInputStream.TC_REFERENCE:
			int handle = in.readInt();
			if(handle == ObjectInputStream.baseWireHandle) {
				Long pos = in.getPosition() - 6;
				return pos;
			}
		case ObjectInputStream.TC_CLASSDESC:
			long p1 = in.getPosition();
			if(p1 == 6) {
				return (long)4;
			}
		}

		return null;
	}

}
