package common.io.objectstream.index;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;

class RandomObjectInputSeeker {

	private final RandomObjectInput in;
	private final byte[] byteArrays;
	private final ByteBuffer byteBuffer;
	private long basePosition;

	public RandomObjectInputSeeker(RandomObjectInput in) {
		this.in = in;
		byteArrays = new byte[4096];
		byteBuffer = ByteBuffer.wrap(byteArrays);
	}

	public long seekObjectNext(long pos) {
		if(pos < 0)
			throw new IllegalArgumentException();

		basePosition = pos + 1;
		try {
			long len = in.length();
			if(basePosition > len) {
				throw new IllegalArgumentException();
			}

			// seek to next object in object stream
			in.seek(basePosition);

			// read some bytes
			byteBuffer.rewind();
			int l = in.read(byteArrays);
			byteBuffer.limit(l);

			while (true) {
				byte b = byteBuffer.get();
				if(b == ObjectInputStream.TC_OBJECT) {
					byteBuffer.mark();
					Long p1 = checkForObject();
					if(p1 != null) {
						in.seek(p1);
						return p1;
					}
					byteBuffer.reset();
				}
				if(byteBuffer.position() + basePosition >= len)
					return seekObjectPrevious(len);
			}
		} catch(EOFException e) {
		} catch (IOException e) {
			e.printStackTrace();
		}

		return -1;
	}

	public long seekObjectPrevious(long pos) {
		if(pos < 1)
			throw new IllegalArgumentException();

		try {
			int byteBufferOffset = this.byteArrays.length / 2;

			if(pos < byteBufferOffset) {
				byteBufferOffset = (int) (pos / 2);
			}

			basePosition = pos - byteBufferOffset - 1;

			long len = in.length();
			if(basePosition > len) {
				throw new IllegalArgumentException();
			}

			// seek to next object in object stream
			in.seek(basePosition);

			byteBuffer.rewind();
			// read some bytes
			int l = in.read(byteArrays);
			byteBuffer.limit(l);
			byteBuffer.position(byteBufferOffset);

			while (true) {
				byte b = byteBuffer.get();
				if(b == ObjectInputStream.TC_OBJECT) {
					byteBuffer.mark();
					Long p1 = checkForObject();
					if(p1 != null) {
						in.seek(p1);
						return p1;
					}
					byteBuffer.reset();
				}
				byteBuffer.position(--byteBufferOffset);
			}
		} catch(EOFException e) {
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 4;
	}

	private Long checkForObject() throws IOException {

		byte b = byteBuffer.get();
		switch (b) {
		case ObjectInputStream.TC_REFERENCE:
			int handle = byteBuffer.getInt();
			if(handle == ObjectInputStream.baseWireHandle) {
				Long pos = byteBuffer.position() + basePosition - 6;
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
