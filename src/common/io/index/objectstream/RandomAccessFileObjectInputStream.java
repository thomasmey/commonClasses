package common.io.index.objectstream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamConstants;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class RandomAccessFileObjectInputStream<T> extends InputStream implements RandomObjectInput, ObjectStreamConstants {

	private RandomAccessFile randomAccess;
	private ObjectInputStream in;
	private RandomObjectInputSeeker seeker;

	private static final int BUFFER_SIZE = 65536;
	// for reverse seek
	private static final int BUFFER_OFFSET = BUFFER_SIZE / 10;
	private byte[] byteArrays;
	private ByteBuffer byteBuffer;
	private long basePosition;
	private long seekPosition;

	public RandomAccessFileObjectInputStream(RandomAccessFile raf) throws IOException, ClassNotFoundException {

		this.randomAccess = raf;

		// buffering
		this.byteArrays = new byte[BUFFER_SIZE];
		this.byteBuffer = ByteBuffer.wrap(byteArrays);
		this.byteBuffer.limit(0);
		this.seekPosition = -1;

		this.in = new ObjectInputStream(this);
		// consume first object to create classdef handle...
		// stream must contain only object of same class!
//		in.readObject();
		in.readUnshared();

		// and back to first object
		seek(4);

		seeker = new RandomObjectInputSeeker(this);
	}

	public long seekObjectNext(long pos) throws IOException {
		return seeker.seekObjectNext(pos);
	}

	public long seekObjectPrevious(long pos) throws IOException {
		return seeker.seekObjectPrevious(pos);
	}

	public long length() throws IOException {
		return randomAccess.length();
	}

	public T readObject() throws ClassNotFoundException, IOException {
		// don't share objects!
		return (T) in.readUnshared();
	}

	public void seek(long pos) throws IOException {

		if(pos < 0 || pos > length())
			throw new IllegalArgumentException();

		if(pos >= basePosition && pos <= basePosition + byteBuffer.limit()) {
			byteBuffer.position((int) (pos - basePosition));
		} else {
			seekPosition = pos;
			// drain buffer
			byteBuffer.limit(0);
		}
	}

	@Override
	public int read() throws IOException {

		int size = ensureBuffer();
		if(size < 1) {
			return -1;
		}

		int b = byteBuffer.get() & 0xff;
		return b;
	}

	private int ensureBuffer() throws IOException {

		int remaining = byteBuffer.remaining();

		// any data left in buffer?
		if(remaining > 0)
			return remaining;
		else {
			int bytesRead = -1;
			if(seekPosition >= 0) {
				basePosition = seekPosition;
				seekPosition = -1;
			} else {
				basePosition = randomAccess.getFilePointer();
			}

			if(basePosition > BUFFER_OFFSET) {
				basePosition = basePosition - BUFFER_OFFSET;
				randomAccess.seek(basePosition);
				byteBuffer.rewind();
				bytesRead = randomAccess.read(byteArrays);

				if(bytesRead >= 0) {
					byteBuffer.limit(bytesRead);
					int split = bytesRead < BUFFER_OFFSET ? bytesRead / 2 : BUFFER_OFFSET;
					byteBuffer.position(split);
					bytesRead = bytesRead - split;
				}
			} else {
				randomAccess.seek(basePosition);
				byteBuffer.rewind();
				bytesRead = randomAccess.read(byteArrays);
				if(bytesRead >= 0) {
					byteBuffer.limit(bytesRead);
				}
			}

			return bytesRead;
		}
	}

	@Override
	public void readFully(byte[] b) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int skipBytes(int n) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean readBoolean() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte readByte() throws IOException {
		int size = ensureBuffer();
		if(size < 0) {
			throw new EOFException();
		}

		return byteBuffer.get();
	}

	@Override
	public int readUnsignedByte() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public short readShort() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int readUnsignedShort() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public char readChar() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int readInt() throws IOException {
		int size = ensureBuffer();
		if(size < 0) {
			throw new EOFException();
		}

		return byteBuffer.getInt();
	}

	@Override
	public long readLong() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public float readFloat() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public double readDouble() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String readLine() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String readUTF() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getPosition() throws IOException {
//		return randomAccess.getFilePointer();
		return basePosition + byteBuffer.position();
	}

	@Override
	public void close() throws IOException {

		//FIXME: ObjectInputStream, also closes underlying stream, is this object!
//		in.close();
		randomAccess.close();
		randomAccess = null;
		in = null;
		seeker = null;
		byteArrays = null;
		byteBuffer = null;
	}

}
