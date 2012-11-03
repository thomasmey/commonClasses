package common.io.objectstream.index;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamConstants;
import java.io.RandomAccessFile;

public class RandomAccessFileObjectInputStream<T> extends InputStream implements RandomObjectInput, ObjectStreamConstants {

	private final RandomAccessFile randomAccess;
	private final ObjectInputStream in;
	private final RandomObjectInputSeeker seeker;

	public RandomAccessFileObjectInputStream(RandomAccessFile raf) throws IOException, ClassNotFoundException {
		this.randomAccess = raf;
		this.in = new ObjectInputStream(this);
		// consume first object to create classdef handle...
		// stream must contain only object of same class!
		seeker = new RandomObjectInputSeeker(this);
		in.readObject();
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

	public void seek(long mid) throws IOException {
		randomAccess.seek(mid);
	}

	@Override
	public int read() throws IOException {
		return randomAccess.read();
	}

	public T readObject() throws ClassNotFoundException, IOException {
		return (T) in.readObject();
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
		return randomAccess.readByte();
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
		return randomAccess.readInt();
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
		return randomAccess.getFilePointer();
	}
}
