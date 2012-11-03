package common.io.objectstream.index;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamConstants;

public class ByteArrayInputStreamObjectInputStream<T> extends InputStream implements RandomObjectInput, ObjectStreamConstants {

	private final ByteArrayInputStream randomAccess;
	private final ObjectInputStream in;
	private final long length;
	private final RandomObjectInputSeeker seeker;

	public ByteArrayInputStreamObjectInputStream(ByteArrayInputStream raf) throws IOException, ClassNotFoundException {
		this.randomAccess = raf;
		this.length = raf.available();
		seeker = new RandomObjectInputSeeker(this);
		this.in = new ObjectInputStream(this);
		// consume first object to create classdef handle...
		// stream must contain only object of same class!
		in.readObject();
	}

	public long seekObjectNext(long pos) throws IOException {
		return seeker.seekObjectNext(pos);
	}

	public long seekObjectPrevious(long pos) throws IOException {
		return seeker.seekObjectPrevious(pos);
	}

	public long getPosition() {
		return length - randomAccess.available();
	}

	public long length() throws IOException {
		return length;
	}

	public void seek(long pos) throws IOException {
		randomAccess.reset();
		randomAccess.skip(pos);
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
		int b = randomAccess.read();
		if(b < 0)
			throw new EOFException();
		return (byte)b;
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
		byte[] i = new byte[4];
		int l = randomAccess.read(i);
		if(l < 4)
			throw new EOFException();
		return i[0] << 24 | i[1] << 16 | i[2] << 8 | i[3];
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
}
