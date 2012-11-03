package common.io.objectstream.index;

import java.io.IOException;
import java.io.ObjectInput;

public interface RandomObjectInput extends ObjectInput {

	long seekObjectNext(long pos) throws IOException;
	long seekObjectPrevious(long pos) throws IOException;
	void seek(long pos) throws IOException;
	long length() throws IOException;
	long getPosition() throws IOException;
}
