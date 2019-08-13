package common.io.index;

import java.io.Closeable;
import java.io.IOException;

public interface IndexWriter<E> extends Closeable {

	void writeObject(E obj) throws IOException;

}
