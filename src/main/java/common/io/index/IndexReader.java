package common.io.index;

import java.io.Closeable;
import java.io.IOException;

public interface IndexReader<E> extends Closeable {
	E readObject() throws IOException;
}
