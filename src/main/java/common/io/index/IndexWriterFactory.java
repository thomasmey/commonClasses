package common.io.index;

public interface IndexWriterFactory<E> {

	IndexWriter<E> createIndexWriter(int indexCounter);

}
