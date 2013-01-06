package common.io.index.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import common.io.index.AbstractIndexReader;

public class AvroIndexReader<T extends SpecificRecord> extends AbstractIndexReader<T> {

	private final DataFileReader<T> dataFileReader;
	private final DatumReader<T> datumReader;

	public AvroIndexReader(Class<T> clazz, File baseFile, String indexName) throws IOException {
		super(baseFile, indexName);

		this.datumReader = new SpecificDatumReader<T>(clazz);
		this.dataFileReader = new DataFileReader<T>(fullIndexName, datumReader);
	}

	public AvroIndexReader(Class<T> clazz, File fullIndexName) throws IOException {
		// a bit cheating here
		super(fullIndexName, clazz.getSimpleName());
		this.datumReader = new SpecificDatumReader<T>(clazz);
		this.dataFileReader = new DataFileReader<T>(fullIndexName, datumReader);
	}

	@Override
	public T readObject() throws IOException {
		if(dataFileReader.hasNext()) {
			return dataFileReader.next();
		} else
			return null;
	}

	@Override
	public void close() throws IOException {
		dataFileReader.close();
	}
}
