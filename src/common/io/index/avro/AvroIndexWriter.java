package common.io.index.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import common.io.index.AbstractIndexWriter;

public class AvroIndexWriter<T extends SpecificRecord> extends AbstractIndexWriter<T> {

	private final DataFileWriter<T> dataFileWriter;
	private final DatumWriter<T> datumWriter;

	public AvroIndexWriter(Class<T> clazz, File baseFile, String indexName) throws IOException {
		super(baseFile, indexName);

//		Schema schema = null;
//		try {
//			schema = (Schema) clazz.getField("SCHEMA$").get(null);
//		} catch (IllegalArgumentException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IllegalAccessException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (NoSuchFieldException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (SecurityException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		this.datumWriter = new SpecificDatumWriter<T>(clazz);
		this.dataFileWriter = new DataFileWriter<T>(datumWriter);
		Schema schema = SpecificData.get().getSchema(clazz);
		dataFileWriter.create(schema, fullIndexName);
	}

	@Override
	public void close() throws IOException {
		dataFileWriter.close();
	}

	@Override
	public void writeObject(T obj) throws IOException {
		dataFileWriter.append(obj);
	}

	@Override
	public void flush() throws IOException {
		dataFileWriter.flush();
	}
}
