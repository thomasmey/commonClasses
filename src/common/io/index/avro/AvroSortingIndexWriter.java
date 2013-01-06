package common.io.index.avro;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import common.io.index.AbstractSortingIndexWriter;

public class AvroSortingIndexWriter<T extends SpecificRecord> extends
		AbstractSortingIndexWriter<T> {

	private final Schema schema;
	private final DatumWriter<SpecificRecord> datumWriter;

	public AvroSortingIndexWriter(Schema schema, File baseFile, String indexName,
			int noMaxObjects, Comparator<T> comparator) throws IOException {

		super(baseFile, indexName, noMaxObjects, comparator);
		this.schema = schema;
		this.datumWriter = new SpecificDatumWriter<SpecificRecord>(schema);
	}

	@Override
	protected void writeElements(List<T> list) throws IOException {
		File file = new File(baseFile.toString() + '.' + indexName + '.' + filePostfix + '.' + indexCounter);
		DataFileWriter<SpecificRecord> dataFileWriter = new DataFileWriter<SpecificRecord>(datumWriter);
		dataFileWriter.create(schema, file);

		for(T el: list)
			dataFileWriter.append(el);
		dataFileWriter.close();
	}

}
