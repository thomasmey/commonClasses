package common.io.index.avro;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import common.io.index.AbstractIndexMerger;

public class AvroIndexMerger<T extends SpecificRecord> extends AbstractIndexMerger<T> {

	public AvroIndexMerger(Class<T> clazz, File baseFileName, String indexName,
			int noMaxObjects, Comparator<T> comparator, boolean removeInputfiles)
			throws IOException {

		super(baseFileName, indexName, noMaxObjects, comparator, removeInputfiles);

		this.indexWriter = new AvroIndexWriter<T>(clazz, baseFileName, indexName);

		for(int i = 0; i < indexReader.length; i++) {
			indexReader[i] = new AvroIndexReader<T>(clazz, inFiles[i]);
		}
	}
}
