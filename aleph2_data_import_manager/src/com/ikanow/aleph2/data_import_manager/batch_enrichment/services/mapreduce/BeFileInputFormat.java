package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;

public class BeFileInputFormat extends FileInputFormat<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> {

	@Override
	public RecordReader<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> createRecordReader(InputSplit inputSplit,
			TaskAttemptContext context) throws IOException, InterruptedException {
		BeFileInputReader reader = new BeFileInputReader();
		try {
			reader.initialize(inputSplit, context);
		} 
		catch (InterruptedException e) {
			throw new IOException(e);
		}
		return reader;
	} // createRecordReader

	
	
}
