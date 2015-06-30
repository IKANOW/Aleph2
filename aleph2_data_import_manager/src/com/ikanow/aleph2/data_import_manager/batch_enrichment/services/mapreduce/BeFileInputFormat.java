package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;

public class BeFileInputFormat extends CombineFileInputFormat<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> {

	
	private static final Logger logger = LogManager.getLogger(BeFileInputFormat.class);

	public BeFileInputFormat(){
		super();
		logger.debug("BeFileInputFormat.constructor");
	}
	
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		logger.debug("BeFileInputFormat.isSplitable");
		return false;
	}

	@Override
	public RecordReader<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> createRecordReader(InputSplit inputSplit,	TaskAttemptContext context) throws IOException {
		logger.debug("BeFileInputFormat.createRecordReader");
		BeFileInputReader reader = new BeFileInputReader();
		try {
			reader.initialize(inputSplit, context);
		} 
		catch (InterruptedException e) {
			throw new IOException(e);
		}
		return reader;
	} // createRecordReader
	
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		logger.debug("BeFileInputFormat.getSplits");
		List<InputSplit> tmp = null;
		try {
			
			tmp = super.getSplits(context);
		} catch (Throwable t) {
			logger.error(t);
		}
		
		logger.debug("BeFileInputFormat.getSplits: " +((tmp!=null)? tmp.size():"null"));
		return tmp;
	}
}
