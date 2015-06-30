package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;

public class BeFileInputReader extends  RecordReader<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> {

	private static final Logger logger = LogManager.getLogger(BeJobLauncher.class);

	protected CombineFileSplit _fileSplit;
	protected InputStream _inStream = null;
	protected FileSystem _fs;
	protected Configuration _config;
	protected int _currFile = 0;
	protected int _numFiles = 1;
	
	protected String currrentFileName = null;

	private Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>> _record;
	protected static Map<String, IParser> parsers = new HashMap<String, IParser>();
	static{
		parsers.put("JSON", new JsonParser());
		parsers.put("BIN", new StreamParser());
	}
	
	
	public BeFileInputReader(){
		super();
		logger.debug("BeFileInputReader.constructor");
	}
	
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		
		
		_config = context.getConfiguration();
		_fileSplit = (CombineFileSplit) inputSplit;
		_numFiles = _fileSplit.getNumPaths();

		String jobName = _config.get("mapred.job.name", "unknown");
		logger.info(jobName + ": new split, contains " + _numFiles + " files, total size: " + _fileSplit.getLength());		
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (null == _inStream) {
			
			// Step 1: get input stream
			_fs = FileSystem.get(_config);
			try {
				_inStream = _fs.open(_fileSplit.getPath(_currFile));
			}
			catch (FileNotFoundException e) { // probably: this is a spare mapper, and the original mapper has deleted this file using renameAfterParse
				_currFile++;
				if (_currFile < _numFiles) {
					_inStream.close();
					_inStream = null;
					return nextKeyValue();		// (just a lazy way of getting to the next file)		
				}
				else {
					return false; // all done
				}
			}
		}	 // instream = null		
				this.currrentFileName = _fileSplit.getPath(_currFile).toString();
				IParser parser = getParser(currrentFileName);
		_record = parser.getNextRecord(_currFile,currrentFileName,_inStream);
		if (null == _record) { // Finished this file - are there any others?
			
			_currFile++;
			if (_currFile < _numFiles) {
				_inStream.close();
				_inStream = null;
				return nextKeyValue();				
			}
			else {
				return false; // all done
			}
		}//TESTED
				
		return true;
	}

	protected  IParser getParser(String fileName) {
		IParser parser = null;
		
		if(fileName!=null){
			 int dotPos =  fileName.lastIndexOf("."); 
			String ext = fileName.substring(dotPos+1).toUpperCase();  
			parser = parsers.get(ext);
			// default to binary
			if(parser == null){
				parser = parsers.get("BIN");
			}
		}
		return parser;
	}

	@Override
	public String getCurrentKey() throws IOException, InterruptedException {
		return currrentFileName;
	}

	@Override
	public Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>> getCurrentValue() throws IOException, InterruptedException {
		return _record;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float)_currFile/(float)_numFiles;
	}

	@Override
	public void close() throws IOException {
		if (null != _inStream) {
			_inStream.close();
		}
		if (null != _fs) {
			_fs.close();
		}		
	}
	
	

}
