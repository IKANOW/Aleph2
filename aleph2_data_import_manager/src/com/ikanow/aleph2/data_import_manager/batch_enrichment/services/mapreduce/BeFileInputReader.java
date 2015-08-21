package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_import.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.ContextUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.TimeUtils;

public class BeFileInputReader extends  RecordReader<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> implements IBeJobConfigurable{

	private static final Logger logger = LogManager.getLogger(BeJobLauncher.class);
	public static String DEFAULT_GROUPING = "daily";

	protected CombineFileSplit _fileSplit;
	protected InputStream _inStream = null;
	protected FileSystem _fs;
	protected Configuration _config;
	protected int _currFile = 0;
	protected int _numFiles = 1;
	
	protected String currrentFileName = null;

	private Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>> _record;

	protected IEnrichmentModuleContext enrichmentContext;

	protected DataBucketBean dataBucket;

	protected SharedLibraryBean beSharedLibrary;

	protected EnrichmentControlMetadataBean ecMetadata;
	protected static Map<String, IParser> parsers = new HashMap<String, IParser>();
	static{
		parsers.put("JSON", new JsonParser());
		parsers.put("BIN", new StreamParser());
	}
	
	Date start = null;
	private int batchSize;
	private IEnrichmentBatchModule enrichmentBatchModule;
	
	public BeFileInputReader(){
		super();
		logger.debug("BeFileInputReader.constructor");
	}
	
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException{
		
		
		_config = context.getConfiguration();
		_fileSplit = (CombineFileSplit) inputSplit;
		_numFiles = _fileSplit.getNumPaths();
		this.start =  new Date();
		String contextSignature = context.getConfiguration().get(BatchEnrichmentJob.BE_CONTEXT_SIGNATURE);   
		try {
			this.enrichmentContext = ContextUtils.getEnrichmentContext(contextSignature);
			this.dataBucket = enrichmentContext.getBucket().get();
			this.beSharedLibrary = enrichmentContext.getLibraryConfig();		
			this.ecMetadata = BeJobBean.extractEnrichmentControlMetadata(dataBucket, context.getConfiguration().get(BatchEnrichmentJob.BE_META_BEAN_PARAM)).get();
		} catch (Exception e) {
			logger.error(ErrorUtils.getLongForm("{0}", e),e);
		}

		String jobName = _config.get("mapred.job.name", "unknown");
		logger.info(jobName + ": new split, contains " + _numFiles + " files, total size: " + _fileSplit.getLength());		
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (_currFile >= _numFiles) {
			return false;
		}
		
		if (null == _inStream){
			
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
			archiveOrDeleteFile();
			_currFile++;
			if (_currFile < _numFiles) {
				_inStream.close();
				_inStream = null;
				return nextKeyValue();				
			}
			else {
				return false; // all done
			}
		} // record = null
		// close stream if not multiple records per file supported
		if(!parser.multipleRecordsPerFile()){
			archiveOrDeleteFile();
			_currFile++;
			_inStream.close();
			_inStream = null;
		}
		return true;
	}

	private void archiveOrDeleteFile() {
		try {
			if (dataBucket.data_schema()!=null && dataBucket.data_schema().storage_schema()!=null && dataBucket.data_schema().storage_schema().enabled()) {
				Path currentPath = _fileSplit.getPath(_currFile);
				_fs.rename(currentPath, createArchivePath(currentPath));
			} else {
				_fs.delete(_fileSplit.getPath(_currFile), false);
			}
		} catch (Exception e) {
			logger.error(ErrorUtils.getLongForm(ErrorUtils.EXCEPTION_CAUGHT, e));
			// We're just going to move on if we can't delete the file, it's
			// probably a permissions error
		}
	}

	private Path createArchivePath(Path currentPath) throws Exception {
		

		ChronoUnit timeGroupingUnit = ChronoUnit.DAYS;
		try {
			timeGroupingUnit = TimeUtils.getTimePeriod(Optionals.of(() -> dataBucket.data_schema().storage_schema().processed_grouping_time_period()).orElse(DEFAULT_GROUPING)).success();			
		} catch (Throwable t) {			
			logger.error(ErrorUtils.getLongForm(ErrorUtils.VALIDATION_ERROR,t),t);
		}
		String timeGroupingFormat = TimeUtils.getTimeBasedSuffix(timeGroupingUnit,Optional.of(ChronoUnit.MINUTES));
		SimpleDateFormat sdf = new SimpleDateFormat(timeGroupingFormat);
		String timeGroup = sdf.format(start);
		Path storedPath = Path.mergePaths(currentPath.getParent().getParent(),new Path("/stored/processed/"+timeGroup));

		return storedPath;
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

	@Override
	public void setEcMetadata(EnrichmentControlMetadataBean ecMetadata) {
		this.ecMetadata = ecMetadata;
	}

	@Override
	public void setBeSharedLibrary(SharedLibraryBean beSharedLibrary) {
		this.beSharedLibrary = beSharedLibrary;
	}

	@Override
	public void setDataBucket(DataBucketBean dataBucketBean) {
		this.dataBucket = dataBucketBean;
		
	}
		
	
	@Override
	public void setEnrichmentContext(IEnrichmentModuleContext enrichmentContext) {
		this.enrichmentContext = enrichmentContext;
	}

	@Override
	public void setBatchSize(int size) {
		this.batchSize = size;
		
	}

	@Override
	public void setEnrichmentBatchModule(IEnrichmentBatchModule beModule) {
		this.enrichmentBatchModule = beModule;
		
	}
	
	

}
