package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

public class BeFileOutputWriter extends RecordWriter<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>>{

	static final Logger _logger = LogManager.getLogger(BeFileOutputWriter.class); 
	List<Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> batch = new ArrayList<Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>>();

	Configuration configuration = null;
	IEnrichmentModuleContext enrichmentContext = null;
	DataBucketBean dataBucket = null;
	SharedLibraryBean beSharedLibrary = null;
	EnrichmentControlMetadataBean ecMetadata = null;
	private int batchSize = 100;
	private IEnrichmentBatchModule enrichmentBatchModule = null;
	
	public BeFileOutputWriter(Configuration configuration, IEnrichmentModuleContext enrichmentContext,IEnrichmentBatchModule enrichmentBatchModule,DataBucketBean dataBucket,
			SharedLibraryBean beSharedLibrary, EnrichmentControlMetadataBean ecMetadata) {
		super();
		this.configuration = configuration;
		this.enrichmentContext =  enrichmentContext;
		this.enrichmentBatchModule = enrichmentBatchModule;
		this.dataBucket = dataBucket;
		this.beSharedLibrary = beSharedLibrary;
		this.ecMetadata = ecMetadata;
		// TODO check where final_stage is defined
		boolean final_stage = true;
		enrichmentBatchModule.onStageInitialize(enrichmentContext, dataBucket, final_stage);

	}

	@Override
	public void write(String key, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>> value) throws IOException, InterruptedException {
				batch.add(value);
		checkBatch(false);
	}

	protected void checkBatch(boolean flush){
		if((batch.size()>=batchSize) || flush){
			enrichmentBatchModule.onObjectBatch(batch);
			batch.clear();
		}		
	}
	
	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		checkBatch(true);
		enrichmentBatchModule.onStageComplete();		
	}

}
