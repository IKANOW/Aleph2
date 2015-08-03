package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

public class BeFileOutputWriter extends RecordWriter<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>>{

	Configuration configuration = null;
	IEnrichmentModuleContext enrichmentContext = null;
	DataBucketBean dataBucket = null;
	SharedLibraryBean beSharedLibrary = null;
	EnrichmentControlMetadataBean ecMetadata = null;
	
	public BeFileOutputWriter(Configuration configuration, IEnrichmentModuleContext enrichmentContext, DataBucketBean dataBucket,
			SharedLibraryBean beSharedLibrary, EnrichmentControlMetadataBean ecMetadata) {
		super();
		this.configuration = configuration;
		this.enrichmentContext =  enrichmentContext;
		this.dataBucket = dataBucket;
		this.beSharedLibrary = beSharedLibrary;
		this.ecMetadata = ecMetadata;
	}

	@Override
	public void write(String key, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>> value) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}
	
	

}
