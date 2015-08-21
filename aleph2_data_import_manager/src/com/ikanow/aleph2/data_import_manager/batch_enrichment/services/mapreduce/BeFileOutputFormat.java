package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

public class BeFileOutputFormat extends FileOutputFormat<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> implements IBeJobConfigurable{

	private EnrichmentControlMetadataBean ecMetadata;
	private SharedLibraryBean beSharedLibrary;
	private DataBucketBean dataBucket;
	private IEnrichmentModuleContext enrichmentContext;
	private IEnrichmentBatchModule enrichmentBatchModule = null;			


	@Override
	public RecordWriter<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> getRecordWriter(TaskAttemptContext jobContext)
			throws IOException, InterruptedException {
		try {
			BatchEnrichmentJob.extractBeJobParameters(this, jobContext.getConfiguration());
		} catch (Exception e) {
			throw new IOException(e);
		}
		return new BeFileOutputWriter(jobContext.getConfiguration(), enrichmentContext,enrichmentBatchModule,dataBucket,beSharedLibrary,ecMetadata);
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
	public void setBatchSize(int int1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setEnrichmentBatchModule(IEnrichmentBatchModule enrichmentBatchModule) {
		this.enrichmentBatchModule = enrichmentBatchModule;
		
	}

}
