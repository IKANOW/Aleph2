package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.ContextUtils;

import org.apache.hadoop.conf.Configuration;

public class BatchEnrichmentJob{

	public static String BATCH_SIZE_PARAM = "batchSize";
	public static String BE_META_BEAN_PARAM = "metadataName";
	public static String BE_CONTEXT_SIGNATURE = "beContextSignature";

	private static final Logger logger = LogManager.getLogger(BatchEnrichmentJob.class);
	
	public BatchEnrichmentJob(){
		logger.debug("BatchEnrichmentJob constructor");
		
	}
	
	@SuppressWarnings("unused")
	public static class BatchErichmentMapper extends Mapper<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>, String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>>		
	implements IBeJobConfigurable {

		protected DataBucketBean dataBucket = null;
		protected IEnrichmentBatchModule enrichmentBatchModule = null;			

		protected IEnrichmentModuleContext enrichmentContext = null;

		private int batchSize = 100;
		protected BeJobBean beJob = null;;
		protected EnrichmentControlMetadataBean ecMetadata = null;
		protected SharedLibraryBean beSharedLibrary = null;
		List<Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> batch = new ArrayList<Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>>();
		
		public BatchErichmentMapper(){
			super();
			System.out.println("BatchErichmentMapper constructor");
		}
		
		@Override
		protected void setup(Mapper<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>, String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>>.Context context) throws IOException, InterruptedException {
			logger.debug("BatchEnrichmentJob setup");
			try{
				

			extractBeJobParameters(this,  context.getConfiguration());
			
			this.batchSize = context.getConfiguration().getInt(BATCH_SIZE_PARAM,1);	
			this.enrichmentBatchModule = (IEnrichmentBatchModule)Class.forName(beSharedLibrary.batch_enrichment_entry_point()).newInstance();
			
			boolean final_stage = true;
			enrichmentBatchModule.onStageInitialize(enrichmentContext, dataBucket, final_stage);
			}
			catch(Exception e){
				logger.error("Caught Exception",e);
			}

		} // setup

		

		@Override
		protected void map(String key, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>> value,
				Mapper<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>, String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>>.Context context) throws IOException, InterruptedException {
			logger.debug("BatchEnrichmentJob map");
			batch.add(value);
			if(batch.size()>=batchSize){	
				enrichmentBatchModule.onObjectBatch(batch);
				batch.clear();
			}
			
		} // map

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
		protected void cleanup(
				Mapper<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>, String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>>.Context context)
				throws IOException, InterruptedException {
			    //send out the rest of the batch
				enrichmentBatchModule.onObjectBatch(batch);
				batch.clear();
		}
		
	} //BatchErichmentMapper

	public static class BatchEnrichmentReducer extends Reducer<String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>, String, Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> {

		
	} // reducer

	public static void extractBeJobParameters(IBeJobConfigurable beJobConfigurable, Configuration configuration) throws Exception{
		
		String contextSignature = configuration.get(BE_CONTEXT_SIGNATURE);  
		IEnrichmentModuleContext enrichmentContext = ContextUtils.getEnrichmentContext(contextSignature);
		beJobConfigurable.setEnrichmentContext(enrichmentContext);
		DataBucketBean dataBucket = enrichmentContext.getBucket().get();
		beJobConfigurable.setDataBucket(dataBucket);
		beJobConfigurable.setBeSharedLibrary(enrichmentContext.getLibraryConfig());		
		beJobConfigurable.setEcMetadata(BeJobBean.extractEnrichmentControlMetadata(dataBucket, configuration.get(BE_META_BEAN_PARAM)).get());		
	}

}
