package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

public class BatchEnrichmentJob{


	public class BatchErichmentMapper extends Mapper<Object, Object, Object, Object>{

		DataBucketBean bucket = null;
		IEnrichmentBatchModule module = null;			
		IEnrichmentModuleContext enrichmentContext = null;
		
		@Override
		protected void setup(Mapper<Object, Object, Object, Object>.Context context) throws IOException, InterruptedException {
			
		} // setup

		@Override
		protected void map(Object key, Object value, Mapper<Object, Object, Object, Object>.Context context) throws IOException,
				InterruptedException {			
			
			boolean final_stage = false;
			module.onStageInitialize(enrichmentContext, bucket, final_stage);
			List<Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> batch = new ArrayList<Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>>();
			module.onObjectBatch(batch);
		} // map
			
		
	} //BatchErichmentMapper

	public class BatchEnrichmentReducer extends Reducer<Object, Object, Object, Object> {

	}

}
