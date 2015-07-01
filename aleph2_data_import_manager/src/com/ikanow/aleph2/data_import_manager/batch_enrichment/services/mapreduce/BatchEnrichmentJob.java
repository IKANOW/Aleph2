package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

public class BatchEnrichmentJob{

	public static String DATA_BUCKET_BEAN_PARAM = "dataBucketBean";
	public static String BATCH_SIZE_PARAM = "batchSize";
	public static String BUCKET_FULL_NAME_PARAM = "bucketFullName";
	public static String BUCKET_PATH_PARAM = "bucketFullName";
	public static String EC_META_NAME_PARAM = "ecMetaName";
	private static final Logger logger = LogManager.getLogger(BatchEnrichmentJob.class);
	
	public BatchEnrichmentJob(){
		System.out.println("BatchEnrichmentJob constructor");
		
	}
	
	public static class BatchErichmentMapper extends Mapper<LongWritable,Text, LongWritable,Text>		
	{

		DataBucketBean bucket = null;
		IEnrichmentBatchModule module = null;			
		IEnrichmentModuleContext enrichmentContext = null;
		private int batchSize = 1;
		
		public BatchErichmentMapper(){
			super();
			System.out.println("BatchErichmentMapper constructor");
		}
		
		@Override
		protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
			logger.debug("BatchEnrichmentJob setup");
			System.out.println("BatchEnrichmentJob setup");
			String dataBucketBeanJson = context.getConfiguration().get(DATA_BUCKET_BEAN_PARAM);
			this.bucket = BeanTemplateUtils.from(dataBucketBeanJson, DataBucketBean.class).get();
			this.batchSize = context.getConfiguration().getInt(BATCH_SIZE_PARAM,1);
			boolean final_stage = true;
			// TODO initialize module
			//this.module = // 
			// TODO initialize context
			//this.enrichmentContext = // 
			module.onStageInitialize(enrichmentContext, bucket, final_stage);
		} // setup

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws InterruptedException {			
			logger.debug("BatchEnrichmentJob map");
			System.out.println("BatchEnrichmentJob map");
			List<Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> batch = new ArrayList<Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>>();			
			module.onObjectBatch(batch);
			
		} // map
			
		
	} //BatchErichmentMapper

	public static class BatchEnrichmentReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

		
	} // reducer



}
