/*******************************************************************************
 * Copyright 2015, The IKANOW Open Source Project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;
import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.ContextUtils;

public class BatchEnrichmentJob{

	public static String BATCH_SIZE_PARAM = "batchSize";
	public static String BE_META_BEAN_PARAM = "metadataName";
	public static String BE_CONTEXT_SIGNATURE = "beContextSignature";

	private static final Logger logger = LogManager.getLogger(BatchEnrichmentJob.class);
	
	public BatchEnrichmentJob(){
		logger.debug("BatchEnrichmentJob constructor");
		
	}
	
	public static class BatchErichmentMapper extends Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>>		
	implements IBeJobConfigurable {

		protected DataBucketBean dataBucket = null;
		protected IEnrichmentBatchModule enrichmentBatchModule = null;			

		protected IEnrichmentModuleContext enrichmentContext = null;

		@SuppressWarnings("unused")
		private int batchSize = 100;
		protected BeJobBean beJob = null;;
		protected EnrichmentControlMetadataBean ecMetadata = null;
		protected SharedLibraryBean beSharedLibrary = null;
		
		public BatchErichmentMapper(){
			super();
			System.out.println("BatchErichmentMapper constructor");
		}
		
		@Override
		protected void setup(Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>>.Context context) throws IOException, InterruptedException {
			logger.debug("BatchEnrichmentJob setup");
			try{
				

			//extractBeJobParameters(this,  context.getConfiguration());
			
			
			}
			catch(Exception e){
				logger.error("Caught Exception",e);
			}

		} // setup

		

		@Override
		protected void map(String key, Tuple2<Long, IBatchRecord> value,
				Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>>.Context context) throws IOException, InterruptedException {
			logger.debug("BatchEnrichmentJob map");
			context.write(key, value);			
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
				Mapper<String, Tuple2<Long, IBatchRecord>, String, Tuple2<Long, IBatchRecord>>.Context context)
				throws IOException, InterruptedException {
		}

		@Override
		public void setBatchSize(int bs) {
			this.batchSize=bs;
			
		}

		@Override
		public void setEnrichmentBatchModule(IEnrichmentBatchModule ebm) {
			this.enrichmentBatchModule = ebm;
			
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
		SharedLibraryBean beSharedLibrary = enrichmentContext.getModuleConfig().get();
		beJobConfigurable.setBeSharedLibrary(beSharedLibrary);		
		beJobConfigurable.setEcMetadata(BeJobBean.extractEnrichmentControlMetadata(dataBucket, configuration.get(BE_META_BEAN_PARAM)).get());	
		beJobConfigurable.setBatchSize(configuration.getInt(BATCH_SIZE_PARAM,100));	
		beJobConfigurable.setEnrichmentBatchModule((IEnrichmentBatchModule)Class.forName(beSharedLibrary.batch_enrichment_entry_point()).newInstance());
	}

}
