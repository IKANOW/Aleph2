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
package com.ikanow.aleph2.data_import_manager.batch_enrichment.module;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.data_analytics.IBatchRecord;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

public class MockEnrichmentBatchModule implements IEnrichmentBatchModule {
	
	private static final Logger logger = LogManager.getLogger(MockEnrichmentBatchModule.class);

	@Override
	public void onStageInitialize(IEnrichmentModuleContext context, DataBucketBean bucket, boolean final_stage) {
		// TODO Auto-generated method stub
		logger.debug("MockEnrichmentBatchModule.onStageInitialize:"+ context+", DataBucketBean:"+ bucket+", final_stage"+final_stage);

	}

	@Override
	public void onObjectBatch(List<Tuple2<Long, IBatchRecord>> batch) {
		// TODO Auto-generated method stub
		logger.debug("MockEnrichmentBatchModule.onObjectBatch:"+ batch);

	}

	@Override
	public void onStageComplete() {
		// TODO Auto-generated method stub

	}

}
