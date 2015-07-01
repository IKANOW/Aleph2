package com.ikanow.aleph2.data_import_manager.batch_enrichment.module;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
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
	public void onObjectBatch(List<Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> batch) {
		// TODO Auto-generated method stub
		logger.debug("MockEnrichmentBatchModule.onObjectBatch:"+ batch);

	}

	@Override
	public void onStageComplete() {
		// TODO Auto-generated method stub

	}

}
