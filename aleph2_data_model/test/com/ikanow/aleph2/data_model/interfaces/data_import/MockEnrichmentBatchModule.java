package com.ikanow.aleph2.data_model.interfaces.data_import;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Optional;

import scala.Tuple3;

import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;

public class MockEnrichmentBatchModule implements IEnrichmentBatchModule {

	@Override
	public void onStageInitialize(IEnrichmentModuleContext context, DataBucketBean bucket, boolean final_stage) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onObjectBatch(List<Tuple3<Long, JsonNode, Optional<ByteArrayOutputStream>>> batch) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onStageComplete() {
		// TODO Auto-generated method stub

	}

}
