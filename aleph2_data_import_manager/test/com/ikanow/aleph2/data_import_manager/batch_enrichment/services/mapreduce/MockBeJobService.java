package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ikanow.aleph2.data_import_manager.batch_enrichment.services.DataImportManager;

public class MockBeJobService implements IBeJobService {
	private static final Logger logger = LogManager.getLogger(DataImportManager.class);

	@Override
	public String runEnhancementJob(String bucketFullName, String bucketPathStr, String enrichmentControlName) {
		logger.debug("runEnhancementJob:"+bucketFullName+",bucketPathStr:"+bucketPathStr+",enrichmentControlName:"+enrichmentControlName);
		return bucketFullName;
	}

}
