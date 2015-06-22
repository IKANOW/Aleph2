package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ikanow.aleph2.data_import_manager.batch_enrichment.services.DataImportManager;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

public class MockBeJobService implements IBeJobService {
	private static final Logger logger = LogManager.getLogger(DataImportManager.class);

	@Override
	public boolean runEnhancementJob(DataBucketBean bucket, EnrichmentControlMetadataBean ec, List<SharedLibraryBean> sharedLibraries,
			Path bucketInput, Path bucketOutput) {
		logger.debug("runEnhancementJob:"+bucket.full_name()+",ec:"+ec.name()+",sharedLibraries"+sharedLibraries.size());
		return true;
	}

}
