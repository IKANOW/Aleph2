package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.util.List;

import org.apache.hadoop.fs.Path;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

public interface IBeJobService {

	boolean runEnhancementJob(DataBucketBean bucket, EnrichmentControlMetadataBean ec, List<SharedLibraryBean> sharedLibraries,
			Path bucketInput, Path bucketOutput);

}
