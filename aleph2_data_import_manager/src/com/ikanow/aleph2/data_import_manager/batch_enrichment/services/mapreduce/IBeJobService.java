package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;


public interface IBeJobService {


	boolean runEnhancementJob(String bucketFullName, String bucketPathStr, String ecMetadataBeanName);

}
