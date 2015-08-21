package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentBatchModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

public interface IBeJobConfigurable {

	public void setDataBucket(DataBucketBean dataBucketBean);


	public void setEnrichmentContext(IEnrichmentModuleContext enrichmentContext);


	public void setBeSharedLibrary(SharedLibraryBean beSharedLibrary);


	public void setEcMetadata(EnrichmentControlMetadataBean ecMetadata);


	public void setBatchSize(int size);


	public void setEnrichmentBatchModule(IEnrichmentBatchModule beModule);
	
	
}
