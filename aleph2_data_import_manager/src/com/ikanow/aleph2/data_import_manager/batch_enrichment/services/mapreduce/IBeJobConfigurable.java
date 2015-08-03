package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

public interface IBeJobConfigurable {

	public void setDataBucket(DataBucketBean dataBucketBean);


	void setEnrichmentContext(IEnrichmentModuleContext enrichmentContext);


	void setBeSharedLibrary(SharedLibraryBean beSharedLibrary);


	void setEcMetadata(EnrichmentControlMetadataBean ecMetadata);
	
	
}
