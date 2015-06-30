package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.MultiQueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;


public class BeJobLoader {
	private static final Logger logger = LogManager.getLogger(BeJobLoader.class);

	protected final IManagementDbService managementDbService;
	
	@Inject
	public BeJobLoader(IServiceContext serviceContext)
	{
		this.managementDbService = serviceContext.getCoreManagementDbService();
	}
	
	public BeJob loadBeJob(String bucketFullName, String bucketPathStr, String ecMetadataBeanName) {
		BeJob beJob = null;
		try {
			IManagementCrudService<DataBucketBean> dataBucketStore = managementDbService.getDataBucketStore();
			SingleQueryComponent<DataBucketBean> querydatBucketFullName = CrudUtils.anyOf(DataBucketBean.class).when("full_name",
					bucketFullName);

			Optional<DataBucketBean> odb = dataBucketStore.getObjectBySpec(querydatBucketFullName).get();
			if (odb.isPresent()) {
				DataBucketBean dataBucketBean = odb.get();

				List<EnrichmentControlMetadataBean> enrichmentConfigs = dataBucketBean.batch_enrichment_configs();
				for (EnrichmentControlMetadataBean ec : enrichmentConfigs) {
					if (ec.name().equals(ecMetadataBeanName)) {
						logger.info("Loading libraries: " + bucketFullName);

						List<SingleQueryComponent<SharedLibraryBean>> sharedLibsQuery = ec
								.library_ids_or_names()
								.stream()
								.map(name -> {
									return CrudUtils.anyOf(SharedLibraryBean.class)
											.when(SharedLibraryBean::_id, name)
											.when(SharedLibraryBean::path_name, name);
								}).collect(Collectors.toList());

						MultiQueryComponent<SharedLibraryBean> spec = CrudUtils.<SharedLibraryBean> anyOf(sharedLibsQuery);
						IManagementCrudService<SharedLibraryBean> shareLibraryStore = managementDbService.getSharedLibraryStore();

						List<SharedLibraryBean> sharedLibraries = StreamSupport.stream(
								shareLibraryStore.getObjectsBySpec(spec).get().spliterator(), false).collect(Collectors.toList());
						beJob = new BeJob(dataBucketBean, ec, sharedLibraries, bucketPathStr);
					} // if name
					else {
						logger.info("Skipping Enrichment, no enrichment found for bean:" + bucketFullName + " and enrichmentName:"
								+ ecMetadataBeanName);
					}
				} // for
			} // odb present
		} catch (Exception e) {
			logger.error("Caught exception loading shared libraries for job:" + bucketFullName, e);

		}
		return beJob;
	}

}
