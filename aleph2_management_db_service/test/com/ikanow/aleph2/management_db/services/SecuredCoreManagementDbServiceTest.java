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
package com.ikanow.aleph2.management_db.services;

import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class SecuredCoreManagementDbServiceTest {
	private static final Logger logger = LogManager.getLogger(SecuredCoreManagementDbServiceTest.class);

	protected Config config = null;

	@Inject
	protected IServiceContext _service_context = null;

	protected IManagementDbService managementDbService;
	protected ISecurityService securityService = null;

	@Before
	public void setupDependencies() throws Exception {
		try {
			
		if (_service_context != null) {
			return;
		}

		final String temp_dir = System.getProperty("java.io.tmpdir");

		// OK we're going to use guice, it was too painful doing this by hand...
		config = ConfigFactory.parseReader(new InputStreamReader(this.getClass().getResourceAsStream("/test_secured_core_managment_service.properties")))
				.withValue("globals.local_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
				.withValue("globals.local_cached_jar_dir", ConfigValueFactory.fromAnyRef(temp_dir))
				.withValue("globals.distributed_root_dir", ConfigValueFactory.fromAnyRef(temp_dir))
				.withValue("globals.local_yarn_config_dir", ConfigValueFactory.fromAnyRef(temp_dir));

		Injector app_injector = ModuleUtils.createTestInjector(Arrays.asList(), Optional.of(config));	
		app_injector.injectMembers(this);
		this.managementDbService = _service_context.getCoreManagementDbService();
		this.securityService =  _service_context.getSecurityService();
		} catch(Throwable e) {
			
			e.printStackTrace();
		}
	}


	@Test
	public void testSharedLibraryAccess(){
/*		try {
			String bucketFullName = "";
		}
			IManagementCrudService<DataBucketBean> dataBucketStore = managementDbService.getDataBucketStore();
			SingleQueryComponent<DataBucketBean> querydatBucketFullName = CrudUtils.anyOf(DataBucketBean.class).when("full_name",
					bucketFullName);
			Optional<DataBucketBean> odb = dataBucketStore.getObjectBySpec(querydatBucketFullName).get();
			if (odb.isPresent()) {
				DataBucketBean dataBucketBean = odb.get();
				// TODO hook in security check
				String ownerId = dataBucketBean.owner_id();

				List<EnrichmentControlMetadataBean> enrichmentConfigs = dataBucketBean.batch_enrichment_configs();
				for (EnrichmentControlMetadataBean ec : enrichmentConfigs) {
					if (ec.name().equals(ecMetadataBeanName)) {
						logger.info("Loading libraries: " + bucketFullName);

						List<QueryComponent<SharedLibraryBean>> sharedLibsQuery = ec
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
						beJob = new BeJobBean(dataBucketBean, ecMetadataBeanName, sharedLibraries, bucketPathStr,bucketPathStr + "/managed_bucket/import/ready",bucketPathStr + "/managed_bucket/import/temp");
					} // if name
					else {
						logger.info("Skipping Enrichment, no enrichment found for bean:" + bucketFullName + " and enrichmentName:"
								+ ecMetadataBeanName);
					}
				} // for
			} // odb present
		} catch (Exception e) {
			logger.error("Caught exception loading shared libraries for job:" + bucketFullName, e);
*/
		}
	
}
