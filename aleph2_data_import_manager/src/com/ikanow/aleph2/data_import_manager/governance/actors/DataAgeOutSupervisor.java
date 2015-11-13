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
package com.ikanow.aleph2.data_import_manager.governance.actors;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;




import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;





import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;





import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.MethodNamingHelper;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;





import akka.actor.Cancellable;
import akka.actor.UntypedActor;

/** Responsible for checking data vs the various age out fields in the data schema
 * @author Alex
 */
public class DataAgeOutSupervisor extends UntypedActor {
	protected static final Logger _logger = LogManager.getLogger();	

	protected final ManagementDbActorContext _actor_context;
	protected final IServiceContext _context;
	protected final IManagementDbService _core_management_db;
	protected final IManagementDbService _underlying_management_db;
	protected final SetOnce<ICrudService<DataBucketBean>> _bucket_crud = new SetOnce<>();	
	protected final SetOnce<Cancellable> _ticker = new SetOnce<>();
	
	final protected static MethodNamingHelper<DataSchemaBean> _schema_fields = BeanTemplateUtils.from(DataBucketBean.class).nested(DataBucketBean::data_schema, DataSchemaBean.class);
	final protected static MethodNamingHelper<DataSchemaBean.TemporalSchemaBean> _time_fields = _schema_fields.nested(DataSchemaBean::temporal_schema, DataSchemaBean.TemporalSchemaBean.class);
	final protected static MethodNamingHelper<DataSchemaBean.StorageSchemaBean> _disk_fields = _schema_fields.nested(DataSchemaBean::storage_schema, DataSchemaBean.StorageSchemaBean.class);
	final protected static MethodNamingHelper<DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean> _disk_fields_raw = _disk_fields.nested(DataSchemaBean.StorageSchemaBean::raw, DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean.class);
	final protected static MethodNamingHelper<DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean> _disk_fields_json = _disk_fields.nested(DataSchemaBean.StorageSchemaBean::json, DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean.class);
	final protected static MethodNamingHelper<DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean> _disk_fields_px = _disk_fields.nested(DataSchemaBean.StorageSchemaBean::processed, DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean.class);
	
	//TODO (ALEPH-40): Add DateAgeOut workers on a round robin message bus so we can distribute the load
	
	/** Akka c'tor
	 */
	public DataAgeOutSupervisor() {
		_actor_context = ManagementDbActorContext.get();
		
		_context = _actor_context.getServiceContext();
		_core_management_db = Lambdas.get(() -> { try { return _context.getCoreManagementDbService(); } catch (Exception e) { return null; } });
		_underlying_management_db = Lambdas.get(() -> { try { return _context.getService(IManagementDbService.class, Optional.empty()).get();  } catch (Exception e) { return null; } });
		// (must exist if _core_management_db)

		if (null != _core_management_db) {
			final FiniteDuration poll_delay = Duration.create(1, TimeUnit.SECONDS);
			final FiniteDuration poll_frequency = Duration.create(30, TimeUnit.MINUTES); // (runs every 30m)
			_ticker.set(this.context().system().scheduler()
						.schedule(poll_delay, poll_frequency, this.self(), "Tick", this.context().system().dispatcher(), null));
			
			_logger.info("DataAgeOutSupervisor has started on this node.");						
		}		
	}
	
	/** Workaround for the usual Guice-related issues
	 */
	public void setup() {
		if (!_bucket_crud.isSet()) {
			
			final IManagementCrudService<DataBucketBean> writable_crud = _underlying_management_db.getDataBucketStore();
			_bucket_crud.set(writable_crud.readOnlyVersion());
			
			// Optimize the query the the age out manager is going to make
			
			writable_crud.optimizeQuery(Arrays.asList(_time_fields.field(DataSchemaBean.TemporalSchemaBean::exist_age_max))).join();
			writable_crud.optimizeQuery(Arrays.asList(_disk_fields_raw.field(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::exist_age_max))).join();
			writable_crud.optimizeQuery(Arrays.asList(_disk_fields_json.field(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::exist_age_max))).join();
			writable_crud.optimizeQuery(Arrays.asList(_disk_fields_px.field(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::exist_age_max))).join();
		}
	}
	
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
	 */
	@Override
	public void onReceive(final Object message) throws Exception {
		setup(); // (only does anything first time through)
		
		if (String.class.isAssignableFrom(message.getClass())) { // tick!
			
			final QueryComponent<DataBucketBean> query = CrudUtils.anyOf(DataBucketBean.class)
					.withPresent(_time_fields.field(DataSchemaBean.TemporalSchemaBean::exist_age_max))
					.withPresent(_disk_fields_raw.field(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::exist_age_max))
					.withPresent(_disk_fields_json.field(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::exist_age_max))
					.withPresent(_disk_fields_px.field(DataSchemaBean.StorageSchemaBean.StorageSubSchemaBean::exist_age_max))
					;

			_bucket_crud.get().getObjectsBySpec(query).thenAccept(cursor -> {
				_logger.info(ErrorUtils.get("DataAgeOutSupervisor checking age out on {0} bucket(s)", cursor.count()));
				
				StreamSupport.stream(cursor.spliterator(), true)
					.forEach(bucket -> {

						// Currently supported: search index service and storage service
						
						Arrays.asList(
							_context.getSearchIndexService().flatMap(ISearchIndexService::getDataService),
							_context.getStorageService().getDataService()								
							)
							.stream()
							.filter(Optional::isPresent)
							.map(Optional::get)
							.forEach(data_service -> {
								data_service.handleAgeOutRequest(bucket).thenAccept(return_val -> {
									
									if (return_val.success()) {
										// (only print out if there's something interesting to say)
										Optional.of(return_val.details()).filter(m -> m.containsKey("loggable"))
												.ifPresent(__ -> _logger.info(ErrorUtils.get("Bucket {0}:  {1}", bucket.full_name(), return_val.message())));
									}
									else {
										_logger.warn(ErrorUtils.get("Bucket {0}:  {1}", bucket.full_name(), return_val.message()));
									}
								});
							})
							;
					});
			});
		}		
	}

	
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#postStop()
	 */
	@Override
	public void postStop() {
		if (_ticker.isSet()) {
			_ticker.get().cancel();
		}
		_logger.info("DataAgeOutSupervisor has stopped on this node.");								
	}
}
