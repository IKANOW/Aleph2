/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
package com.ikanow.aleph2.management_db.services;

import java.sql.Timestamp; //(provides a short cut for some datetime manipulation that doesn't confusingly refer to timezones)
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean.StateDirectoryType;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.ProcessingTestSpecBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.FutureUtils;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;
import com.ikanow.aleph2.management_db.controllers.actors.BucketDeletionActor;
import com.ikanow.aleph2.management_db.data_model.BucketActionRetryMessage;
import com.ikanow.aleph2.management_db.data_model.BucketMgmtMessage.BucketDeletionMessage;
import com.ikanow.aleph2.management_db.module.CoreManagementDbModule;

/** A layer that sits in between the managers and modules on top, and the actual database technology underneath,
 *  and performs control activities (launching into Akka) and an additional layer of validation
 * @author acp
 */
public class CoreManagementDbService implements IManagementDbService, IExtraDependencyLoader {
	@SuppressWarnings("unused")
	private static final Logger _logger = LogManager.getLogger();	

	protected final IServiceContext _service_context;
	protected final IManagementDbService _underlying_management_db;	
	protected final DataBucketCrudService _data_bucket_service;
	protected final DataBucketStatusCrudService _data_bucket_status_service;
	protected final SharedLibraryCrudService _shared_library_service;
	
	/** DON'T USE THIS DIRECTLY, ONLY VIA getBucketDeletionQueue
	 * (lazily constructed) 
	 */
	protected ICrudService<?> _bucket_deletion_q = null;
	
	protected final Optional<AuthorizationBean> _auth;
	protected final Optional<ProjectBean> _project;	
	
	protected final boolean _read_only;
	
	/** Guice invoked constructor
	 * @param underlying_management_db
	 * @param data_bucket_service
	 */
	@Inject
	public CoreManagementDbService(final IServiceContext service_context,
			final DataBucketCrudService data_bucket_service, final DataBucketStatusCrudService data_bucket_status_service,
			final SharedLibraryCrudService shared_library_service
			)
	{
		//(just return null here if underlying management not present, things will fail catastrophically unless this is a test)
		_service_context = service_context;
		_underlying_management_db = service_context.getService(IManagementDbService.class, Optional.empty()).orElse(null);
		_data_bucket_service = data_bucket_service;
		_data_bucket_status_service = data_bucket_status_service;
		_shared_library_service = shared_library_service;
		
		_auth = Optional.empty();
		_project = Optional.empty();
		
		_read_only = false;
		
		//DEBUG
		//System.out.println("Hello world from: " + this.getClass() + ": bucket=" + _data_bucket_service);
		//System.out.println("Hello world from: " + this.getClass() + ": underlying=" + _underlying_management_db);

	}
	
	/** User constructor for building a cloned version with different auth settings
	 * @param crud_factory 
	 * @param auth_fieldname
	 * @param auth
	 * @param project
	 */
	public CoreManagementDbService(final IServiceContext service_context,
			final IManagementDbService underlying_management_db,
			final DataBucketCrudService data_bucket_service, final DataBucketStatusCrudService data_bucket_status_service,
			final SharedLibraryCrudService shared_library_service,		
			final Optional<AuthorizationBean> auth, final Optional<ProjectBean> project, boolean read_only) {
		_service_context = service_context;
		_underlying_management_db = underlying_management_db;
		_data_bucket_service = data_bucket_service;
		_data_bucket_status_service = data_bucket_status_service;
		_shared_library_service = shared_library_service;
		
		_auth = auth;
		_project = project;		
		
		_read_only = read_only;
	}
	
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getFilteredDb(java.lang.String, java.util.Optional, java.util.Optional)
	 */
	public IManagementDbService getFilteredDb(final Optional<AuthorizationBean> client_auth, final Optional<ProjectBean> project_auth)
	{
		return new CoreManagementDbService(_service_context, _underlying_management_db, 
				_data_bucket_service, _data_bucket_status_service, _shared_library_service,
				client_auth, project_auth, _read_only);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getSharedLibraryStore()
	 */
	public IManagementCrudService<SharedLibraryBean> getSharedLibraryStore() {
		if (!_read_only)
			ManagementDbActorContext.get().getDistributedServices().waitForAkkaJoin(Optional.empty());
		return _shared_library_service.readOnlyVersion(_read_only);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getPerLibraryState(java.lang.Class, com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean, java.util.Optional)
	 */
	public <T> ICrudService<T> getPerLibraryState(Class<T> clazz,
			SharedLibraryBean library, Optional<String> sub_collection) {		
		return _underlying_management_db.getPerLibraryState(clazz, library, sub_collection).readOnlyVersion(_read_only);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getDataBucketStore()
	 */
	public IManagementCrudService<DataBucketBean> getDataBucketStore() {
		if (!_read_only)
			ManagementDbActorContext.get().getDistributedServices().waitForAkkaJoin(Optional.empty());
		return _data_bucket_service.readOnlyVersion(_read_only);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getDataBucketStatusStore()
	 */
	public IManagementCrudService<DataBucketStatusBean> getDataBucketStatusStore() {
		if (!_read_only)
			ManagementDbActorContext.get().getDistributedServices().waitForAkkaJoin(Optional.empty());
		return _data_bucket_status_service.readOnlyVersion(_read_only);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getPerBucketState(java.lang.Class, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional)
	 */
	public <T> ICrudService<T> getBucketHarvestState(Class<T> clazz,
			DataBucketBean bucket, Optional<String> sub_collection) {
		return _underlying_management_db.getBucketHarvestState(clazz, bucket, sub_collection).readOnlyVersion(_read_only);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getPerBucketState(java.lang.Class, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional)
	 */
	public <T> ICrudService<T> getBucketEnrichmentState(Class<T> clazz,
			DataBucketBean bucket, Optional<String> sub_collection) {
		return _underlying_management_db.getBucketEnrichmentState(clazz, bucket, sub_collection).readOnlyVersion(_read_only);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getPerAnalyticThreadState(java.lang.Class, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean, java.util.Optional)
	 */
	public <T> ICrudService<T> getBucketAnalyticThreadState(Class<T> clazz,
			DataBucketBean bucket, Optional<String> sub_collection) {
		return _underlying_management_db.getBucketAnalyticThreadState(clazz, bucket, sub_collection).readOnlyVersion(_read_only);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		throw new RuntimeException("No underlying drivers for CoreManagementDbService - did you want to get the underlying IManagementDbService? Use IServiceContext.getService(IManagementDbService.class, ...) if so.");
	}

	/** This service needs to load some additional classes via Guice. Here's the module that defines the bindings
	 * @return
	 */
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList((Module)new CoreManagementDbModule());
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IExtraDependencyLoader#youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules()
	 */
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		// (done see above)		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getRetryStore(java.lang.Class)
	 */
	@Override
	public <T> ICrudService<T> getRetryStore(
			Class<T> retry_message_clazz) {
		if (!_read_only)
			ManagementDbActorContext.get().getDistributedServices().waitForAkkaJoin(Optional.empty());		
		return _underlying_management_db.getRetryStore(retry_message_clazz).readOnlyVersion(_read_only);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getBucketDeletionQueue(java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> ICrudService<T> getBucketDeletionQueue(
			Class<T> deletion_queue_clazz) {
		if (!_read_only)
			ManagementDbActorContext.get().getDistributedServices().waitForAkkaJoin(Optional.empty());		
		if (null == _bucket_deletion_q) {
			_bucket_deletion_q = _underlying_management_db.getBucketDeletionQueue(deletion_queue_clazz).readOnlyVersion(_read_only);
		}
		return (ICrudService<T>) _bucket_deletion_q;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getStateDirectory(java.util.Optional)
	 */
	@Override
	public ICrudService<AssetStateDirectoryBean> getStateDirectory(
			Optional<DataBucketBean> bucket_filter, Optional<StateDirectoryType> type_filter) {
		return _underlying_management_db.getStateDirectory(bucket_filter, type_filter).readOnlyVersion(_read_only);
	}
	

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		final LinkedList<Object> ll = new LinkedList<Object>();
		ll.add(this);
		ll.addAll(_underlying_management_db.getUnderlyingArtefacts());
		return ll;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService#readOnlyVersion()
	 */
	@Override
	public IManagementDbService readOnlyVersion() {
		return new CoreManagementDbService(_service_context, _underlying_management_db, 
				_data_bucket_service, _data_bucket_status_service, _shared_library_service,
				_auth, _project, true);
	}

	@Override
	public ManagementFuture<Boolean> purgeBucket(DataBucketBean to_purge, Optional<Duration> in) {
		
		//TODO (ALEPH-23): decide .. only allow this if bucket is suspended?
		
		if (in.isPresent()) { // perform scheduled purge
			
			final Date to_purge_date = Timestamp.from(Instant.now().plus(in.get().getSeconds(), ChronoUnit.SECONDS));			
			
			return FutureUtils.createManagementFuture(this.getBucketDeletionQueue(BucketDeletionMessage.class).storeObject(new BucketDeletionMessage(to_purge, to_purge_date, true), false)					
					.thenApply(__ -> true)
					.exceptionally(___ -> false)) // (fail if already present)
					; 
		}
		else { // purge now...
		
			final CompletableFuture<Collection<BasicMessageBean>> sys_res = BucketDeletionActor.deleteAllDataStoresForBucket(to_purge, _service_context, false);
			
			final CompletableFuture<Collection<BasicMessageBean>> user_res = BucketDeletionActor.notifyHarvesterOfPurge(to_purge, this.getDataBucketStatusStore(), this.getRetryStore(BucketActionRetryMessage.class));
			//(longer term I wonder if should allow the harvester reply to dictate the level of deletion, eg could return an _id and then only delete up to that id?)

			final CompletableFuture<Collection<BasicMessageBean>> combined_res = sys_res.thenCombine(user_res, (a, b) -> {
				return Stream.concat(a.stream(), b.stream()).collect(Collectors.toList());
			});
			
			return FutureUtils.createManagementFuture(
					combined_res.thenApply(msgs -> msgs.stream().allMatch(m -> m.success()))
					, 
					combined_res);
		}
	}

	@Override
	public ManagementFuture<Boolean> testBucket(DataBucketBean to_test, ProcessingTestSpecBean test_spec) {
		// TODO Auto-generated method stub
		
		//TODO: delegate straight over to DataBucketCrudService to avoid putting bucket specific logic here
		// maybe something like (in DBCS):
		// - validate the bucket 
		// - is there any test data already present for this user, delete if so (?)
		//   (I'm writing deletion logic atm so may need to TODO that out for now until I'm done)
		// - send messages to start the test (ie like other messages, via the BucketActionSupervisor)
		// - add to the test queue
		// anything else?
		
		return null;
	}

	@Override
	public <T> ICrudService<T> getBucketTestQueue(Class<T> test_queue_clazz) {
		return _underlying_management_db.getBucketTestQueue(test_queue_clazz);
	}

}
