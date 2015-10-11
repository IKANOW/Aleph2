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
package com.ikanow.aleph2.analytics.services;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.ikanow.aleph2.analytics.utils.ErrorUtils;
import com.ikanow.aleph2.core.shared.utils.LiveInjector;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobOutputBean;
import com.ikanow.aleph2.data_model.objects.data_import.AnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean.StateDirectoryType;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.PropertiesUtils;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.BeanTemplate;
import com.ikanow.aleph2.data_model.utils.CrudUtils.MultiQueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;

import fj.data.Either;

/** The implementation of the analytics context interface
 * @author Alex
 */
public class AnalyticsContext implements IAnalyticsContext {
	protected static final Logger _logger = LogManager.getLogger();	

	////////////////////////////////////////////////////////////////
	
	// CONSTRUCTION
	
	public static final String __MY_BUCKET_ID = "3fdb4bfa-2024-11e5-b5f7-727283247c7e";	
	public static final String __MY_TECH_LIBRARY_ID = "3fdb4bfa-2024-11e5-b5f7-727283247c7f";
	public static final String __MY_MODULE_LIBRARY_ID = "3fdb4bfa-2024-11e5-b5f7-727283247cff";
	public static final String __MY_JOB_ID = "3fdb4bfa-2024-11e5-b5f7-7272832480f0";
	
	private static final EnumSet<MasterEnrichmentType> _streaming_types = EnumSet.of(MasterEnrichmentType.streaming, MasterEnrichmentType.streaming_and_batch);	
	private static final EnumSet<MasterEnrichmentType> _batch_types = EnumSet.of(MasterEnrichmentType.batch, MasterEnrichmentType.streaming_and_batch);	
	
	protected static class MutableState {
		SetOnce<DataBucketBean> bucket = new SetOnce<>();
		SetOnce<AnalyticThreadJobBean> job = new SetOnce<>();
		SetOnce<SharedLibraryBean> technology_config = new SetOnce<>();
		SetOnce<Map<String, SharedLibraryBean>> library_configs = new SetOnce<>();
		final SetOnce<ImmutableSet<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> service_manifest_override = new SetOnce<>();
		final SetOnce<String> signature_override = new SetOnce<>();		
	};	
	protected final MutableState _mutable_state = new MutableState(); 
	
	public enum State { IN_TECHNOLOGY, IN_MODULE };
	protected final State _state_name;	
	
	// (stick this injection in and then call injectMembers in IN_MODULE case)
	@Inject protected IServiceContext _service_context;	
	protected IManagementDbService _core_management_db;
	protected ICoreDistributedServices _distributed_services; 	
	protected ISearchIndexService _index_service;
	protected IStorageService _storage_service;
	protected ISecurityService _security_service;
	protected GlobalPropertiesBean _globals;

	protected final ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());	
	
	// For writing objects out
	// TODO (ALEPH-12): this needs to get moved into the object output library
	protected Optional<IDataWriteService<JsonNode>> _crud_index_service;
	protected Optional<IDataWriteService.IBatchSubservice<JsonNode>> _batch_index_service;
	protected Optional<IDataWriteService<JsonNode>> _crud_storage_service;
	protected Optional<IDataWriteService.IBatchSubservice<JsonNode>> _batch_storage_service;
	
	private static ConcurrentHashMap<String, AnalyticsContext> static_instances = new ConcurrentHashMap<>();
	
	/**Guice injector
	 * @param service_context
	 */
	@Inject 
	public AnalyticsContext(final IServiceContext service_context) {
		_state_name = State.IN_TECHNOLOGY;
		_service_context = service_context;
		_core_management_db = service_context.getCoreManagementDbService(); // (actually returns the _core_ management db service)
		_distributed_services = service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();		
		_index_service = service_context.getService(ISearchIndexService.class, Optional.empty()).get();
		_storage_service = service_context.getStorageService();
		_security_service = service_context.getSecurityService();
		_globals = service_context.getGlobalProperties();
	}

	/** In-module constructor
	 */
	public AnalyticsContext() {
		_state_name = State.IN_MODULE;
		
		// Can't do anything until initializeNewContext is called
	}	
	
	/** (FOR INTERNAL DATA MANAGER USE ONLY) Sets the bucket for this context instance
	 * @param this_bucket - the bucket to associate
	 * @returns whether the bucket has been updated (ie fails if it's already been set)
	 */
	public boolean setBucket(final DataBucketBean this_bucket) {
		return _mutable_state.bucket.set(this_bucket);
	}
	
	/** (FOR INTERNAL DATA MANAGER USE ONLY) Sets the technology library bean for this context instance
	 * @param this_bucket - the library bean to be associated
	 * @returns whether the library bean has been updated (ie fails if it's already been set)
	 */
	public boolean setTechnologyConfig(final SharedLibraryBean lib_config) {
		return _mutable_state.technology_config.set(lib_config);
	}
	
	/** (FOR INTERNAL DATA MANAGER USE ONLY) Sets the optional module library bean for this context instance
	 * @param lib_configs - the library beans to be associated
	 */
	@SuppressWarnings("deprecation")
	public void resetLibraryConfigs(final Map<String, SharedLibraryBean> lib_configs) {
		_mutable_state.library_configs.forceSet(lib_configs);
	}	
	
	/** (FOR INTERNAL DATA MANAGER USE ONLY) Sets the job for this context
	 * @param job - the job for this context
	 */
	@SuppressWarnings("deprecation")
	public void resetJob(final AnalyticThreadJobBean job) {
		_mutable_state.job.forceSet(job);
	}
	
	/** Gets the job if one has been set else is empty
	 * @return
	 */
	public Optional<AnalyticThreadJobBean> getJob() {
		return Optional.of(_mutable_state.job).filter(SetOnce::isSet).map(SetOnce::get);
	}
	
	/** A very simple container for library beans
	 * @author Alex
	 */
	public static class LibraryContainerBean {
		LibraryContainerBean() {}
		LibraryContainerBean(Collection<SharedLibraryBean> libs) { this.libs = new ArrayList<>(libs); }
		List<SharedLibraryBean> libs;
	}
	
	/** FOR DEBUGGING AND TESTING ONLY, inserts a copy of the current context into the saved "in module" versions
	 */
	public void overrideSavedContext() {
		static_instances.put(_mutable_state.signature_override.get(), this);
	}

	/** Gets the secondary buffer (deletes any existing data, and switches to "ping" on an uninitialized index)
	 * @param bucket
	 * @param need_ping_pong_buffer - based on the job.output
	 * @param data_service
	 * @return
	 */
	protected static Optional<String> getSecondaryBuffer(final DataBucketBean bucket, final boolean need_ping_pong_buffer, final IGenericDataService data_service)
	{
		if (need_ping_pong_buffer) {
			final Optional<String> write_buffer = 
					data_service.getPrimaryBufferName(bucket).map(Optional::of)
						.orElseGet(() -> { // Two cases:
							
							final Set<String> secondaries = data_service.getSecondaryBuffers(bucket);
							final int ping_pong_count = (secondaries.contains(IGenericDataService.SECONDARY_PING) ? 1 : 0)
													+ (secondaries.contains(IGenericDataService.SECONDARY_PONG) ? 1 : 0);
							
							if (1 == ping_pong_count) { // 1) one of ping/pong exists but not the other ... this is the file case where we can't tell what the primary actually is
								if (secondaries.contains(IGenericDataService.SECONDARY_PONG)) { //(eg pong is secondary so ping must be primary)
									return Optional.of(IGenericDataService.SECONDARY_PING);
								}
								else return Optional.of(IGenericDataService.SECONDARY_PONG);
							}
							else { // 2) all other cases: this is the ES case where we haven't built
								if (0 == ping_pong_count) { // first time through, create the buffers:
									data_service.getWritableDataService(JsonNode.class, bucket, Optional.empty(), Optional.of(IGenericDataService.SECONDARY_PONG));
									data_service.getWritableDataService(JsonNode.class, bucket, Optional.empty(), Optional.of(IGenericDataService.SECONDARY_PING));
								}								
								final Optional<String> curr_primary = Optional.of(IGenericDataService.SECONDARY_PING);
								final CompletableFuture<BasicMessageBean> future_res = data_service.switchCrudServiceToPrimaryBuffer(bucket, curr_primary, Optional.empty());							
								future_res.thenAccept(res -> {
									/**/
									System.out.println("?? "  + res.success() + ": switching between ping/pong buffers: " + res.message() + ": " + data_service.getPrimaryBufferName(bucket));
									//TODO: ^
									if (!res.success()) {
										_logger.error("Error switching between ping/pong buffers: " + res.message());
									}
								});
								return curr_primary;
							}
						})
						.map(curr_pri -> { // then just pick the buffer that isn't the primary
							if (IGenericDataService.SECONDARY_PING.equals(curr_pri)) {
								return IGenericDataService.SECONDARY_PONG;
							}
							else return IGenericDataService.SECONDARY_PING;							
						});			
			
			return write_buffer;			
		}
		else return Optional.empty();						
	};
	
	/** Setup the outputs for the given bucket
	 */
	protected void setupOutputs(final DataBucketBean bucket, final AnalyticThreadJobBean job) {
		final boolean need_ping_pong_buffer = needPingPongBuffer(job);

		_batch_index_service = 
				(_crud_index_service = _index_service.getDataService()
											.flatMap(s -> s.getWritableDataService(JsonNode.class, bucket, Optional.empty(), 
													getSecondaryBuffer(bucket, need_ping_pong_buffer, s)))
				)
				.flatMap(IDataWriteService::getBatchWriteSubservice)
				;

		_batch_storage_service = 
				(_crud_storage_service = _storage_service.getDataService()
											.flatMap(s -> s.getWritableDataService(JsonNode.class, bucket, 
															Optional.of(IStorageService.StorageStage.processed.toString()), 
																getSecondaryBuffer(bucket, need_ping_pong_buffer, s)))
				)
				.flatMap(IDataWriteService::getBatchWriteSubservice)
				;
		
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#initializeNewContext(java.lang.String)
	 */
	@Override
	public void initializeNewContext(final String signature) {
		try {
			// Inject dependencies
			final Config parsed_config = ConfigFactory.parseString(signature);
			final AnalyticsContext to_clone = static_instances.get(signature);
			
			if (null != to_clone) { //copy the fields				
				_service_context = to_clone._service_context;
				_core_management_db = to_clone._core_management_db;
				_security_service = to_clone._security_service;
				_distributed_services = to_clone._distributed_services;	
				_index_service = to_clone._index_service;
				_storage_service = to_clone._storage_service;
				_globals = to_clone._globals;
				// (apart from bucket, which is handled below, rest of mutable state is not needed)
			}
			else {				
				ModuleUtils.initializeApplication(Collections.emptyList(), Optional.of(parsed_config), Either.right(this));

				_core_management_db = _service_context.getCoreManagementDbService(); // (actually returns the _core_ management db service)
				_distributed_services = _service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
				_index_service = _service_context.getService(ISearchIndexService.class, Optional.empty()).get();
				_storage_service = _service_context.getStorageService();
				_security_service = _service_context.getSecurityService();
				_globals = _service_context.getGlobalProperties();
			}			
			// Get bucket 

			final BeanTemplate<DataBucketBean> retrieve_bucket = BeanTemplateUtils.from(parsed_config.getString(__MY_BUCKET_ID), DataBucketBean.class);
			_mutable_state.bucket.set(retrieve_bucket.get());
			final BeanTemplate<SharedLibraryBean> retrieve_library = BeanTemplateUtils.from(parsed_config.getString(__MY_TECH_LIBRARY_ID), SharedLibraryBean.class);
			_mutable_state.technology_config.set(retrieve_library.get());
			if (parsed_config.hasPath(__MY_MODULE_LIBRARY_ID)) {
				final BeanTemplate<LibraryContainerBean> retrieve_module = BeanTemplateUtils.from(parsed_config.getString(__MY_MODULE_LIBRARY_ID), LibraryContainerBean.class);
				_mutable_state.library_configs.set(
						Optional.ofNullable(retrieve_module.get().libs).orElse(Collections.emptyList())
									.stream()
									// (split each lib bean into 2 tuples, ie indexed by _id and path_name)
									.flatMap(mod -> Arrays.asList(Tuples._2T(mod._id(), mod), Tuples._2T(mod.path_name(), mod)).stream())
									.collect(Collectors.toMap(
											t2 -> t2._1()
											, 
											t2 -> t2._2()
											,
											(t1, t2) -> t1 // (can't happen, ignore if it does)
											,
											() -> new LinkedHashMap<String, SharedLibraryBean>()
											))
						);
			}
			if (parsed_config.hasPath(__MY_JOB_ID)) {
				final String job_name = parsed_config.getString(__MY_JOB_ID);
				
				Optionals.of(() -> retrieve_bucket.get().analytic_thread().jobs()).orElse(Collections.emptyList())
					.stream()
					.filter(job -> job_name.equals(job.name()))
					.findFirst()
					.ifPresent(job -> _mutable_state.job.trySet(job));

				getJob().ifPresent(job -> 
					setupOutputs(_mutable_state.bucket.get(), job)
				);
			}			
			static_instances.put(signature, this);
		}
		catch (Exception e) {
			//DEBUG
			//System.out.println(ErrorUtils.getLongForm("{0}", e));			

			throw new RuntimeException(e);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getEnrichmentContextSignature(java.util.Optional)
	 */
	@Override
	public String getAnalyticsContextSignature(final Optional<DataBucketBean> bucket, final Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services) {
		if (_state_name == State.IN_TECHNOLOGY) {
			final DataBucketBean my_bucket = bucket.orElseGet(() -> _mutable_state.bucket.get());
			
			// Returns a config object containing:
			// - set up for any of the services described
			// - all the rest of the configuration
			// - the bucket bean ID
			
			final Config full_config = ModuleUtils.getStaticConfig()
										.withoutPath(DistributedServicesPropertyBean.APPLICATION_NAME)
										.withoutPath("MongoDbManagementDbService.v1_enabled") // (special workaround for V1 sync service)
										;
	
			final Optional<Config> service_config = PropertiesUtils.getSubConfig(full_config, "service");
			
			final ImmutableSet<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>> complete_services_set = 
					ImmutableSet.<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>builder()
							.addAll(services.orElse(Collections.emptySet()))
							.add(Tuples._2T(ICoreDistributedServices.class, Optional.empty()))
							.add(Tuples._2T(IManagementDbService.class, Optional.empty()))
							.add(Tuples._2T(ISearchIndexService.class, Optional.empty()))
							.add(Tuples._2T(ISecurityService.class, Optional.empty()))
							.add(Tuples._2T(IStorageService.class, Optional.empty()))
							.add(Tuples._2T(IManagementDbService.class, IManagementDbService.CORE_MANAGEMENT_DB))
							.build();
			
			if (_mutable_state.service_manifest_override.isSet()) {
				if (!complete_services_set.equals(_mutable_state.service_manifest_override.get())) {
					throw new RuntimeException(ErrorUtils.SERVICE_RESTRICTIONS);
				}
			}
			else {
				_mutable_state.service_manifest_override.set(complete_services_set);
			}
			
			final Config config_no_services = full_config.withoutPath("service");
			
			// Ugh need to add: core deps, core + underlying management db to this list
			
			final Config service_subset = complete_services_set.stream() // DON'T MAKE PARALLEL SEE BELOW
				.map(clazz_name -> {
					final String config_path = clazz_name._2().orElse(clazz_name._1().getSimpleName().substring(1));
					return service_config.get().hasPath(config_path) 
							? Tuples._2T(config_path, service_config.get().getConfig(config_path)) 
							: null;
				})
				.filter(cfg -> null != cfg)
				.reduce(
						ConfigFactory.empty(),
						(acc, k_v) -> acc.withValue(k_v._1(), k_v._2().root()),
						(acc1, acc2) -> acc1 // (This will never be called as long as the above stream is not parallel)
						);
				
			final Config config_subset_services = config_no_services.withValue("service", service_subset.root());
			
			final Config last_call = 
					Lambdas.<Config, Config>wrap_u(config_pipeline -> 
						_mutable_state.library_configs.isSet()
						?
						config_pipeline
							.withValue(__MY_MODULE_LIBRARY_ID, 
									ConfigValueFactory
										.fromAnyRef(BeanTemplateUtils.toJson(
												new LibraryContainerBean(
														_mutable_state.library_configs.get().entrySet().stream()
															.filter(kv -> kv.getValue().path_name().equals(kv.getKey()))
															.map(kv -> kv.getValue())
															.collect(Collectors.toList())
													)
												).toString())
										)
						:
						config_pipeline						
					)
					.andThen(Lambdas.wrap_u(config_pipeline ->
						getJob().isPresent()
						?
						config_pipeline
							.withValue(__MY_JOB_ID, 
									ConfigValueFactory
									.fromAnyRef(getJob().get().name()) //(exists by above "if")
									)
						: config_pipeline
					))
					.apply(config_subset_services)
					.withValue(__MY_BUCKET_ID, 
								ConfigValueFactory
									.fromAnyRef(BeanTemplateUtils.toJson(my_bucket).toString())
									)
					.withValue(__MY_TECH_LIBRARY_ID, 
								ConfigValueFactory
									.fromAnyRef(BeanTemplateUtils.toJson(_mutable_state.technology_config.get()).toString())
									)
									;
			
			final String ret1 = last_call.root().render(ConfigRenderOptions.concise());
			_mutable_state.signature_override.set(ret1);
			final String ret = this.getClass().getName() + ":" + ret1;

			// Finally this is a good central place to sort out deleting ping pong buffers
			getJob().ifPresent(job -> {
				final boolean need_ping_pong_buffer = needPingPongBuffer(job);
	
				if (need_ping_pong_buffer) {
					setupOutputs(_mutable_state.bucket.get(), job);
	
					// Clean the data out from the buffers we're about to write into
					_crud_index_service.ifPresent(outputter -> {
						outputter.deleteDatastore().join();
					});
					_crud_storage_service.ifPresent(outputter -> {
						outputter.deleteDatastore().join();
					});
				}
			});		
			return ret;
		}
		else {
			throw new RuntimeException(ErrorUtils.TECHNOLOGY_NOT_MODULE);			
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		if (_state_name == State.IN_TECHNOLOGY) {
			if (!_mutable_state.service_manifest_override.isSet()) {
				throw new RuntimeException(ErrorUtils.SERVICE_RESTRICTIONS);				
			}
			return Stream.concat(
				Stream.of(this, _service_context)
				,
				_mutable_state.service_manifest_override.get().stream()
					.map(t2 -> _service_context.getService(t2._1(), t2._2()))
					.filter(service -> service.isPresent())
					.flatMap(service -> service.get().getUnderlyingArtefacts().stream())
			)
			.collect(Collectors.toList());
		}
		else {
			throw new RuntimeException(ErrorUtils.TECHNOLOGY_NOT_MODULE);			
		}
	}

	////////////////////////////////////////////////////////////////
	
	// OVERRIDES
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(final Class<T> driver_class,
			final Optional<String> driver_options) {
		return Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getService(java.lang.Class, java.util.Optional)
	 */
	@Override
	public IServiceContext getServiceContext() {
		return _service_context;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#getOutputPath(java.util.Optional, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean)
	 */
	@Override
	public Optional<Tuple2<String, Optional<String>>> getOutputPath(
			final Optional<DataBucketBean> bucket, final AnalyticThreadJobBean job) {
		// TODO (ALEPH-12)
		throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#getOutputTopic(java.util.Optional, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean)
	 */
	@Override
	public Optional<String> getOutputTopic(
			final Optional<DataBucketBean> bucket,
			final AnalyticThreadJobBean job)
	{
		final DataBucketBean this_bucket = bucket.orElseGet(() -> _mutable_state.bucket.get());		
		final AnalyticThreadJobOutputBean output = Optional.ofNullable(job.output()).orElseGet(() -> BeanTemplateUtils.build(AnalyticThreadJobOutputBean.class).done().get());		
		final boolean is_transient = Optional.ofNullable(output.is_transient()).orElse(false);
		
		if (_streaming_types.contains(Optional.ofNullable(output.transient_type()).orElse(MasterEnrichmentType.none))) {
			final String topic = is_transient
					? _distributed_services.generateTopicName(this_bucket.full_name(), Optional.of(job.name()))
					: 
					  _distributed_services.generateTopicName(Optional.ofNullable(output.sub_bucket_path()).orElse(this_bucket.full_name()), 
																ICoreDistributedServices.QUEUE_END_NAME)
					;
			return Optional.of(topic);
		}
		else return Optional.empty();
	}
	
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#getInputTopics(java.util.Optional, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputBean)
	 */
	@Override
	public List<String> getInputTopics(
			final Optional<DataBucketBean> bucket, final AnalyticThreadJobBean job,
			final AnalyticThreadJobInputBean job_input)
	{	
		final DataBucketBean my_bucket = bucket.orElseGet(() -> _mutable_state.bucket.get());
		
		final AuthorizationBean auth_bean = new AuthorizationBean(my_bucket.owner_id());
		final ICrudService<DataBucketBean> secured_bucket_crud = _core_management_db.readOnlyVersion().getDataBucketStore().secured(_service_context, auth_bean);
		
		return Optional.of(job_input)
				.filter(i -> "stream".equalsIgnoreCase(i.data_service()))
				.map(Lambdas.wrap_u(i -> {					
					//Topic naming: 5 cases: 
					// 1) i.resource_name_or_id is a bucket path, ie starts with "/", and then:
					// 1.1) if it ends ":name" then it points to a specific point in the bucket processing
					// 1.2) if it ends ":" or ":$start" then it points to the start of that bucket's processing (ie the output of its harvester) .. which corresponds to the queue name with no sub-channel
					// 1.3) otherwise it points to the end of the bucket's processing (ie immediately before it's output) ... which corresponds to the queue name with the sub-channel "$end"
					// 2) i.resource_name_or_id does not (start with a /), in which case:
					// 2.1) if it's a non-empty string, then it's the name of one the internal jobs (can interpret that as this.full_name + name)  
					// 2.2) if it's "" or null then it's pointing to the output of its own bucket's harvester
					
					final String[] bucket_subchannel = Lambdas.<String, String[]> wrap_u(s -> {
						if (s.startsWith("/")) { //1.*
							if (s.endsWith(":")) {
								return new String[] { s.substring(0, s.length() - 1), "" }; // (1.2a)
							}
							else {
								final String[] b_sc = s.split(":");
								if (1 == b_sc.length) {
									return new String[] { b_sc[0], "$end" }; // (1.3)
								}
								else if ("$start".equals(b_sc[1])) {
									return new String[] { b_sc[0], "" }; // (1.2b)									
								}
								else {
									return b_sc; //(1.1)
								}
							}
						}
						else { //2.*
							return new String[] { my_bucket.full_name(), s };
						}
					})
					.apply(i.resource_name_or_id())
					;
										
					// Check this bucket exists and I have read access to it
					if (!my_bucket.full_name().equals(bucket_subchannel[0])) {
						boolean found_bucket = secured_bucket_crud
							.getObjectBySpec(CrudUtils.allOf(DataBucketBean.class).when(DataBucketBean::full_name, bucket_subchannel[0]),
											Collections.emptyList(), // (don't want any part of the bucket, just whether it exists or not)
											true
									)
							.get()
							.isPresent()
							;
						if (!found_bucket) {
							throw new RuntimeException(ErrorUtils.get(ErrorUtils.BUCKET_NOT_FOUND_OR_NOT_READABLE, bucket_subchannel[0]));
						}
					}
					
					final String topic = _distributed_services.generateTopicName(bucket_subchannel[0], Optional.of(bucket_subchannel[1]).filter(s -> !s.isEmpty()));
					_distributed_services.createTopic(topic, Optional.empty());
					return topic;
				}))
				.map(i -> Arrays.asList(i))
				.orElse(Collections.emptyList())
				;
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#getInputPaths(java.util.Optional, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputBean)
	 */
	@Override
	public List<String> getInputPaths(
			final Optional<DataBucketBean> bucket, 
			final AnalyticThreadJobBean job,
			final AnalyticThreadJobInputBean job_input) {

		final DataBucketBean my_bucket = bucket.orElseGet(() -> _mutable_state.bucket.get());
		
		final AuthorizationBean auth_bean = new AuthorizationBean(my_bucket.owner_id());
		final ICrudService<DataBucketBean> secured_bucket_crud = _core_management_db.readOnlyVersion().getDataBucketStore().secured(_service_context, auth_bean);
		
		return Optional.of(job_input)
				.filter(i -> null != i.data_service())
				.filter(i -> "batch".equalsIgnoreCase(i.data_service()) || "storage_service".equalsIgnoreCase(i.data_service()))
				.map(Lambdas.wrap_u(i -> {					
					if ("batch".equalsIgnoreCase(i.data_service())) {
						if (null != i.filter()) {
							//(actually not sure if i ever plan to implement this?)
							throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED) + ": input.filter");
						}						
						
						final String[] bucket_subchannel = Lambdas.<String, String[]> wrap_u(s -> {
							
							// 1) If the resource starts with "/" then must point to an intermediate batch result of an external bucket
							// 2) If the resource is a pointer then

							if (s.startsWith("/")) { //1.*
								if (s.endsWith(":")) {
									return new String[] { s.substring(0, s.length() - 1), "" }; // (1.2a)
								}
								else {
									final String[] b_sc = s.split(":");
									if (1 == b_sc.length) {
										return new String[] { my_bucket.full_name(), "" };
									}
									else {
										return b_sc; //(1.1)
									}
								}
							}
							else { //2.*
								return new String[] { my_bucket.full_name(), s };
							}
						})
						.apply(Optional.ofNullable(i.resource_name_or_id()).orElse(""));

						final Optional<DataBucketBean> bucket_to_check = Lambdas.get(Lambdas.wrap_u(() -> {
							if (bucket_subchannel[0] == my_bucket.full_name()) {
								return Optional.of(my_bucket);
							}
							else {
								return secured_bucket_crud.getObjectBySpec(
										CrudUtils.allOf(DataBucketBean.class).when(DataBucketBean::full_name, bucket_subchannel[0])
										).get();
							}
						}));
						return Lambdas.get(() -> {
							if (!bucket_subchannel[0].equals(my_bucket.full_name()) || !bucket_subchannel[1].isEmpty())
							{
								bucket_to_check
									.map(input_bucket -> input_bucket.analytic_thread())
									.flatMap(a_thread -> Optional.ofNullable(a_thread.jobs()))
									.flatMap(jobs -> jobs.stream()
															.filter(j -> bucket_subchannel[1].equals(j.name()))
															.filter(j -> _batch_types.contains(Optionals.of(() -> j.output().transient_type()).orElse(MasterEnrichmentType.none)))
															.filter(j -> Optionals.of(() -> j.output().is_transient()).orElse(false))
															.findFirst())
									.orElseThrow(() -> new RuntimeException(
											ErrorUtils.get(ErrorUtils.INPUT_PATH_NOT_A_TRANSIENT_BATCH,
													my_bucket.full_name(), job.name(), bucket_subchannel[0], bucket_subchannel[1])));
					
								return Arrays.asList(_storage_service.getBucketRootPath() + bucket_subchannel[0] + IStorageService.TRANSIENT_DATA_SUFFIX + bucket_subchannel[1]);
							}
							else { // This is my input directory
								return Arrays.asList(_storage_service.getBucketRootPath() + my_bucket.full_name() + IStorageService.TO_IMPORT_DATA_SUFFIX);
							}
						});
					}
					else { // storage service ... 3 options :raw, :json, :processed (defaults to :processed)
						
						final String bucket_name = i.resource_name_or_id().split(":")[0];
						
						// Check we have authentication for this bucket:
						
						final boolean found_bucket = secured_bucket_crud
								.getObjectBySpec(CrudUtils.allOf(DataBucketBean.class).when(DataBucketBean::full_name, bucket_name),
												Collections.emptyList(), // (don't want any part of the bucket, just whether it exists or not)
												true
										)
								.get()
								.isPresent()
								;
						
						if (!found_bucket) {
							throw new RuntimeException(ErrorUtils.get(ErrorUtils.BUCKET_NOT_FOUND_OR_NOT_READABLE, bucket_name));
						}
						final String sub_service = 
										Patterns.match(i.resource_name_or_id()).<String>andReturn()
													.when(s -> s.endsWith(":raw"), __ -> "raw/current/") // (input paths are always from primary)
													.when(s -> s.endsWith(":json"), __ -> "json/current/")
													.otherwise(__ -> "processed/current/");
						
						//TODO: ALEPH-12 enable time-based filtering
						if (null != i.filter()) {
							throw new RuntimeException(ErrorUtils.get(ErrorUtils.NOT_YET_IMPLEMENTED) + ": input.filter");
						}
						final String suffix = "**/*";
						return Arrays.asList(_storage_service.getBucketRootPath() + bucket_name + IStorageService.STORED_DATA_SUFFIX + sub_service + suffix);
					}
				}))
				.orElse(Collections.emptyList())
				;		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#checkForListeners(java.util.Optional, java.util.Optional)
	 */
	@Override
	public boolean checkForListeners(final Optional<DataBucketBean> bucket, final AnalyticThreadJobBean job) {

		final DataBucketBean this_bucket = bucket.orElseGet(() -> _mutable_state.bucket.get());
		final AnalyticThreadJobOutputBean output = Optional.ofNullable(job.output()).orElseGet(() -> BeanTemplateUtils.build(AnalyticThreadJobOutputBean.class).done().get());		

		final String topic_name = Optional.ofNullable(output.is_transient()).orElse(false)
				? _distributed_services.generateTopicName(this_bucket.full_name(), Optional.of(job.name()))
				: 
				  _distributed_services.generateTopicName(Optional.ofNullable(output.sub_bucket_path()).orElse(this_bucket.full_name()), 
															ICoreDistributedServices.QUEUE_END_NAME)
				;		
		
		return _distributed_services.doesTopicExist(topic_name);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#getAnalyticsContextLibraries(java.util.Optional)
	 */
	@Override
	public List<String> getAnalyticsContextLibraries(
			final Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services) {
		
		// Consists of:
		// 1) This library 
		// 2) Libraries that are always needed:		
		//    - core distributed services (implicit)
		//    - management db (core + underlying + any underlying drivers)
		//    - data model 
		// 3) Any libraries associated with the services		
		
		if (_state_name == State.IN_TECHNOLOGY) {
			// This very JAR			
			final String this_jar = Lambdas.get(() -> {
				return LiveInjector.findPathJar(this.getClass(), "");	
			});
			
			// Data model			
			final String data_model_jar = Lambdas.get(() -> {
				return LiveInjector.findPathJar(_service_context.getClass(), "");	
			});
			
			// Libraries associated with services:
			final Set<String> user_service_class_files = services.map(set -> {
				return set.stream()
						.map(clazz_name -> _service_context.getService(clazz_name._1(), clazz_name._2()))
						.filter(service -> service.isPresent())
						.flatMap(service -> service.get().getUnderlyingArtefacts().stream())
						.map(artefact -> LiveInjector.findPathJar(artefact.getClass(), ""))
						.collect(Collectors.toSet());
			})
			.orElse(Collections.emptySet());
			
			// Mandatory services
			final Set<String> mandatory_service_class_files =
						Arrays.asList(
								_distributed_services.getUnderlyingArtefacts(),
								_service_context.getStorageService().getUnderlyingArtefacts(),
								_service_context.getCoreManagementDbService().getUnderlyingArtefacts() 
								)
							.stream()
							.flatMap(x -> x.stream())
							.map(service -> LiveInjector.findPathJar(service.getClass(), ""))
							.collect(Collectors.toSet());
			
			// Combine them together
			final List<String> ret_val = ImmutableSet.<String>builder()
							.add(this_jar)
							.add(data_model_jar)
							.addAll(user_service_class_files)
							.addAll(mandatory_service_class_files)
							.build()
							.stream()
							.filter(f -> (null != f) && !f.equals(""))
							.collect(Collectors.toList())
							;
			
			if (ret_val.isEmpty()) {
				_logger.warn("WARNING: no library files found, probably because this is running from an IDE - instead taking all JARs from: " + (_globals.local_root_dir() + "/lib/"));
			}
			
			return !ret_val.isEmpty()
					? ret_val
					:
					// Special case: no aleph2 libs found, this is almost certainly because this is being run from eclipse...
					Lambdas.get(() -> {
						try {
							return FileUtils.listFiles(new File(_globals.local_root_dir() + "/lib/"), new String[] { "jar" }, false)
										.stream()
										.map(File::toString)
										.collect(Collectors.toList());
						}
						catch (Exception e) {
							throw new RuntimeException("In eclipse/IDE mode, directory not found: " + (_globals.local_root_dir() + "/lib/"));
						}
					});
		}
		else {
			throw new RuntimeException(ErrorUtils.TECHNOLOGY_NOT_MODULE);
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#getAnalyticsLibraries(java.util.Optional)
	 */
	@Override
	public CompletableFuture<Map<String, String>> getAnalyticsLibraries(final Optional<DataBucketBean> bucket, final Collection<AnalyticThreadJobBean> jobs) {
		if (_state_name == State.IN_TECHNOLOGY) {
			
			final String name_or_id = jobs.stream().findFirst().get().analytic_technology_name_or_id();
			
			final SingleQueryComponent<SharedLibraryBean> tech_query = 
					CrudUtils.anyOf(SharedLibraryBean.class)
						.when(SharedLibraryBean::_id, name_or_id)
						.when(SharedLibraryBean::path_name, name_or_id);
			
			final List<SingleQueryComponent<SharedLibraryBean>> other_libs = 
				Optionals.ofNullable(jobs).stream()
					.flatMap(job -> Optionals.ofNullable(job.library_names_or_ids()).stream())
					.map(name -> {
						return CrudUtils.anyOf(SharedLibraryBean.class)
								.when(SharedLibraryBean::_id, name)
								.when(SharedLibraryBean::path_name, name);
					})
					.collect(Collector.of(
							LinkedList::new,
							LinkedList::add,
							(left, right) -> { left.addAll(right); return left; }
							));

			@SuppressWarnings("unchecked")
			final MultiQueryComponent<SharedLibraryBean> spec = CrudUtils.<SharedLibraryBean>anyOf(tech_query,
					other_libs.toArray(new SingleQueryComponent[other_libs.size()]));
			
			// Get the names or ids, get the shared libraries, get the cached ids (must be present)
			
			return this._core_management_db.readOnlyVersion().getSharedLibraryStore().getObjectsBySpec(spec, Arrays.asList("_id", "path_name"), true)
				.thenApply(cursor -> {
					return StreamSupport.stream(cursor.spliterator(), false)
						.collect(Collectors.<SharedLibraryBean, String, String>toMap(
								lib -> lib.path_name(), 
								lib -> _globals.local_cached_jar_dir() + "/" + lib._id() + ".cache.jar"));
				});
		}
		else {
			throw new RuntimeException(ErrorUtils.TECHNOLOGY_NOT_MODULE);			
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#getGlobalAnalyticTechnologyObjectStore(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <S> ICrudService<S> getGlobalAnalyticTechnologyObjectStore(
			final Class<S> clazz, final Optional<String> collection) {
		return this.getBucketObjectStore(clazz, Optional.empty(), collection, Optional.of(AssetStateDirectoryBean.StateDirectoryType.library));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#getLibraryObjectStore(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <S> Optional<ICrudService<S>> getLibraryObjectStore(final Class<S> clazz, final String name_or_id, final Optional<String> collection)
	{
		return Optional.ofNullable(this.getLibraryConfigs().get(name_or_id))
				.map(module_lib -> _core_management_db.getPerLibraryState(clazz, module_lib, collection));
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#getBucketObjectStore(java.lang.Class, java.util.Optional, java.util.Optional, java.util.Optional)
	 */
	@Override
	public <S> ICrudService<S> getBucketObjectStore(final Class<S> clazz,
			final Optional<DataBucketBean> bucket, final Optional<String> collection,
			final Optional<StateDirectoryType> type)
	{
		final Optional<DataBucketBean> this_bucket = 
				bucket
					.map(x -> Optional.of(x))
					.orElseGet(() -> _mutable_state.bucket.isSet() 
										? Optional.of(_mutable_state.bucket.get()) 
										: Optional.empty());
		
		return Patterns.match(type).<ICrudService<S>>andReturn()
				.when(t -> t.isPresent() && AssetStateDirectoryBean.StateDirectoryType.enrichment == t.get(), 
						__ -> _core_management_db.getBucketEnrichmentState(clazz, this_bucket.get(), collection))
				.when(t -> t.isPresent() && AssetStateDirectoryBean.StateDirectoryType.harvest == t.get(), 
						__ -> _core_management_db.getBucketHarvestState(clazz, this_bucket.get(), collection))
				// assume this is the technology context, most likely usage
				.when(t -> t.isPresent() && AssetStateDirectoryBean.StateDirectoryType.library == t.get(), 
						__ -> _core_management_db.getPerLibraryState(clazz, this.getTechnologyConfig(), collection))
				// default: analytics or not specified: analytics
				.otherwise(__ -> _core_management_db.getBucketAnalyticThreadState(clazz, this_bucket.get(), collection))
				;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#getBucket()
	 */
	@Override
	public Optional<DataBucketBean> getBucket() {
		return _mutable_state.bucket.isSet() ? Optional.of(_mutable_state.bucket.get()) : Optional.empty();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#getLibraryConfig()
	 */
	@Override
	public SharedLibraryBean getTechnologyConfig() {
		return _mutable_state.technology_config.get();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#getLibraryConfigs()
	 */
	@Override
	public Map<String, SharedLibraryBean> getLibraryConfigs() {
		return _mutable_state.library_configs.isSet()
				? _mutable_state.library_configs.get()
				: Collections.emptyMap();
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#getBucketStatus(java.util.Optional)
	 */
	@Override
	public CompletableFuture<DataBucketStatusBean> getBucketStatus(
			final Optional<DataBucketBean> bucket) {
		return this._core_management_db
				.readOnlyVersion()
				.getDataBucketStatusStore()
				.getObjectById(bucket.orElseGet(() -> _mutable_state.bucket.get())._id())
				.thenApply(opt_status -> opt_status.get());		
		// (ie will exception if not present)
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#logStatusForThreadOwner(java.util.Optional, com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean, boolean)
	 */
	@Override
	public void logStatusForThreadOwner(final Optional<DataBucketBean> bucket,
			final BasicMessageBean message, final boolean roll_up_duplicates) {
		throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#logStatusForThreadOwner(java.util.Optional, com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean)
	 */
	@Override
	public void logStatusForThreadOwner(final Optional<DataBucketBean> bucket,
			final BasicMessageBean message) {
		throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#emergencyDisableBucket(java.util.Optional)
	 */
	@Override
	public void emergencyDisableBucket(final Optional<DataBucketBean> bucket) {
		throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#emergencyQuarantineBucket(java.util.Optional, java.lang.String)
	 */
	@Override
	public void emergencyQuarantineBucket(
			final Optional<DataBucketBean> bucket,
			final String quarantine_duration)
	{
		throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#sendObjectToStreamingPipeline(java.util.Optional, java.util.Optional, fj.data.Either)
	 */
	@Override
	public void sendObjectToStreamingPipeline(final Optional<DataBucketBean> bucket,
			final AnalyticThreadJobBean job, 
			final Either<JsonNode, Map<String, Object>> object, 
			final Optional<AnnotationBean> annotations)
	{
		if (annotations.isPresent()) {
			throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);			
		}
		final JsonNode obj_json =  object.either(__->__, map -> (JsonNode) _mapper.convertValue(map, JsonNode.class));

		this.getOutputTopic(bucket, job).ifPresent(topic -> {	
			if (_distributed_services.doesTopicExist(topic)) {
				// (ie someone is listening in on our output data, so duplicate it for their benefit)
				_distributed_services.produce(topic, obj_json.toString());
			}
		});
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#getServiceInput(java.lang.Class, java.util.Optional, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputBean)
	 */
	@Override
	public <T extends IAnalyticsAccessContext<?>> Optional<T> getServiceInput(
			final Class<T> clazz, 
			final Optional<DataBucketBean> bucket,
			final AnalyticThreadJobBean job, 
			final AnalyticThreadJobInputBean job_input)
	{	
		final Optional<String> job_config = Optional.of(BeanTemplateUtils.toJson(job_input).toString());
		if ("storage_service".equalsIgnoreCase(Optional.ofNullable(job_input.data_service()).orElse(""))) {
			return _storage_service.getUnderlyingPlatformDriver(clazz, job_config);
		}
		else if ("search_index_service".equalsIgnoreCase(Optional.ofNullable(job_input.data_service()).orElse(""))) {			
			return _storage_service.getUnderlyingPlatformDriver(clazz, job_config);
		}
		else { // (currently no other  
			return Optional.empty();
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#getServiceOutput(java.lang.Class, java.util.Optional, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean, java.lang.String)
	 */
	@Override
	public <T extends IAnalyticsAccessContext<?>> Optional<T> getServiceOutput(
			final Class<T> clazz, 
			final Optional<DataBucketBean> bucket,
			final AnalyticThreadJobBean job, 
			final String data_service)
	{
		final Optional<String> job_config = Optional.of(BeanTemplateUtils.toJson(job).toString());
		if ("storage_service".equalsIgnoreCase(data_service)) {
			return _storage_service.getUnderlyingPlatformDriver(clazz, job_config);
		}
		else if ("search_index_service".equalsIgnoreCase(data_service)) {			
			return _storage_service.getUnderlyingPlatformDriver(clazz, job_config);
		}
		else { // (currently no other  
			return Optional.empty();
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext#emitObject(java.util.Optional, com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean, fj.data.Either, java.util.Optional)
	 */
	@Override
	public void emitObject(final Optional<DataBucketBean> bucket,
			final AnalyticThreadJobBean job,
			final Either<JsonNode, Map<String, Object>> object, 
			final Optional<AnnotationBean> annotations)
	{
		final DataBucketBean this_bucket = bucket.orElseGet(() -> _mutable_state.bucket.get()); 
		
		if (annotations.isPresent()) {
			throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);			
		}
		final JsonNode obj_json =  object.either(__->__, map -> (JsonNode) _mapper.convertValue(map, JsonNode.class));
		
		if (_batch_index_service.isPresent()) {
			_batch_index_service.get().storeObject(obj_json);
		}
		else if (_crud_index_service.isPresent()){ // (super slow)
			_crud_index_service.get().storeObject(obj_json);
		}
		if (_batch_storage_service.isPresent()) {
			_batch_storage_service.get().storeObject(obj_json);
		}
		else if (_crud_storage_service.isPresent()){ // (super slow)
			_crud_storage_service.get().storeObject(obj_json);
		}
		
		final String topic = _distributed_services.generateTopicName(this_bucket.full_name(), ICoreDistributedServices.QUEUE_END_NAME);
		if (_distributed_services.doesTopicExist(topic)) {
			// (ie someone is listening in on our output data, so duplicate it for their benefit)
			_distributed_services.produce(topic, obj_json.toString());
		}
		//(else nothing to do)
	}

	/** Util to check if we're using a ping pong buffer to change over data "atomically"
	 * @param job
	 * @return
	 */
	protected static boolean needPingPongBuffer(final AnalyticThreadJobBean job) {
		final boolean need_ping_pong_buffer = 
				!Optionals.of(() -> job.output().preserve_existing_data()).orElse(false)
				&&
				_batch_types.contains(Optional.ofNullable(job.analytic_type()).orElse(MasterEnrichmentType.none))
			;
		return need_ping_pong_buffer;
	}
	
}
