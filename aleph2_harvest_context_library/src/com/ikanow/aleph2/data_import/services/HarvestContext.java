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
package com.ikanow.aleph2.data_import.services;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.core.shared.utils.LiveInjector;
import com.ikanow.aleph2.data_import.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.BeanTemplate;
import com.ikanow.aleph2.data_model.utils.CrudUtils.MultiQueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.PropertiesUtils;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.data_model.DistributedServicesPropertyBean;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.utils.KafkaUtils;
import com.sun.xml.internal.rngom.binary.Pattern;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;

import fj.Unit;
import fj.data.Either;

//TODO: ALEPH-12 wire up module config via signature

@SuppressWarnings("unused")
public class HarvestContext implements IHarvestContext {
	protected static final Logger _logger = LogManager.getLogger();	

	public static final String __MY_BUCKET_ID = "030e2b82-0285-11e5-a322-1697f925ec7b";
	public static final String __MY_TECH_LIBRARY_ID = "030e2b82-0285-11e5-a322-1697f925ec7c";
	public static final String __MY_MODULE_LIBRARY_ID = "030e2b82-0285-11e5-a322-1697f925ec7d";
	
	public enum State { IN_TECHNOLOGY, IN_MODULE };
	protected final State _state_name;
	
	protected static class MutableState {
		final SetOnce<DataBucketBean> bucket = new SetOnce<>();
		final SetOnce<SharedLibraryBean> technology_config = new SetOnce<>();
		SetOnce<Map<String, SharedLibraryBean>> library_configs = new SetOnce<>();
		final SetOnce<ImmutableSet<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> service_manifest_override = new SetOnce<>();
		final SetOnce<Boolean> initialized_direct_output = new SetOnce<>();		
	};
	protected final MutableState _mutable_state = new MutableState(); 
	
	// (stick this injection in and then call injectMembers in IN_MODULE case)
	@Inject protected IServiceContext _service_context;	
	protected IManagementDbService _core_management_db;
	protected ICoreDistributedServices _distributed_services; 	
	protected IStorageService _storage_service;
	protected Optional<ISearchIndexService> _index_service; //(only need this sometimes)
	protected GlobalPropertiesBean _globals;
	
	protected Optional<IDataWriteService<String>> _crud_intermed_storage_service = Optional.empty();
	protected Optional<IDataWriteService.IBatchSubservice<String>> _batch_intermed_storage_service = Optional.empty();	
	
	// For writing objects out
	// TODO (ALEPH-12): this needs to get moved into the object output library
	protected Optional<IDataWriteService<JsonNode>> _crud_index_service = Optional.empty();
	protected Optional<IDataWriteService.IBatchSubservice<JsonNode>> _batch_index_service = Optional.empty();
	protected Optional<IDataWriteService<JsonNode>> _crud_storage_service = Optional.empty();
	protected Optional<IDataWriteService.IBatchSubservice<JsonNode>> _batch_storage_service = Optional.empty();
	
	
	protected final ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());
	
	private static ConcurrentHashMap<String, HarvestContext> static_instances = new ConcurrentHashMap<>();
	
	/**Guice injector
	 * @param service_context
	 */
	@Inject 
	public HarvestContext(final IServiceContext service_context) {
		_state_name = State.IN_TECHNOLOGY;
		_service_context = service_context;
		_core_management_db = service_context.getCoreManagementDbService(); // (actually returns the _core_ management db service)
		_distributed_services = service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		_storage_service = service_context.getStorageService();
		_globals = service_context.getGlobalProperties();
	}

	/** In-module constructor
	 */
	public HarvestContext() {
		_state_name = State.IN_MODULE;
		
		// Can't do anything until initializeNewContext is called
	}	
	
	/** (FOR INTERNAL DATA MANAGER USE ONLY) Sets the bucket for this harvest context instance
	 * @param this_bucket - the bucket to associated
	 * @returns whether the bucket has been updated (ie fails if it's already been set)
	 */
	public boolean setBucket(DataBucketBean this_bucket) {
		return _mutable_state.bucket.set(this_bucket);
	}
	
	/** (FOR INTERNAL DATA MANAGER USE ONLY) Sets the library bean for this harvest context instance
	 * @param this_bucket - the library bean to be associated
	 * @returns whether the library bean has been updated (ie fails if it's already been set)
	 */
	public boolean setTechnologyConfig(SharedLibraryBean lib_config) {
		return _mutable_state.technology_config.set(lib_config);
	}
	
	/** (FOR INTERNAL DATA MANAGER USE ONLY) Sets the optional module library bean for this context instance
	 * @param this_bucket - the library bean to be associated
	 * @returns whether the library bean has been updated (ie fails if it's already been set)
	 */
	public boolean setLibraryConfigs(final Map<String, SharedLibraryBean> lib_configs) {
		return _mutable_state.library_configs.set(lib_configs);
	}	
	
	/** A very simple container for library beans
	 * @author Alex
	 */
	public static class LibraryContainerBean {
		LibraryContainerBean() {}
		LibraryContainerBean(Collection<SharedLibraryBean> libs) { this.libs = new ArrayList<>(libs); }
		List<SharedLibraryBean> libs;
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#initializeNewContext(java.lang.String)
	 */
	@Override
	public void initializeNewContext(final String signature) {
		try {
			// Inject dependencies
			
			final Config parsed_config = ConfigFactory.parseString(signature);
			final HarvestContext to_clone = static_instances.get(signature);			
			if (null != to_clone) { //copy the fields				
				_service_context = to_clone._service_context;
				_core_management_db = to_clone._core_management_db;
				_distributed_services = to_clone._distributed_services;	
				_storage_service = to_clone._storage_service;
				_globals = to_clone._globals;
				// (apart from bucket, which is handled below, rest of mutable state is not needed)
				
				// Only sometimes need this:
				_index_service = to_clone._index_service;
			}
			else {							
				ModuleUtils.initializeApplication(Collections.emptyList(), Optional.of(parsed_config), Either.right(this));
				_core_management_db = _service_context.getCoreManagementDbService(); // (actually returns the _core_ management db service)
				_distributed_services = _service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
				_storage_service = _service_context.getStorageService();
				_globals = _service_context.getGlobalProperties();

				// Only sometimes need this:
				_index_service = _service_context.getService(ISearchIndexService.class, Optional.empty());
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
			
			// Always want intermediate output service:
			_batch_intermed_storage_service = 
					(_crud_intermed_storage_service = _storage_service.getDataService()
												.flatMap(s -> 
															s.getWritableDataService(String.class, retrieve_bucket.get(), 
																Optional.of(IStorageService.StorageStage.json.toString()), Optional.empty()))
					)
					.flatMap(IDataWriteService::getBatchWriteSubservice)
					;			
			
			// Only create final output services for buckets that have no streaming enrichment:
			// (otherwise can still create lazily if emitObject is called)
			if (MasterEnrichmentType.none == Optional.ofNullable(retrieve_bucket.get().master_enrichment_type()).orElse(MasterEnrichmentType.none)) {
				initializeOptionalOutput(Optional.empty());
			}
			
			static_instances.put(signature, this);
		}
		catch (Exception e) {
			//DEBUG
			//System.out.println(ErrorUtils.getLongForm("{0}", e));			

			throw new RuntimeException(e);
		}
	}
	
	/** Sets up the writers for optional output (not normally needed - only if enrichment is disabled)
	 * @param bucket
	 */
	protected void initializeOptionalOutput(final Optional<DataBucketBean> bucket) {
		
		if (_mutable_state.initialized_direct_output.isSet()) {
			return;
		}
		final DataBucketBean my_bucket = bucket.orElseGet(() -> _mutable_state.bucket.get());
		synchronized (this) {
			_mutable_state.initialized_direct_output.trySet(true);
			if (!_batch_index_service.isPresent()) {
				if (hasDirectStorageOutput(my_bucket)) {
					_batch_index_service = 
							(_crud_index_service = _index_service
														.flatMap(s -> s.getDataService())
														.flatMap(s -> s.getWritableDataService(JsonNode.class, my_bucket, Optional.empty(), Optional.empty()))
							)
							.flatMap(IDataWriteService::getBatchWriteSubservice)
							;
				}
			}
			if (!_batch_storage_service.isPresent()) {
				if (hasSearchIndexOutput(my_bucket)) {
					_batch_storage_service = 
							(_crud_storage_service = _storage_service.getDataService()
														.flatMap(s -> 
																	s.getWritableDataService(JsonNode.class, my_bucket, 
																		Optional.of(IStorageService.StorageStage.processed.toString()), Optional.empty()))
							)
							.flatMap(IDataWriteService::getBatchWriteSubservice)
							;								
				}			
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getService(java.lang.Class, java.util.Optional)
	 */
	@Override
	public IServiceContext getServiceContext() {
		return _service_context;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#sendObjectToStreamingPipeline(java.util.Optional, com.fasterxml.jackson.databind.JsonNode)
	 */
	@Override
	public void sendObjectToStreamingPipeline(
			Optional<DataBucketBean> bucket, Either<JsonNode, Map<String, Object>> object) {
		
		final DataBucketBean this_bucket = bucket.orElseGet(() -> _mutable_state.bucket.get()); 

		final boolean streaming_pipeline_disabled = 
				(MasterEnrichmentType.none == Optional.ofNullable(this_bucket.master_enrichment_type()).orElse(MasterEnrichmentType.none));
		
		final String topic = _distributed_services.generateTopicName(this_bucket.full_name(), Optional.empty());
		
		final boolean emit_to_pipeline = Lambdas.get(() -> {
			if  (streaming_pipeline_disabled) {
				this.emitObject(bucket, object);
				return (_distributed_services.doesTopicExist(topic));				
			}
			else return true;
		});
		
		if (emit_to_pipeline) {
			final String obj_str =  object.either(JsonNode::toString, map -> _mapper.convertValue(map, JsonNode.class).toString());
			
			if (_batch_intermed_storage_service.isPresent()) {
				_batch_intermed_storage_service.get().storeObject(obj_str);
			}
			else if (_crud_intermed_storage_service.isPresent()){ // (super slow)
				_crud_intermed_storage_service.get().storeObject(obj_str);
			}				
			_distributed_services.produce(topic, obj_str);
		}
	}

	/** Whether the bucket needs direct output to file
	 * @param bucket
	 * @return
	 */
	public static boolean hasDirectStorageOutput(final DataBucketBean bucket) {
		return (MasterEnrichmentType.none == Optional.ofNullable(bucket.master_enrichment_type()).orElse(MasterEnrichmentType.none))
				&&
				Optionals.of(() -> bucket.data_schema().storage_schema().processed())
						.map(p -> Optional.ofNullable(p.enabled()).orElse(true))
						.orElse(false);
	}
	/** Whether the bucket needs direct output to the search index
	 * @param bucket
	 * @return
	 */
	public static boolean hasSearchIndexOutput(final DataBucketBean bucket) {
		return (MasterEnrichmentType.none == Optional.ofNullable(bucket.master_enrichment_type()).orElse(MasterEnrichmentType.none))
				&&
				Optionals.of(() -> bucket.data_schema().search_index_schema())
						.map(p -> Optional.ofNullable(p.enabled()).orElse(true))
						.orElse(false);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getHarvestContextLibraries(java.util.Optional)
	 */
	@Override
	public List<String> getHarvestContextLibraries(final Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services) {
		
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
								_service_context.getSecurityService().getUnderlyingArtefacts(),
								_service_context.getCoreManagementDbService().getUnderlyingArtefacts() 
								)
							.stream()
							.flatMap(x -> x.stream())
							.map(service -> LiveInjector.findPathJar(service.getClass(), ""))
							.collect(Collectors.toSet());
			
			if (_mutable_state.bucket.isSet()) {
				if (hasSearchIndexOutput(_mutable_state.bucket.get())) {
					_service_context.getSearchIndexService().ifPresent(search_index_service -> {
						mandatory_service_class_files.add(LiveInjector.findPathJar(search_index_service.getClass(), ""));
					});
				}
			}
			
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
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getHarvestContextSignature(java.util.Optional)
	 */
	@Override
	public String getHarvestContextSignature(final Optional<DataBucketBean> bucket, 
			final Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services)
	{		
		if (_state_name == State.IN_TECHNOLOGY) {
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
					Optional.of(
						ImmutableSet.<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>builder()
								.addAll(services.orElse(Collections.emptySet()))
								.add(Tuples._2T(ICoreDistributedServices.class, Optional.empty()))
								.add(Tuples._2T(IManagementDbService.class, Optional.empty()))
								.add(Tuples._2T(IStorageService.class, Optional.empty()))
								.add(Tuples._2T(ISecurityService.class, Optional.empty()))
								.add(Tuples._2T(IManagementDbService.class, IManagementDbService.CORE_MANAGEMENT_DB))
					)
					// Optional services:
					.map(sb -> 
						(hasSearchIndexOutput(bucket.orElseGet(() -> _mutable_state.bucket.get())))
								? 
								sb.add(Tuples._2T(ISearchIndexService.class, Optional.empty()))
									.add(Tuples._2T(ITemporalService.class, Optional.empty()))
									.add(Tuples._2T(IColumnarService.class, Optional.empty()))
								: 
								sb)
					.map(sb -> sb.build()).get();
			
			final Config config_no_services = full_config.withoutPath("service");
			
			if (_mutable_state.service_manifest_override.isSet()) {
				if (!complete_services_set.equals(_mutable_state.service_manifest_override.get())) {
					throw new RuntimeException(ErrorUtils.SERVICE_RESTRICTIONS);
				}
			}
			else {
				_mutable_state.service_manifest_override.set(complete_services_set);
			}			
			
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
					Lambdas.get(() -> 
						_mutable_state.library_configs.isSet()
						?
						config_subset_services
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
						config_subset_services						
					)
					.withValue(__MY_BUCKET_ID, 
								ConfigValueFactory
									.fromAnyRef(BeanTemplateUtils.toJson(bucket.orElseGet(() -> _mutable_state.bucket.get())).toString())
									)
					.withValue(__MY_TECH_LIBRARY_ID, 
								ConfigValueFactory
									.fromAnyRef(BeanTemplateUtils.toJson(_mutable_state.technology_config.get()).toString())
									)
									;
			
			return this.getClass().getName() + ":" + last_call.root().render(ConfigRenderOptions.concise());
		}
		else {
			throw new RuntimeException(ErrorUtils.TECHNOLOGY_NOT_MODULE);			
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getGlobalHarvestTechnologyObjectStore()
	 */
	@Override
	public <S> ICrudService<S> getGlobalHarvestTechnologyObjectStore(final Class<S> clazz, final Optional<String> collection)
	{
		return this.getBucketObjectStore(clazz, Optional.empty(), collection, Optional.of(AssetStateDirectoryBean.StateDirectoryType.library));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IHarvestContext#getLibraryObjectStore(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <S> Optional<ICrudService<S>> getLibraryObjectStore(final Class<S> clazz, final String name_or_id, final Optional<String> collection)
	{
		return Optional.ofNullable(this.getLibraryConfigs().get(name_or_id))
				.map(module_lib -> _core_management_db.getPerLibraryState(clazz, module_lib, collection));
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getHarvestLibraries(java.util.Optional)
	 */
	@Override
	public CompletableFuture<Map<String, String>> getHarvestLibraries(
			final Optional<DataBucketBean> bucket) {
		if (_state_name == State.IN_TECHNOLOGY) {
			
			final DataBucketBean my_bucket = bucket.orElseGet(() -> _mutable_state.bucket.get());
			
			final SingleQueryComponent<SharedLibraryBean> tech_query = 
					CrudUtils.anyOf(SharedLibraryBean.class)
						.when(SharedLibraryBean::_id, my_bucket.harvest_technology_name_or_id())
						.when(SharedLibraryBean::path_name, my_bucket.harvest_technology_name_or_id());
			
			final List<SingleQueryComponent<SharedLibraryBean>> other_libs = 
				Optionals.ofNullable(my_bucket.harvest_configs()).stream()
					.flatMap(hcfg -> Optionals.ofNullable(hcfg.library_names_or_ids()).stream())
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
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getBucketObjectStore(java.lang.Class, java.util.Optional, java.util.Optional, boolean)
	 */
	@Override
	public <S> ICrudService<S> getBucketObjectStore(final Class<S> clazz, final Optional<DataBucketBean> bucket, final Optional<String> collection, final Optional<AssetStateDirectoryBean.StateDirectoryType> type)
	{		
		final Optional<DataBucketBean> this_bucket = 
				bucket
					.map(x -> Optional.of(x))
					.orElseGet(() -> _mutable_state.bucket.isSet() 
										? Optional.of(_mutable_state.bucket.get()) 
										: Optional.empty());
		
		return Patterns.match(type).<ICrudService<S>>andReturn()
				.when(t -> t.isPresent() && AssetStateDirectoryBean.StateDirectoryType.analytic_thread == t.get(), 
						__ -> _core_management_db.getBucketAnalyticThreadState(clazz, this_bucket.get(), collection))
				.when(t -> t.isPresent() && AssetStateDirectoryBean.StateDirectoryType.enrichment == t.get(), 
						__ -> _core_management_db.getBucketEnrichmentState(clazz, this_bucket.get(), collection))
				// assume this is the technology context, most likely usage
				.when(t -> t.isPresent() && AssetStateDirectoryBean.StateDirectoryType.library == t.get(), 
						__ -> _core_management_db.getPerLibraryState(clazz, this.getTechnologyLibraryConfig(), collection))
				// default: harvest or not specified: harvest
				.otherwise(__ -> _core_management_db.getBucketHarvestState(clazz, this_bucket.get(), collection))
				;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getBucket()
	 */
	@Override
	public Optional<DataBucketBean> getBucket() {
		return _mutable_state.bucket.isSet() ? Optional.of(_mutable_state.bucket.get()) : Optional.empty();
	}
	
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getBucketStatus(java.util.Optional)
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
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#logStatusForBucketOwner(java.util.Optional, com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean, boolean)
	 */
	@Override
	public void logStatusForBucketOwner(
			Optional<DataBucketBean> bucket,
			BasicMessageBean message, boolean roll_up_duplicates) 
	{
		//TODO (ALEPH-19): Fill this in later
		throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#logStatusForBucketOwner(java.util.Optional, com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean)
	 */
	@Override
	public void logStatusForBucketOwner(
			Optional<DataBucketBean> bucket,
			BasicMessageBean message) {
		logStatusForBucketOwner(bucket, message, true);		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getTempOutputLocation(java.util.Optional)
	 */
	@Override
	public String getTempOutputLocation(
			Optional<DataBucketBean> bucket) {
		return _globals.distributed_root_dir() + "/" + bucket.orElseGet(() -> _mutable_state.bucket.get()).full_name() + "/managed_bucket/import/temp/";
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getFinalOutputLocation(java.util.Optional)
	 */
	@Override
	public String getFinalOutputLocation(
			Optional<DataBucketBean> bucket) {
		return _globals.distributed_root_dir() + "/" + bucket.orElseGet(() -> _mutable_state.bucket.get()).full_name() + "/managed_bucket/import/ready/";
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#emergencyDisableBucket(java.util.Optional)
	 */
	@Override
	public void emergencyDisableBucket(Optional<DataBucketBean> bucket) {
		//TODO (ALEPH-19): Fill this in later (need distributed Akka working)
		throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#emergencyQuarantineBucket(java.util.Optional, java.lang.String)
	 */
	@Override
	public void emergencyQuarantineBucket(
			Optional<DataBucketBean> bucket,
			String quarantine_duration) {
		//TODO (ALEPH-19): Fill this in later (need distributed Akka working)
		throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getLibraryConfig()
	 */
	@Override
	public SharedLibraryBean getTechnologyLibraryConfig() {
		return _mutable_state.technology_config.get();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_analytics.IHarvestContext#getModuleConfig()
	 */
	@Override
	public Map<String, SharedLibraryBean> getLibraryConfigs() {
		return _mutable_state.library_configs.isSet()
				? _mutable_state.library_configs.get()
				: Collections.emptyMap();
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

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(final Class<T> driver_class, final Optional<String> driver_options) {
		return Optional.empty();
	}

	@Override
	public void emitObject(Optional<DataBucketBean> bucket, Either<JsonNode, Map<String, Object>> object)
	{
		initializeOptionalOutput(bucket);
		
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
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#flushBatchOutput(java.util.Optional)
	 */
	@Override
	public CompletableFuture<?> flushBatchOutput(Optional<DataBucketBean> bucket) {
		
		@SuppressWarnings("unchecked")
		final CompletableFuture<Object> cf1 = 
				_batch_index_service.map(s -> (CompletableFuture<Object>)s.flushOutput())
				.orElseGet(() -> CompletableFuture.completedFuture((Object)Unit.unit()));
		
		@SuppressWarnings("unchecked")
		final CompletableFuture<Object> cf2 = 
				_batch_storage_service.map(s -> (CompletableFuture<Object>)s.flushOutput())
				.orElseGet(() -> CompletableFuture.completedFuture((Object)Unit.unit()));
		
		return CompletableFuture.allOf(cf1, cf2);
	}
}
