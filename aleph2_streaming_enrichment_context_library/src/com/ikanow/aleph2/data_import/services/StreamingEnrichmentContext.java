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
package com.ikanow.aleph2.data_import.services;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import scala.Tuple2;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.spout.SchemeAsMultiScheme;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import.context.stream_enrichment.utils.ErrorUtils;
import com.ikanow.aleph2.data_import.stream_enrichment.storm.OutputBolt;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_import.AnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.PropertiesUtils;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.distributed_services.utils.KafkaUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;

/** The implementation of an enrichment context, specifically designed for enrichment
 * @author Alex
 */
public class StreamingEnrichmentContext implements IEnrichmentModuleContext {

	////////////////////////////////////////////////////////////////
	
	// CONSTRUCTION
	
	public static final String __MY_ID = "3fdb4bfa-2024-11e5-b5f7-727283247c7f";	
	
	protected static class MutableState {
		//TODO (ALEPH-10) logging information - will be genuinely mutable
		SetOnce<DataBucketBean> bucket = new SetOnce<DataBucketBean>();
		SetOnce<String> user_topology_entry_point = new SetOnce<String>();
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
	protected GlobalPropertiesBean _globals;

	// For writing objects out
	protected Optional<ICrudService<JsonNode>> _crud_index_service;
	protected Optional<ICrudService.IBatchSubservice<JsonNode>> _batch_index_service;
	
	
	/**Guice injector
	 * @param service_context
	 */
	@Inject 
	public StreamingEnrichmentContext(final IServiceContext service_context) {
		_state_name = State.IN_TECHNOLOGY;
		_service_context = service_context;
		_core_management_db = service_context.getCoreManagementDbService(); // (actually returns the _core_ management db service)
		_distributed_services = service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
		_index_service = service_context.getService(ISearchIndexService.class, Optional.empty()).get();
		_globals = service_context.getGlobalProperties();
	}

	/** In-module constructor
	 */
	public StreamingEnrichmentContext() {
		_state_name = State.IN_MODULE;
		
		// Can't do anything until initializeNewContext is called
	}	
	
	/** (FOR INTERNAL DATA MANAGER USE ONLY) Sets the bucket for this harvest context instance
	 * @param this_bucket - the bucket to associate
	 * @returns whether the bucket has been updated (ie fails if it's already been set)
	 */
	public boolean setBucket(DataBucketBean this_bucket) {
		return _mutable_state.bucket.set(this_bucket);
	}
	
	/** (FOR INTERNAL DATA MANAGER USE ONLY) Sets the user topology entry point for this harvest context instance
	 * @param this_bucket - the user entry point to associate
	 * @returns whether the user entry point has been updated (ie fails if it's already been set)
	 */
	public boolean setUserTopologyEntryPoint(final String entry_point) {
		return _mutable_state.user_topology_entry_point.set(entry_point);
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#initializeNewContext(java.lang.String)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void initializeNewContext(final String signature) {
		try {
			// Inject dependencies
			
			final Config parsed_config = ConfigFactory.parseString(signature);
			
			final Injector injector = ModuleUtils.createInjector(Collections.emptyList(), Optional.of(parsed_config));
			injector.injectMembers(this);			
			_core_management_db = _service_context.getCoreManagementDbService(); // (actually returns the _core_ management db service)
			_distributed_services = _service_context.getService(ICoreDistributedServices.class, Optional.empty()).get();
			_index_service = _service_context.getService(ISearchIndexService.class, Optional.empty()).get();
			_globals = _service_context.getGlobalProperties();
			
			// Get bucket 
			
			String bucket_id = parsed_config.getString(__MY_ID);
			
			Optional<DataBucketBean> retrieve_bucket = _core_management_db.getDataBucketStore().getObjectById(bucket_id).get();
			if (!retrieve_bucket.isPresent()) {
				throw new RuntimeException("Unable to locate bucket: " + bucket_id);
			}
			_batch_index_service = (_crud_index_service = _index_service.getCrudService(JsonNode.class, retrieve_bucket.get()))
											.flatMap(cs -> cs.getUnderlyingPlatformDriver(ICrudService.IBatchSubservice.class, Optional.empty()))
											.map(x -> (ICrudService.IBatchSubservice<JsonNode>) x);
			_mutable_state.bucket.set(retrieve_bucket.get());
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
	public String getEnrichmentContextSignature(final Optional<DataBucketBean> bucket, final Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services) {
		if (_state_name == State.IN_TECHNOLOGY) {
			// Returns a config object containing:
			// - set up for any of the services described
			// - all the rest of the configuration
			// - the bucket bean ID
			
			final Config full_config = ModuleUtils.getStaticConfig();
	
			final Optional<Config> service_config = PropertiesUtils.getSubConfig(full_config, "service");
			
			final ImmutableSet<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>> complete_services_set = 
					ImmutableSet.<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>builder()
							.addAll(services.orElse(Collections.emptySet()))
							.add(Tuples._2T(ICoreDistributedServices.class, Optional.empty()))
							.add(Tuples._2T(IManagementDbService.class, Optional.empty()))
							.add(Tuples._2T(ISearchIndexService.class, Optional.empty()))
							.add(Tuples._2T(IStorageService.class, Optional.empty()))
							.add(Tuples._2T(IManagementDbService.class, Optional.of("CoreManagementDbService")))
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
			
			final Config last_call = config_subset_services
								.withValue(__MY_ID, ConfigValueFactory
										.fromAnyRef(bucket.orElseGet(() -> _mutable_state.bucket.get())._id(), "bucket id"));
			
			final String ret = this.getClass().getName() + ":" + last_call.root().render(ConfigRenderOptions.concise());
			_mutable_state.signature_override.set(ret);

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
	
	@SuppressWarnings("unchecked")
	@Override
	public <T> T getTopologyEntryPoint(final Class<T> clazz, final Optional<DataBucketBean> bucket) {
		if (_state_name == State.IN_TECHNOLOGY) {
			
			final DataBucketBean my_bucket = bucket.orElseGet(() -> _mutable_state.bucket.get());
			final BrokerHosts hosts = new ZkHosts(KafkaUtils.getZookeperConnectionString());
			final String full_path = (_globals.distributed_root_dir() + GlobalPropertiesBean.BUCKET_DATA_ROOT_OFFSET + my_bucket.full_name()).replace("//", "/");
			final SpoutConfig spout_config = new SpoutConfig(hosts, my_bucket.full_name(), full_path, my_bucket._id()); 
			spout_config.scheme = new SchemeAsMultiScheme(new StringScheme());
			final KafkaSpout kafka_spout = new KafkaSpout(spout_config);
			return (T) kafka_spout;			
		}
		else {
			throw new RuntimeException(ErrorUtils.TECHNOLOGY_NOT_MODULE);						
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyStorageEndpoint(java.lang.Class, java.util.Optional)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> T getTopologyStorageEndpoint(final Class<T> clazz, final Optional<DataBucketBean> bucket) {
		if (_state_name == State.IN_TECHNOLOGY) {
			if (!_mutable_state.user_topology_entry_point.isSet()) {
				throw new RuntimeException(ErrorUtils.get(ErrorUtils.USER_TOPOLOGY_NOT_SET, "getTopologyStorageEndpoint"));
			}
			if (!_mutable_state.signature_override.isSet()) {
				// Assume the user is happy with defaults:
				getEnrichmentContextSignature(bucket, Optional.empty());
			}
			final DataBucketBean my_bucket = bucket.orElseGet(() -> _mutable_state.bucket.get());
			return (T) new OutputBolt(my_bucket, _mutable_state.signature_override.get(), _mutable_state.user_topology_entry_point.get());
		}
		else {
			throw new RuntimeException(ErrorUtils.TECHNOLOGY_NOT_MODULE);						
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyErrorEndpoint(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> T getTopologyErrorEndpoint(final Class<T> clazz, final Optional<DataBucketBean> bucket) {
		if (_state_name == State.IN_TECHNOLOGY) {
			throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);
		}
		else {
			throw new RuntimeException(ErrorUtils.TECHNOLOGY_NOT_MODULE);						
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#convertToMutable(com.fasterxml.jackson.databind.JsonNode)
	 */
	@Override
	public ObjectNode convertToMutable(final JsonNode original) {
		return (ObjectNode) original;
	}

	@Override
	public void emitMutableObject(final long id, final ObjectNode mutated_json, final Optional<AnnotationBean> annotation) {
		if (annotation.isPresent()) {
			throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);			
		}
		if (_batch_index_service.isPresent()) {
			_batch_index_service.get().storeObject(mutated_json, false);
		}
		else if (_crud_index_service.isPresent()){ // (super slow)
			_crud_index_service.get().storeObject(mutated_json, false);
		}
		//(else nothing to do)
	}

	@Override
	public void emitImmutableObject(final long id, final JsonNode original_json, final Optional<ObjectNode> mutations, final Optional<AnnotationBean> annotations)
	{
		if (annotations.isPresent()) {
			throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);			
		}
		final JsonNode to_emit = 
				mutations.map(o -> StreamSupport.<Map.Entry<String, JsonNode>>stream(Spliterators.spliteratorUnknownSize(o.fields(), Spliterator.ORDERED), false)
									.reduce(original_json, (acc, kv) -> ((ObjectNode) acc).set(kv.getKey(), kv.getValue()), (acc1, acc2) -> acc1))
									.orElse(original_json);
		
		emitMutableObject(0L, (ObjectNode)to_emit, annotations);
	}

	@Override
	public void storeErroredObject(final long id, final JsonNode original_json) {
		throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);
	}

	@Override
	public long getNextUnusedId() {
		throw new RuntimeException(ErrorUtils.NOT_SUPPORTED_IN_STREAMING_ENRICHMENT);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getService(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <I extends IUnderlyingService> Optional<I> getService(final Class<I> service_clazz, final Optional<String> service_name) {
		return _service_context.getService(service_clazz, service_name);
	}

	@Override
	public <S> ICrudService<S> getBucketObjectStore(final Class<S> clazz, final Optional<DataBucketBean> bucket, final Optional<String> sub_collection, final boolean auto_apply_prefix) {
		throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);
	}

	@Override
	public Future<DataBucketStatusBean> getBucketStatus(final Optional<DataBucketBean> bucket) {
		throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);
	}

	@Override
	public void logStatusForBucketOwner(final Optional<DataBucketBean> bucket, final BasicMessageBean message, final boolean roll_up_duplicates) {
		throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);
	}

	@Override
	public void logStatusForBucketOwner(final Optional<DataBucketBean> bucket, final BasicMessageBean message) {
		throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);
	}

	@Override
	public void emergencyDisableBucket(final Optional<DataBucketBean> bucket) {
		throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);
	}

	@Override
	public void emergencyQuarantineBucket(final Optional<DataBucketBean> bucket, final String quarantine_duration) {
		throw new RuntimeException(ErrorUtils.NOT_YET_IMPLEMENTED);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class, Optional<String> driver_options) {
		return Optional.empty();
	}

}
