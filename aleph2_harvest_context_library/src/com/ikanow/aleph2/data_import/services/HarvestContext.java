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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Collector;





import java.util.stream.StreamSupport;

import org.checkerframework.checker.nullness.qual.NonNull;







import scala.Tuple2;






import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_import.utils.LiveInjector;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.MultiQueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.PropertiesUtils;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;

public class HarvestContext implements IHarvestContext {

	public static final String __MY_ID = "030e2b82-0285-11e5-a322-1697f925ec7b";
	
	public enum State { IN_TECHNOLOGY, IN_MODULE };
	protected final State _state_name;
	
	protected static class MutableState {
		//TODO (ALEPH-19) logging information - will be genuinely mutable
		SetOnce<DataBucketBean> bucket = new SetOnce<DataBucketBean>();
	};
	protected final MutableState _mutable_state = new MutableState(); 
	
	// (stick this injection in and then call injectMembers in IN_MODULE case)
	@Inject protected IServiceContext _service_context;	
	protected IManagementDbService _core_management_db;
	protected ICoreDistributedServices _distributed_services; 	
	protected GlobalPropertiesBean _globals;
	
	/**Guice injector
	 * @param service_context
	 */
	@Inject 
	public HarvestContext(final IServiceContext service_context) {
		_state_name = State.IN_TECHNOLOGY;
		_service_context = service_context;
		_core_management_db = service_context.getCoreManagementDbService(); // (actually returns the _core_ management db service)
		_distributed_services = service_context.getService(ICoreDistributedServices.class, Optional.empty());
		_globals = service_context.getGlobalProperties();
	}

	/**
	 * 
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
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#initializeNewContext(java.lang.String)
	 */
	@Override
	public void initializeNewContext(final @NonNull String signature) {
		try {
			// Inject dependencies
			
			final Config parsed_config = ConfigFactory.parseString(signature);
			
			final Injector injector = ModuleUtils.createInjector(Collections.emptyList(), Optional.of(parsed_config));
			injector.injectMembers(this);			
			_core_management_db = _service_context.getCoreManagementDbService(); // (actually returns the _core_ management db service)
			_distributed_services = _service_context.getService(ICoreDistributedServices.class, Optional.empty());
			_globals = _service_context.getGlobalProperties();
			
			// Get bucket 
			
			String bucket_id = parsed_config.getString(__MY_ID);
			
			Optional<DataBucketBean> retrieve_bucket = _core_management_db.getDataBucketStore().getObjectById(bucket_id).get();
			if (!retrieve_bucket.isPresent()) {
				throw new RuntimeException("Unable to locate bucket: " + bucket_id);
			}
			_mutable_state.bucket.set(retrieve_bucket.get());
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getService(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <I> @NonNull Optional<I> getService(@NonNull Class<I> service_clazz,
			@NonNull Optional<String> service_name) {
		return Optional.ofNullable(_service_context.getService(service_clazz, service_name));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#sendObjectToStreamingPipeline(java.util.Optional, com.fasterxml.jackson.databind.JsonNode)
	 */
	@Override
	public void sendObjectToStreamingPipeline(
			@NonNull Optional<DataBucketBean> bucket, @NonNull JsonNode object) {
		//TODO (ALEPH-19): Fill this in later
		throw new RuntimeException("This operation is not currently supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#sendObjectToStreamingPipeline(java.util.Optional, java.lang.Object)
	 */
	@Override
	public <T> void sendObjectToStreamingPipeline(
			@NonNull Optional<DataBucketBean> bucket, @NonNull T object) {
		//TODO (ALEPH-19): Fill this in later
		throw new RuntimeException("This operation is not currently supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#sendObjectToStreamingPipeline(java.util.Optional, java.util.Map)
	 */
	@Override
	public void sendObjectToStreamingPipeline(
			@NonNull Optional<DataBucketBean> bucket,
			@NonNull Map<String, Object> object) {
		//TODO (ALEPH-19): Fill this in later
		throw new RuntimeException("This operation is not currently supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getHarvestContextLibraries(java.util.Optional)
	 */
	@Override
	public @NonNull List<String> getHarvestContextLibraries(final @NonNull Optional<Set<Tuple2<Class<?>, Optional<String>>>> services) {
		// Consists of:
		// 1) This library + data model 
		// 2) Libraries that are always needed:
		//    - core distributed services (implicit)
		//    - management db (core + underlying?)
		//    - data model (implicit)
		// 3) Any libraries associated with the services		
		
		if (_state_name == State.IN_TECHNOLOGY) {
			// This very JAR			
			final String this_jar = Lambdas.exec(() -> {
				return LiveInjector.findPathJar(this.getClass(), _globals.local_root_dir() + "/lib/aleph2_harvest_context_library.jar");	
			});
			
			// Libraries associated with services:
			final Set<String> user_service_class_files = services.map(set -> {
				return set.stream()
						.map(clazz_name -> _service_context.getService(clazz_name._1(), clazz_name._2()))
						.filter(service -> service != null)
						.map(service -> LiveInjector.findPathJar(service.getClass(), ""))
						.filter(jar_path -> jar_path != null && !jar_path.isEmpty())
						.collect(Collectors.toSet());
			})
			.orElse(Collections.emptySet());
			
			// Mandatory services
			final Set<String> mandatory_service_class_files = 
					Arrays.asList(_service_context, //(ie something in the data model)
									_service_context.getCoreManagementDbService(), 
									_service_context.getService(IManagementDbService.class, Optional.empty())
							).stream()
							.map(service -> LiveInjector.findPathJar(service.getClass(), ""))
						.filter(jar_path -> jar_path != null && !jar_path.isEmpty())
							.collect(Collectors.toSet());
			
			// Combine them together
			return ImmutableSet.<String>builder()
							.add(this_jar)
							.addAll(user_service_class_files)
							.addAll(mandatory_service_class_files)
							.build()
							.asList();
		}
		else {
			throw new RuntimeException("Can only be called from technology, not module");
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getHarvestContextSignature(java.util.Optional)
	 */
	@Override
	public @NonNull String getHarvestContextSignature(final @NonNull Optional<DataBucketBean> bucket, 
			final @NonNull Optional<Set<Tuple2<Class<?>, Optional<String>>>> services)
	{		
		if (_state_name == State.IN_TECHNOLOGY) {
			// Returns a config object containing:
			// - set up for any of the services described
			// - all the rest of the configuration
			// - the bucket bean ID
			
			final Config full_config = ModuleUtils.getStaticConfig();
	
			final Optional<Config> service_config = PropertiesUtils.getSubConfig(full_config, "service");
			
			ImmutableSet<Tuple2<Class<?>, Optional<String>>> complete_services_set = 
					ImmutableSet.<Tuple2<Class<?>, Optional<String>>>builder()
							.addAll(services.orElse(Collections.emptySet()))
							.add(Tuples._2T(ICoreDistributedServices.class, Optional.empty()))
							.add(Tuples._2T(IManagementDbService.class, Optional.empty()))
							.add(Tuples._2T(IManagementDbService.class, Optional.of("CoreManagementDbService")))
							.build();
			
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
			
			Config last_call = config_subset_services
								.withValue(__MY_ID, ConfigValueFactory
										.fromAnyRef(bucket.orElseGet(() -> _mutable_state.bucket.get())._id(), "bucket id"));
			
			return this.getClass().getName() + ":" + last_call.root().render(ConfigRenderOptions.concise());
		}
		else {
			throw new RuntimeException("Can only be called from technology, not module");			
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getGlobalHarvestTechnologyObjectStore()
	 */
	@Override
	@NonNull 
	public <S> ICrudService<S> getGlobalHarvestTechnologyObjectStore(final @NonNull Class<S> clazz, final @NonNull Optional<DataBucketBean> bucket)
	{
		//TODO (ALEPH-19): Fill this in later
		throw new RuntimeException("This operation is not currently supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getHarvestLibraries(java.util.Optional)
	 */
	@Override
	public @NonNull CompletableFuture<Map<String, String>> getHarvestLibraries(
			final @NonNull Optional<DataBucketBean> bucket) {
		if (_state_name == State.IN_TECHNOLOGY) {
			
			final DataBucketBean my_bucket = bucket.orElseGet(() -> _mutable_state.bucket.get());
			
			final SingleQueryComponent<SharedLibraryBean> tech_query = 
					CrudUtils.anyOf(SharedLibraryBean.class)
						.when(SharedLibraryBean::_id, my_bucket.harvest_technology_name_or_id())
						.when(SharedLibraryBean::path_name, my_bucket.harvest_technology_name_or_id());
			
			final List<SingleQueryComponent<SharedLibraryBean>> other_libs = 
				Optionals.ofNullable(my_bucket.harvest_configs()).stream()
					.flatMap(hcfg -> Optionals.ofNullable(hcfg.library_ids_or_names()).stream())
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
			
			return this._core_management_db.getSharedLibraryStore().getObjectsBySpec(spec, Arrays.asList("_id", "path_name"), true)
				.thenApply(cursor -> {
					return StreamSupport.stream(cursor.spliterator(), false)
						.collect(Collectors.<SharedLibraryBean, String, String>toMap(
								lib -> lib.path_name(), 
								lib -> _globals.local_cached_jar_dir() + "/" + lib._id() + ".cache.jar"));
				});
		}
		else {
			throw new RuntimeException("Can only be called from technology, not module");			
		}
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getBucketObjectStore(java.lang.Class, java.util.Optional, java.util.Optional, boolean)
	 */
	@Override
	public <S> @NonNull ICrudService<S> getBucketObjectStore(
			@NonNull Class<S> clazz, @NonNull Optional<DataBucketBean> bucket,
			@NonNull Optional<String> sub_collection, boolean auto_apply_prefix)
	{
		//TODO (ALEPH-19): Fill this in later
		throw new RuntimeException("This operation is not currently supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getBucketStatus(java.util.Optional)
	 */
	@Override
	public @NonNull CompletableFuture<DataBucketStatusBean> getBucketStatus(
			final @NonNull Optional<DataBucketBean> bucket) {
		return this._core_management_db
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
			@NonNull Optional<DataBucketBean> bucket,
			@NonNull BasicMessageBean message, boolean roll_up_duplicates) 
	{
		//TODO (ALEPH-19): Fill this in later
		throw new RuntimeException("This operation is not currently supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#logStatusForBucketOwner(java.util.Optional, com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean)
	 */
	@Override
	public void logStatusForBucketOwner(
			@NonNull Optional<DataBucketBean> bucket,
			@NonNull BasicMessageBean message) {
		logStatusForBucketOwner(bucket, message, true);		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getTempOutputLocation(java.util.Optional)
	 */
	@Override
	public @NonNull String getTempOutputLocation(
			@NonNull Optional<DataBucketBean> bucket) {
		return _globals.distributed_root_dir() + "/" + bucket.orElseGet(() -> _mutable_state.bucket.get()).full_name() + "/managed_bucket/import/temp/";
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getFinalOutputLocation(java.util.Optional)
	 */
	@Override
	public @NonNull String getFinalOutputLocation(
			@NonNull Optional<DataBucketBean> bucket) {
		return _globals.distributed_root_dir() + "/" + bucket.orElseGet(() -> _mutable_state.bucket.get()).full_name() + "/managed_bucket/import/ready/";
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#emergencyDisableBucket(java.util.Optional)
	 */
	@Override
	public void emergencyDisableBucket(@NonNull Optional<DataBucketBean> bucket) {
		//TODO (ALEPH-19): Fill this in later (need distributed Akka working)
		throw new RuntimeException("This operation is not currently supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#emergencyQuarantineBucket(java.util.Optional, java.lang.String)
	 */
	@Override
	public void emergencyQuarantineBucket(
			@NonNull Optional<DataBucketBean> bucket,
			@NonNull String quarantine_duration) {
		//TODO (ALEPH-19): Fill this in later (need distributed Akka working)
		throw new RuntimeException("This operation is not currently supported");
	}

}
