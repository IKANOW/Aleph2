/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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

package com.ikanow.aleph2.analytics.services;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Multimap;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_import.AnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean.StateDirectoryType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

import fj.Unit;
import fj.data.Either;
import fj.data.Validation;

/**
 * @author Alex
 *
 */
public class DeduplicationEnrichmentContext implements IEnrichmentModuleContext {
	protected final IEnrichmentModuleContext _delegate;

	//////////////////////////////////////////////////////////////////////

	// INITIALIZATION
	
	protected final DataSchemaBean.DocumentSchemaBean _config; 
	
	protected static class MutableState {
		Multimap<JsonNode, JsonNode> _id_duplicate_map;
		JsonNode _grouping_key;
		final HashSet<JsonNode> _emitted_id_map = new HashSet<>();
		//TODO: list of things to delete
	}
	protected final MutableState _mutable_state = new MutableState();
	
	/** User c'tor
	 * @param delegate
	 * @param id_duplicate_map
	 * @param grouping_key
	 * @param config
	 */
	public DeduplicationEnrichmentContext(
			final IEnrichmentModuleContext delegate,
			final DataSchemaBean.DocumentSchemaBean config
			)
	{
		_delegate = delegate;
		_config = config;
	}
	
	public void resetMutableState(
			final Multimap<JsonNode, JsonNode> id_duplicate_map,
			final JsonNode grouping_key
			)
	{
		_mutable_state._id_duplicate_map = id_duplicate_map;
		_mutable_state._grouping_key = grouping_key;
		_mutable_state._emitted_id_map.clear();
	}
	
	//////////////////////////////////////////////////////////////////////

	// EMIT LOGIC
	
	// Options:
	// - if in "strict" mode (default) then only allow emitting an object without a matching _id if it has a different grouping key
	//   (ensures don't generate more duplicates)
	// - if in "very strict" mode, then only allow one emission per call
	// Separately:
	// - if "custom_delete_unhandled_duplicates" is true (false by default in "strict" mode, true by default in "very_strict" mode) then
	//   will issue deletes to any objects that aren't emitted by the user
	
	//TODO: if emit a null object then simply stick it on the list of things to delete...
	
	protected Validation<BasicMessageBean, Unit> checkObjectLogic(final JsonNode to_emit) {
		//TODO
		return Validation.success(Unit.unit());
	}
	
	@Override
	public Validation<BasicMessageBean, JsonNode> emitMutableObject(long id,
			ObjectNode mutated_json, Optional<AnnotationBean> annotations,
			Optional<JsonNode> grouping_key) {
		return _delegate.emitMutableObject(id, mutated_json, annotations, grouping_key);
	}

	@Override
	public Validation<BasicMessageBean, JsonNode> emitImmutableObject(long id,
			JsonNode original_json, Optional<ObjectNode> mutations,
			Optional<AnnotationBean> annotations,
			Optional<JsonNode> grouping_key) {
		return _delegate.emitImmutableObject(id, original_json, mutations, annotations, grouping_key);
	}

	// (EXTERNAL EMIT HAS NO RESTRICTIONS)
	
	@Override
	public Validation<BasicMessageBean, JsonNode> externalEmit(
			DataBucketBean bucket,
			Either<JsonNode, Map<String, Object>> object,
			Optional<AnnotationBean> annotations) {
		return _delegate.externalEmit(bucket, object, annotations);
	}

	//////////////////////////////////////////////////////////////////////
	
	// FINALIZATION

	public Stream<JsonNode> getObjectsToDelete() {
		//TODO
		return Stream.empty();
	}
	
	//////////////////////////////////////////////////////////////////////
	
	// LEAVE ALL THESE UNCHANGED
	
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return _delegate.getUnderlyingArtefacts();
	}

	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		return _delegate.getUnderlyingPlatformDriver(driver_class,
				driver_options);
	}

	@Override
	public String getEnrichmentContextSignature(
			Optional<DataBucketBean> bucket,
			Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services) {
		return _delegate.getEnrichmentContextSignature(bucket, services);
	}

	@Override
	public <T> Collection<Tuple2<T, String>> getTopologyEntryPoints(
			Class<T> clazz, Optional<DataBucketBean> bucket) {
		return _delegate.getTopologyEntryPoints(clazz, bucket);
	}

	@Override
	public <T> T getTopologyStorageEndpoint(Class<T> clazz,
			Optional<DataBucketBean> bucket) {
		return _delegate.getTopologyStorageEndpoint(clazz, bucket);
	}

	@Override
	public <T> T getTopologyErrorEndpoint(Class<T> clazz,
			Optional<DataBucketBean> bucket) {
		return _delegate.getTopologyErrorEndpoint(clazz, bucket);
	}

	@Override
	public long getNextUnusedId() {
		return _delegate.getNextUnusedId();
	}

	@Override
	public ObjectNode convertToMutable(JsonNode original) {
		return _delegate.convertToMutable(original);
	}

	@Override
	public void storeErroredObject(long id, JsonNode original_json) {
		_delegate.storeErroredObject(id, original_json);
	}

	@Override
	public CompletableFuture<?> flushBatchOutput(Optional<DataBucketBean> bucket) {
		return _delegate.flushBatchOutput(bucket);
	}

	@Override
	public IServiceContext getServiceContext() {
		return _delegate.getServiceContext();
	}

	@Override
	public <S> Optional<ICrudService<S>> getGlobalEnrichmentModuleObjectStore(
			Class<S> clazz, Optional<String> collection) {
		return _delegate.getGlobalEnrichmentModuleObjectStore(clazz,
				collection);
	}

	@Override
	public <S> ICrudService<S> getBucketObjectStore(Class<S> clazz,
			Optional<DataBucketBean> bucket, Optional<String> collection,
			Optional<StateDirectoryType> type) {
		return _delegate
				.getBucketObjectStore(clazz, bucket, collection, type);
	}

	@Override
	public Optional<DataBucketBean> getBucket() {
		return _delegate.getBucket();
	}

	@Override
	public Optional<SharedLibraryBean> getModuleConfig() {
		return _delegate.getModuleConfig();
	}

	@Override
	public Future<DataBucketStatusBean> getBucketStatus(
			Optional<DataBucketBean> bucket) {
		return _delegate.getBucketStatus(bucket);
	}

	@Override
	public void logStatusForBucketOwner(Optional<DataBucketBean> bucket,
			BasicMessageBean message, boolean roll_up_duplicates) {
		_delegate
				.logStatusForBucketOwner(bucket, message, roll_up_duplicates);
	}

	@Override
	public void logStatusForBucketOwner(Optional<DataBucketBean> bucket,
			BasicMessageBean message) {
		_delegate.logStatusForBucketOwner(bucket, message);
	}

	@Override
	public void emergencyDisableBucket(Optional<DataBucketBean> bucket) {
		_delegate.emergencyDisableBucket(bucket);
	}

	@Override
	public void emergencyQuarantineBucket(Optional<DataBucketBean> bucket,
			String quarantine_duration) {
		_delegate.emergencyQuarantineBucket(bucket, quarantine_duration);
	}

	@Override
	public void initializeNewContext(String signature) {
		_delegate.initializeNewContext(signature);
	}
}
