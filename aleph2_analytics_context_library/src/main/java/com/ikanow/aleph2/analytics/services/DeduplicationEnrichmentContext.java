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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.analytics.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_import.AnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.DocumentSchemaBean.CustomPolicy;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean.StateDirectoryType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

import fj.data.Either;
import fj.data.Validation;

/** A wrapper for an enrichment context called as part of a merge between new and existing objects
 * @author Alex
 *
 */
public class DeduplicationEnrichmentContext implements IEnrichmentModuleContext {
	protected final IEnrichmentModuleContext _delegate;

	//////////////////////////////////////////////////////////////////////

	// INITIALIZATION
	
	protected final DataSchemaBean.DocumentSchemaBean _config; 
	
	protected static class MutableState {
		Set<JsonNode> _id_set;
		JsonNode _grouping_key;
		HashSet<JsonNode> _emitted_id_map = new HashSet<>();
		LinkedList<JsonNode> _manual_ids_to_delete = new LinkedList<>();
	}
	protected final MutableState _mutable_state = new MutableState();
	protected final CustomPolicy _custom_policy;
	protected final boolean _delete_unhandled_duplicates;
	protected final Function<JsonNode,  Optional<JsonNode>> _json_to_grouping_key;
	
	protected static final BasicMessageBean _ERROR_BEAN = 
			ErrorUtils.buildErrorMessage
				(DeduplicationEnrichmentContext.class.getSimpleName(), "checkObjectLogic", ErrorUtils.BREAK_DEDUPLICATION_POLICY);
	
	/** User c'tor
	 * @param delegate
	 * @param id_duplicate_map
	 * @param grouping_key
	 * @param config
	 */
	public DeduplicationEnrichmentContext(
			final IEnrichmentModuleContext delegate,
			final DataSchemaBean.DocumentSchemaBean config,
			final Function<JsonNode, Optional<JsonNode>> json_to_grouping_key
			)
	{
		_delegate = delegate;
		_config = config;
		_custom_policy = Optional.ofNullable(_config.custom_policy()).orElse(CustomPolicy.strict);
		_delete_unhandled_duplicates = Optional.ofNullable(_config.delete_unhandled_duplicates()).orElse(false);
		_json_to_grouping_key = json_to_grouping_key;
	}
	
	/** Called for every new/existing merge
	 * @param id_duplicate_map
	 * @param grouping_key
	 */
	public void resetMutableState(
			final Collection<JsonNode> dups,
			final JsonNode grouping_key
			)
	{
		_mutable_state._id_set = dups.stream().map(j -> j.get("_id")).filter(id -> null != id).collect(Collectors.toSet());
		_mutable_state._grouping_key = grouping_key;
		_mutable_state._emitted_id_map = new HashSet<JsonNode>();		
		_mutable_state._manual_ids_to_delete = new LinkedList<JsonNode>();
	}
	
	//////////////////////////////////////////////////////////////////////

	// EMIT LOGIC
	
	/** Will error out based on the mode
	// Options:
	// - if in "strict" mode (default) then only allow emitting an object without a matching _id if it has a different grouping key
	//   (ensures don't generate more duplicates)
	// - if in "very strict" mode, then only allow one emission per call
	// Separately:
	// - if "custom_delete_unhandled_duplicates" is true (false by default in "strict" mode, true by default in "very_strict" mode) then
	//   will issue deletes to any objects that aren't emitted by the user	
	 * @param to_emit
	 * @return
	 */
	protected Validation<BasicMessageBean, JsonNode> checkObjectLogic(final JsonNode to_emit) {		
		if (CustomPolicy.strict == _custom_policy) {
			final JsonNode _id = to_emit.get("_id");
			if (!_mutable_state._id_set.contains(_id)) { // (even if null) if "an object without a matching _id"
				if (_json_to_grouping_key.apply(to_emit).map(j -> j.equals(_mutable_state._grouping_key)).orElse(false)) { // only .. if it has a different grouping key
					return Validation.fail(_ERROR_BEAN);					
				}
			}
		}
		else if (CustomPolicy.very_strict == _custom_policy) {
			if (!_mutable_state._emitted_id_map.isEmpty()) {
				return Validation.fail(_ERROR_BEAN);
			}
		}
		return Validation.success(to_emit);			
	}
	
	@Override
	public Validation<BasicMessageBean, JsonNode> emitMutableObject(long id,
			ObjectNode mutated_json, Optional<AnnotationBean> annotations,
			Optional<JsonNode> grouping_key) {	
		return checkObjectLogic(mutated_json).bind(j -> {
			final JsonNode _id = (1 == mutated_json.size()) ? mutated_json.get("_id") : null; 		
			if (null == _id) {
				_mutable_state._manual_ids_to_delete.add(_id);
				return Validation.success(mutated_json);
			}
			else {
				if (_delete_unhandled_duplicates) _mutable_state._id_set.remove(_id);
				return _delegate.emitMutableObject(id, mutated_json, annotations, grouping_key);
			}
		});
	}

	@Override
	public Validation<BasicMessageBean, JsonNode> emitImmutableObject(long id,
			JsonNode original_json, Optional<ObjectNode> mutations,
			Optional<AnnotationBean> annotations,
			Optional<JsonNode> grouping_key) {
		return checkObjectLogic(original_json).bind(j -> {
			final JsonNode _id = (1 == original_json.size()) ? original_json.get("_id") : null; 		
			if (null == _id) {
				_mutable_state._manual_ids_to_delete.add(_id);
				return Validation.success(original_json);
			}
			else {
				if (_delete_unhandled_duplicates) _mutable_state._id_set.remove(_id);
				return _delegate.emitImmutableObject(id, original_json, mutations, annotations, grouping_key);
			}
		});
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

	/** A stream of object ids that should be deleted
	 * @return
	 */
	public Stream<JsonNode> getObjectIdsToDelete() {
		
		// Get a list of _ids that haven't been emitted
		final Collection<JsonNode> auto_delete = _delete_unhandled_duplicates
													? _mutable_state._id_set
													: Collections.emptyList();
		
		return Stream.concat(
				_mutable_state._manual_ids_to_delete.stream()
				,
				auto_delete.stream()				
				);
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
