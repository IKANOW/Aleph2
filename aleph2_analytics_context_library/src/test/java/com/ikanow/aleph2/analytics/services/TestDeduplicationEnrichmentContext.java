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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.junit.Test;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IBucketLogger;
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
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;

import fj.data.Either;
import fj.data.Validation;

/**
 * @author Alex
 *
 */
public class TestDeduplicationEnrichmentContext {
	protected static final ObjectMapper _mapper = BeanTemplateUtils.configureMapper(Optional.empty());

	@Test
	public void test_laxPolicy_noDelete() {
		final DataSchemaBean.DocumentSchemaBean config =
				BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
					.with(DataSchemaBean.DocumentSchemaBean::delete_unhandled_duplicates, false)
					.with(DataSchemaBean.DocumentSchemaBean::custom_policy, CustomPolicy.lax)
				.done().get();
				
		test_common(config);
	}
	@Test
	public void test_laxPolicy_delete() {
		final DataSchemaBean.DocumentSchemaBean config =
				BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
					.with(DataSchemaBean.DocumentSchemaBean::delete_unhandled_duplicates, true)
					.with(DataSchemaBean.DocumentSchemaBean::custom_policy, CustomPolicy.lax)
				.done().get();
				
		test_common(config);		
	}
	@Test
	public void test_strictPolicy_noDelete() {
		final DataSchemaBean.DocumentSchemaBean config =
				BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
					.with(DataSchemaBean.DocumentSchemaBean::delete_unhandled_duplicates, false)
					.with(DataSchemaBean.DocumentSchemaBean::custom_policy, CustomPolicy.strict)
				.done().get();
				
		test_common(config);		
	}
	@Test
	public void test_strictPolicy_delete() {
		final DataSchemaBean.DocumentSchemaBean config =
				BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
					.with(DataSchemaBean.DocumentSchemaBean::delete_unhandled_duplicates, true)
					.with(DataSchemaBean.DocumentSchemaBean::custom_policy, CustomPolicy.strict)
				.done().get();
				
		test_common(config);		
	}
	@Test
	public void test_veryStrictPolicy_noDelete() {
		final DataSchemaBean.DocumentSchemaBean config =
				BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
					.with(DataSchemaBean.DocumentSchemaBean::delete_unhandled_duplicates, false)
					.with(DataSchemaBean.DocumentSchemaBean::custom_policy, CustomPolicy.very_strict)
				.done().get();
				
		test_common(config);		
	}
	@Test
	public void test_veryStrictPolicy_delete() {
		final DataSchemaBean.DocumentSchemaBean config =
				BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
					.with(DataSchemaBean.DocumentSchemaBean::delete_unhandled_duplicates, true)
					.with(DataSchemaBean.DocumentSchemaBean::custom_policy, CustomPolicy.very_strict)
				.done().get();
				
		test_common(config);				
	}
	
	public void test_common(DataSchemaBean.DocumentSchemaBean config) {
		
		final TestEnrichmentContext delegate = new TestEnrichmentContext();
		
		final DeduplicationEnrichmentContext dedup_context = new DeduplicationEnrichmentContext(delegate, config, j -> Optional.ofNullable(j.get("grouping_key")));
		
		final Collection<JsonNode> dups = 
				Arrays.asList(
						_mapper.createObjectNode().put("_id", "test1").put("grouping_key", "test_group"),
						_mapper.createObjectNode().put("_id", "test2").put("grouping_key", "test_group"),
						_mapper.createObjectNode().put("_id", "test3").put("grouping_key", "test_group")
						);
		
		dedup_context.resetMutableState(dups, _mapper.createObjectNode().put("grouping_key", "test_group"));
		
		JsonNode in1 = _mapper.createObjectNode().put("_id", "test1").put("grouping_key", "test_group");
		final Validation<BasicMessageBean, JsonNode> res1 = dedup_context.emitImmutableObject(0L, in1, Optional.empty(), Optional.empty(), Optional.empty());
		
		JsonNode in2 = _mapper.createObjectNode().put("_id", "test2").put("grouping_key", "test_group");
		final Validation<BasicMessageBean, JsonNode> res2 = dedup_context.emitMutableObject(0L, (ObjectNode) in2, Optional.empty(), Optional.empty());

		JsonNode in3a = _mapper.createObjectNode().put("grouping_key", "test_group");
		JsonNode in3b = _mapper.createObjectNode().put("grouping_key", "test_group_diff");
		JsonNode in4a = _mapper.createObjectNode().put("_id", "test4").put("grouping_key", "test_group");
		JsonNode in4b = _mapper.createObjectNode().put("_id", "test4").put("grouping_key", "test_group_diff");
		
		final Validation<BasicMessageBean, JsonNode> res3a = dedup_context.emitMutableObject(0L, (ObjectNode) in3a, Optional.empty(), Optional.empty());
		final Validation<BasicMessageBean, JsonNode> res3b = dedup_context.emitMutableObject(0L, (ObjectNode) in3b, Optional.empty(), Optional.empty());
		final Validation<BasicMessageBean, JsonNode> res4a = dedup_context.emitMutableObject(0L, (ObjectNode) in4a, Optional.empty(), Optional.empty());
		final Validation<BasicMessageBean, JsonNode> res4b = dedup_context.emitMutableObject(0L, (ObjectNode) in4b, Optional.empty(), Optional.empty());
		
		JsonNode delete = _mapper.createObjectNode().put("_id", "delete_me");
		final Validation<BasicMessageBean, JsonNode> res_del = dedup_context.emitMutableObject(0L, (ObjectNode) delete, Optional.empty(), Optional.empty());
		
		//TODO: send a delete message for an _id
		
		final Collection<String> ret_val = dedup_context.getObjectIdsToDelete().map(j -> j.asText()).collect(Collectors.toList());
		
		// OK lots of asserting depending on what mode we're in
		if (config.custom_policy().equals(CustomPolicy.lax)) {
			org.junit.Assert.assertEquals(in1, res1.success());
			org.junit.Assert.assertEquals(in2, res2.success());
			org.junit.Assert.assertEquals(in3a, res3a.success());
			org.junit.Assert.assertEquals(in3b, res3b.success());
			org.junit.Assert.assertEquals(in4a, res4a.success());
			org.junit.Assert.assertEquals(in4b, res4b.success());
			org.junit.Assert.assertEquals(delete, res_del.success());
			
			org.junit.Assert.assertEquals(config.delete_unhandled_duplicates() ? Arrays.asList("delete_me", "test3") : Arrays.asList("delete_me"), ret_val);
		}
		else if (config.custom_policy().equals(CustomPolicy.strict)) {
			org.junit.Assert.assertEquals(in1, res1.success());
			org.junit.Assert.assertEquals(in2, res2.success());
			org.junit.Assert.assertEquals(DeduplicationEnrichmentContext._ERROR_BEAN, res3a.fail());
			org.junit.Assert.assertEquals(in3b, res3b.success());
			org.junit.Assert.assertEquals(DeduplicationEnrichmentContext._ERROR_BEAN, res4a.fail());
			org.junit.Assert.assertEquals(in4b, res4b.success());
			org.junit.Assert.assertEquals(delete, res_del.success());
			
			org.junit.Assert.assertEquals(config.delete_unhandled_duplicates() ? Arrays.asList("delete_me", "test3") : Arrays.asList("delete_me"), ret_val);
		}
		else if (config.custom_policy().equals(CustomPolicy.very_strict)) {
			org.junit.Assert.assertEquals(in1, res1.success());
			org.junit.Assert.assertEquals(DeduplicationEnrichmentContext._ERROR_BEAN, res2.fail());
			org.junit.Assert.assertEquals(DeduplicationEnrichmentContext._ERROR_BEAN, res3a.fail());
			org.junit.Assert.assertEquals(DeduplicationEnrichmentContext._ERROR_BEAN, res3b.fail());
			org.junit.Assert.assertEquals(DeduplicationEnrichmentContext._ERROR_BEAN, res4a.fail());
			org.junit.Assert.assertEquals(DeduplicationEnrichmentContext._ERROR_BEAN, res4b.fail());
			org.junit.Assert.assertEquals(DeduplicationEnrichmentContext._ERROR_BEAN, res_del.fail());
			
			org.junit.Assert.assertEquals(config.delete_unhandled_duplicates() ? Arrays.asList("test2", "test3") : Arrays.asList(), ret_val);
		}
		
		dedup_context.resetMutableState(Collections.emptyList(), _mapper.createObjectNode());
		
		org.junit.Assert.assertEquals(Collections.<JsonNode>emptyList(), dedup_context.getObjectIdsToDelete().collect(Collectors.toList()));
	}
	
	//////////////////////////////////////////////////////////////
	
	@Test
	public void test_miscCoverage() 
	{
		final DataSchemaBean.DocumentSchemaBean config =
				BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class)
					.with(DataSchemaBean.DocumentSchemaBean::delete_unhandled_duplicates, true)
					.with(DataSchemaBean.DocumentSchemaBean::custom_policy, CustomPolicy.lax)
				.done().get();
		final TestEnrichmentContext delegate = new TestEnrichmentContext();
		final DeduplicationEnrichmentContext dedup_context = new DeduplicationEnrichmentContext(delegate, config, j -> Optional.empty());
		dedup_context.getUnderlyingArtefacts();
		dedup_context.getUnderlyingPlatformDriver(null, null);
		dedup_context.getEnrichmentContextSignature(null, null);
		dedup_context.getTopologyEntryPoints(null, null);
		dedup_context.getTopologyStorageEndpoint(null, null);
		dedup_context.getTopologyErrorEndpoint(null, null);
		dedup_context.getNextUnusedId();
		dedup_context.convertToMutable(null);
		dedup_context.storeErroredObject(0L, null);
		dedup_context.externalEmit(null, null, null);
		dedup_context.flushBatchOutput(null);
		dedup_context.getServiceContext();
		dedup_context.getGlobalEnrichmentModuleObjectStore(null, null);
		dedup_context.getBucketObjectStore(null, null, null, null);
		dedup_context.getBucket();
		dedup_context.getModuleConfig();
		dedup_context.getBucketStatus(null);
		dedup_context.emergencyDisableBucket(null);
		dedup_context.emergencyQuarantineBucket(null, null);
		dedup_context.initializeNewContext(null);
		dedup_context.getLogger(null);
	}
	
	//////////////////////////////////////////////////////////////
	
	public static class TestEnrichmentContext implements IEnrichmentModuleContext {

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
		 */
		@Override
		public Collection<Object> getUnderlyingArtefacts() {
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingPlatformDriver(java.lang.Class, java.util.Optional)
		 */
		@Override
		public <T> Optional<T> getUnderlyingPlatformDriver(
				Class<T> driver_class, Optional<String> driver_options) {
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getEnrichmentContextSignature(java.util.Optional, java.util.Optional)
		 */
		@Override
		public String getEnrichmentContextSignature(
				Optional<DataBucketBean> bucket,
				Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services) {
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyEntryPoints(java.lang.Class, java.util.Optional)
		 */
		@Override
		public <T> Collection<Tuple2<T, String>> getTopologyEntryPoints(
				Class<T> clazz, Optional<DataBucketBean> bucket) {
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyStorageEndpoint(java.lang.Class, java.util.Optional)
		 */
		@Override
		public <T> T getTopologyStorageEndpoint(Class<T> clazz,
				Optional<DataBucketBean> bucket) {
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getTopologyErrorEndpoint(java.lang.Class, java.util.Optional)
		 */
		@Override
		public <T> T getTopologyErrorEndpoint(Class<T> clazz,
				Optional<DataBucketBean> bucket) {
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getNextUnusedId()
		 */
		@Override
		public long getNextUnusedId() {
			return 0;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#convertToMutable(com.fasterxml.jackson.databind.JsonNode)
		 */
		@Override
		public ObjectNode convertToMutable(JsonNode original) {
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emitMutableObject(long, com.fasterxml.jackson.databind.node.ObjectNode, java.util.Optional, java.util.Optional)
		 */
		@Override
		public Validation<BasicMessageBean, JsonNode> emitMutableObject(
				long id, ObjectNode mutated_json,
				Optional<AnnotationBean> annotations,
				Optional<JsonNode> grouping_key) {
			return Validation.success(mutated_json);
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emitImmutableObject(long, com.fasterxml.jackson.databind.JsonNode, java.util.Optional, java.util.Optional, java.util.Optional)
		 */
		@Override
		public Validation<BasicMessageBean, JsonNode> emitImmutableObject(
				long id, JsonNode original_json,
				Optional<ObjectNode> mutations,
				Optional<AnnotationBean> annotations,
				Optional<JsonNode> grouping_key) {
			return Validation.success(original_json);
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#storeErroredObject(long, com.fasterxml.jackson.databind.JsonNode)
		 */
		@Override
		public void storeErroredObject(long id, JsonNode original_json) {
			
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#externalEmit(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, fj.data.Either, java.util.Optional)
		 */
		@Override
		public Validation<BasicMessageBean, JsonNode> externalEmit(
				DataBucketBean bucket,
				Either<JsonNode, Map<String, Object>> object,
				Optional<AnnotationBean> annotations) {
			return null;
		}
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#flushBatchOutput(java.util.Optional)
		 */
		@Override
		public CompletableFuture<?> flushBatchOutput(
				Optional<DataBucketBean> bucket) {
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getServiceContext()
		 */
		@Override
		public IServiceContext getServiceContext() {
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getGlobalEnrichmentModuleObjectStore(java.lang.Class, java.util.Optional)
		 */
		@Override
		public <S> Optional<ICrudService<S>> getGlobalEnrichmentModuleObjectStore(
				Class<S> clazz, Optional<String> collection) {
			return null;
		}
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getBucketObjectStore(java.lang.Class, java.util.Optional, java.util.Optional, java.util.Optional)
		 */
		@Override
		public <S> ICrudService<S> getBucketObjectStore(Class<S> clazz,
				Optional<DataBucketBean> bucket, Optional<String> collection,
				Optional<StateDirectoryType> type) {
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getBucket()
		 */
		@Override
		public Optional<DataBucketBean> getBucket() {
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getModuleConfig()
		 */
		@Override
		public Optional<SharedLibraryBean> getModuleConfig() {
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getBucketStatus(java.util.Optional)
		 */
		@Override
		public Future<DataBucketStatusBean> getBucketStatus(
				Optional<DataBucketBean> bucket) {
			return null;
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emergencyDisableBucket(java.util.Optional)
		 */
		@Override
		public void emergencyDisableBucket(Optional<DataBucketBean> bucket) {
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#emergencyQuarantineBucket(java.util.Optional, java.lang.String)
		 */
		@Override
		public void emergencyQuarantineBucket(Optional<DataBucketBean> bucket,
				String quarantine_duration) {
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#initializeNewContext(java.lang.String)
		 */
		@Override
		public void initializeNewContext(String signature) {
		}
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#getLogger(java.util.Optional)
		 */
		@Override
		public IBucketLogger getLogger(Optional<DataBucketBean> bucket) {
			return null;
		}
	}
	
}
