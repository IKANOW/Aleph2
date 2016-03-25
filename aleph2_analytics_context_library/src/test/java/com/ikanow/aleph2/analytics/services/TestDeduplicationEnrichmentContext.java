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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.junit.Test;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
		
	}
	@Test
	public void test_laxPolicy_delete() {
		
	}
	@Test
	public void test_strictPolicy_noDelete() {
		
	}
	@Test
	public void test_strictPolicy_delete() {
		
	}
	@Test
	public void test_veryStrictPolicy_noDelete() {
		
	}
	@Test
	public void test_veryStrictPolicy_delete() {
		
	}
	
	public void test_common(DataSchemaBean.DocumentSchemaBean config) {
		
		final TestEnrichmentContext delegate = new TestEnrichmentContext();
		
		final DeduplicationEnrichmentContext dedup_context = new DeduplicationEnrichmentContext(delegate, config, j -> Optional.of(j.get("grouping_key")));
		
		dedup_context.resetMutableState(Collections.emptyList(), _mapper.createObjectNode().put("grouping_key", "test_group"));
	}
	
	//////////////////////////////////////////////////////////////
	
	@Test
	public void test_miscCoverage() {
		
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
			return null;
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
			return null;
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
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#logStatusForBucketOwner(java.util.Optional, com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean, boolean)
		 */
		@Override
		public void logStatusForBucketOwner(Optional<DataBucketBean> bucket,
				BasicMessageBean message, boolean roll_up_duplicates) {
			
		}

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext#logStatusForBucketOwner(java.util.Optional, com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean)
		 */
		@Override
		public void logStatusForBucketOwner(Optional<DataBucketBean> bucket,
				BasicMessageBean message) {
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
		
	}
	
}
