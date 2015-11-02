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
 ******************************************************************************/
package com.ikanow.aleph2.data_model.utils;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsAccessContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean.AnalyticThreadJobInputBean;
import com.ikanow.aleph2.data_model.objects.data_import.AnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.AssetStateDirectoryBean.StateDirectoryType;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

import fj.data.Either;

public class TestContextUtils {

	@Test
	public void testHarvestContextCreation() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		
		// 1) class by itself
		IHarvestContext context1 = ContextUtils.getHarvestContext("com.ikanow.aleph2.data_model.utils.TestContextUtils$MockHarvestContext");

		assertTrue(context1 instanceof MockHarvestContext);
		assertTrue(context1 instanceof IHarvestContext);
		assertEquals(((MockHarvestContext)context1).dummySignature, null);
		
		// 2) class + configuration
		IHarvestContext context2 = ContextUtils.getHarvestContext("com.ikanow.aleph2.data_model.utils.TestContextUtils$MockHarvestContext:test_signature:includes:");
		
		assertTrue(context2 instanceof MockHarvestContext);
		assertTrue(context2 instanceof IHarvestContext);
		assertEquals(((MockHarvestContext)context2).dummySignature, "test_signature:includes:");
	}

	@Test
	public void testAnalyticsContextCreation() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		
		// 1) class by itself
		IAnalyticsContext context1 = ContextUtils.getAnalyticsContext("com.ikanow.aleph2.data_model.utils.TestContextUtils$MockAnalyticsContext");

		assertTrue(context1 instanceof MockAnalyticsContext);
		assertTrue(context1 instanceof IAnalyticsContext);
		assertEquals(((MockAnalyticsContext)context1).dummySignature, null);
		
		// 2) class + configuration
		IAnalyticsContext context2 = ContextUtils.getAnalyticsContext("com.ikanow.aleph2.data_model.utils.TestContextUtils$MockAnalyticsContext:test_signature:includes:");
		
		assertTrue(context2 instanceof MockAnalyticsContext);
		assertTrue(context2 instanceof IAnalyticsContext);
		assertEquals(((MockAnalyticsContext)context2).dummySignature, "test_signature:includes:");
	}
	
	@Test
	public void testEnrichmentContextCreation() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		
		// 1) class by itself
		IEnrichmentModuleContext context1 = ContextUtils.getEnrichmentContext("com.ikanow.aleph2.data_model.utils.TestContextUtils$MockEnrichmentContext");

		assertTrue(context1 instanceof MockEnrichmentContext);
		assertTrue(context1 instanceof IEnrichmentModuleContext);
		assertEquals(((MockEnrichmentContext)context1).dummySignature, null);
		
		// 2) class + configuration
		IEnrichmentModuleContext context2 = ContextUtils.getEnrichmentContext("com.ikanow.aleph2.data_model.utils.TestContextUtils$MockEnrichmentContext:test_signature:includes:");
		
		assertTrue(context2 instanceof MockEnrichmentContext);
		assertTrue(context2 instanceof IEnrichmentModuleContext);
		assertEquals(((MockEnrichmentContext)context2).dummySignature, "test_signature:includes:");
	}
	
	
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testHarvestContextCreationFailure() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		
		exception.expect(ClassNotFoundException.class);
		
		IHarvestContext context1 = ContextUtils.getHarvestContext("com.ikanow.aleph2.data_model.utils.TestContextUtils$DOES_NOT_EXIST");

		context1.getClass();
	}
	
	@Test
	public void testAnalyticsContextCreationFailure() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		
		exception.expect(ClassNotFoundException.class);
		
		IAnalyticsContext context1 = ContextUtils.getAnalyticsContext("com.ikanow.aleph2.data_model.utils.TestContextUtils$DOES_NOT_EXIST");

		context1.getClass();
	}
	
	// Needed for the tests
	
	public static class MockHarvestContext implements IHarvestContext {

		public MockHarvestContext() {}
		
		String dummySignature = null;
		
		@Override
		public List<String> getHarvestContextLibraries(
				Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services) {
			return null;
		}

		@Override
		public String getHarvestContextSignature(Optional<DataBucketBean> bucket, Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services) {
			return null;
		}

		@Override
		public IServiceContext getServiceContext() {
			return null;
		}

		@Override
		public void initializeNewContext(String signature) {
			dummySignature = signature;
		}

		@Override
		public CompletableFuture<Map<String, String>> getHarvestLibraries(
				Optional<DataBucketBean> bucket) {
			return null;
		}

		@Override
		public CompletableFuture<DataBucketStatusBean> getBucketStatus(
				Optional<DataBucketBean> bucket) {
			return null;
		}

		@Override
		public void logStatusForBucketOwner(Optional<DataBucketBean> bucket,
				BasicMessageBean message, boolean roll_up_duplicates) {
		}

		@Override
		public void logStatusForBucketOwner(Optional<DataBucketBean> bucket,
				BasicMessageBean message) {
		}

		@Override
		public String getTempOutputLocation(Optional<DataBucketBean> bucket) {
			return null;
		}

		@Override
		public String getFinalOutputLocation(Optional<DataBucketBean> bucket) {
			return null;
		}

		@Override
		public void emergencyDisableBucket(Optional<DataBucketBean> bucket) {
			
		}

		@Override
		public void emergencyQuarantineBucket(Optional<DataBucketBean> bucket,
				String quarantineDuration) {
			
		}

		@Override
		public void sendObjectToStreamingPipeline(
				Optional<DataBucketBean> bucket, Either<JsonNode, Map<String, Object>> object) {
			
		}

		@Override
		public Optional<DataBucketBean> getBucket() {
			return null;
		}

		@Override
		public SharedLibraryBean getTechnologyLibraryConfig() {
			return null;
		}

		@Override
		public <S> ICrudService<S> getGlobalHarvestTechnologyObjectStore(
				Class<S> clazz, Optional<String> collection) {
			return null;
		}

		@Override
		public <S> ICrudService<S> getBucketObjectStore(Class<S> clazz,
				Optional<DataBucketBean> bucket, Optional<String> collection,
				Optional<StateDirectoryType> type) {
			return null;
		}

		@Override
		public Collection<Object> getUnderlyingArtefacts() {
			return null;
		}

		@Override
		public <T> Optional<T> getUnderlyingPlatformDriver(
				Class<T> driver_class, Optional<String> driver_options) {
			return null;
		}

		@Override
		public void emitObject(Optional<DataBucketBean> bucket,
				Either<JsonNode, Map<String, Object>> object) {
		}

		@Override
		public <S> Optional<ICrudService<S>> getLibraryObjectStore(
				Class<S> clazz, String name_or_id, Optional<String> collection) {
			return null;
		}

		@Override
		public Map<String, SharedLibraryBean> getLibraryConfigs() {
			return null;
		}

		@Override
		public CompletableFuture<?> flushBatchOutput(
				Optional<DataBucketBean> bucket) {
			return null;
		}
	}
	public static class MockAnalyticsContext implements IAnalyticsContext {
		
		public MockAnalyticsContext() {}
		
		String dummySignature = null;


		@Override
		public void initializeNewContext(String signature) {
			dummySignature = signature;			
		}

		@Override
		public IServiceContext getServiceContext() {
			return null;
		}

		@Override
		public List<String> getAnalyticsContextLibraries(
				Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services) {
			return null;
		}

		@Override
		public CompletableFuture<Map<String, String>> getAnalyticsLibraries(
				Optional<DataBucketBean> bucket, final Collection<AnalyticThreadJobBean> jobs) {
			return null;
		}

		@Override
		public <S> ICrudService<S> getGlobalAnalyticTechnologyObjectStore(
				Class<S> clazz, Optional<String> collection) {
			return null;
		}

		@Override
		public <S> ICrudService<S> getBucketObjectStore(Class<S> clazz,
				Optional<DataBucketBean> bucket, Optional<String> collection,
				Optional<StateDirectoryType> type) {
			return null;
		}

		@Override
		public Optional<DataBucketBean> getBucket() {
			return null;
		}

		@Override
		public SharedLibraryBean getTechnologyConfig() {
			return null;
		}

		@Override
		public CompletableFuture<DataBucketStatusBean> getBucketStatus(
				Optional<DataBucketBean> bucket) {
			return null;
		}

		@Override
		public void logStatusForThreadOwner(Optional<DataBucketBean> bucket,
				BasicMessageBean message, boolean roll_up_duplicates) {
			
		}

		@Override
		public void logStatusForThreadOwner(Optional<DataBucketBean> bucket,
				BasicMessageBean message) {
			
		}

		@Override
		public void emergencyDisableBucket(Optional<DataBucketBean> bucket) {
			
		}

		@Override
		public void emergencyQuarantineBucket(Optional<DataBucketBean> bucket,
				String quarantine_duration) {
			
		}

		@Override
		public Optional<Tuple2<String, Optional<String>>> getOutputPath(
				Optional<DataBucketBean> bucket, AnalyticThreadJobBean job) {
			return null;
		}

		@Override
		public List<String> getInputPaths(
				Optional<DataBucketBean> bucket, AnalyticThreadJobBean job,
				AnalyticThreadJobInputBean job_input) {
			return null;
		}

		@Override
		public boolean checkForListeners(Optional<DataBucketBean> bucket,
				final AnalyticThreadJobBean job) {
			return false;
		}

		@Override
		public Collection<Object> getUnderlyingArtefacts() {
			return null;
		}

		@Override
		public <T> Optional<T> getUnderlyingPlatformDriver(
				Class<T> driver_class, Optional<String> driver_options) {
			return null;
		}

		@Override
		public String getAnalyticsContextSignature(
				Optional<DataBucketBean> bucket,
				Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services) {
			return null;
		}

		@Override
		public void sendObjectToStreamingPipeline(
				Optional<DataBucketBean> bucket, final AnalyticThreadJobBean job,
				Either<JsonNode, Map<String, Object>> object, final Optional<AnnotationBean> annotations) {
		}

		@Override
		public <T extends IAnalyticsAccessContext<?>> Optional<T> getServiceInput(
				Class<T> clazz, Optional<DataBucketBean> bucket,
				AnalyticThreadJobBean job, AnalyticThreadJobInputBean job_input) {
			return null;
		}

		@Override
		public <T extends IAnalyticsAccessContext<?>> Optional<T> getServiceOutput(
				Class<T> clazz, Optional<DataBucketBean> bucket,
				AnalyticThreadJobBean job, String data_service) {
			return null;
		}

		@Override
		public void emitObject(Optional<DataBucketBean> bucket,
				AnalyticThreadJobBean job,
				Either<JsonNode, Map<String, Object>> object, Optional<AnnotationBean> annotations) {
		}

		@Override
		public List<String> getInputTopics(Optional<DataBucketBean> bucket,
				AnalyticThreadJobBean job, AnalyticThreadJobInputBean job_input) {
			return null;
		}

		@Override
		public Optional<String> getOutputTopic(Optional<DataBucketBean> bucket,
				AnalyticThreadJobBean job) {
			return null;
		}

		@Override
		public <S> Optional<ICrudService<S>> getLibraryObjectStore(
				Class<S> clazz, String name_or_id, Optional<String> collection) {
			return null;
		}

		@Override
		public Map<String, SharedLibraryBean> getLibraryConfigs() {
			return null;
		}

		@Override
		public CompletableFuture<?> flushBatchOutput(
				Optional<DataBucketBean> bucket, AnalyticThreadJobBean job) {
			return null;
		}
	}
	
	public static class MockEnrichmentContext implements IEnrichmentModuleContext {

		public MockEnrichmentContext() {}
		
		String dummySignature = null;		
		
		@Override
		public Collection<Object> getUnderlyingArtefacts() {
			return null;
		}

		@Override
		public <T> Optional<T> getUnderlyingPlatformDriver(
				Class<T> driver_class, Optional<String> driver_options) {
			return null;
		}

		@Override
		public String getEnrichmentContextSignature(
				Optional<DataBucketBean> bucket,
				Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services) {
			return null;
		}

		@Override
		public <T> Collection<Tuple2<T, String>> getTopologyEntryPoints(final Class<T> clazz, final Optional<DataBucketBean> bucket) {
			return null;
		}

		@Override
		public <T> T getTopologyStorageEndpoint(Class<T> clazz,
				Optional<DataBucketBean> bucket) {
			return null;
		}

		@Override
		public <T> T getTopologyErrorEndpoint(Class<T> clazz,
				Optional<DataBucketBean> bucket) {
			return null;
		}

		@Override
		public long getNextUnusedId() {
			return 0;
		}

		@Override
		public ObjectNode convertToMutable(JsonNode original) {
			return null;
		}

		@Override
		public void emitMutableObject(long id, ObjectNode mutated_json,
				Optional<AnnotationBean> annotation, Optional<JsonNode> grouping_fields) {
		}

		@Override
		public void emitImmutableObject(long id, JsonNode original_json,
				Optional<ObjectNode> mutations,
				Optional<AnnotationBean> annotations, Optional<JsonNode> grouping_fields) {
		}

		@Override
		public void storeErroredObject(long id, JsonNode original_json) {
		}

		@Override
		public IServiceContext getServiceContext() {
			return null;
		}

		@Override
		public Future<DataBucketStatusBean> getBucketStatus(
				Optional<DataBucketBean> bucket) {
			return null;
		}

		@Override
		public void logStatusForBucketOwner(Optional<DataBucketBean> bucket,
				BasicMessageBean message, boolean roll_up_duplicates) {
		}

		@Override
		public void logStatusForBucketOwner(Optional<DataBucketBean> bucket,
				BasicMessageBean message) {
		}

		@Override
		public void emergencyDisableBucket(Optional<DataBucketBean> bucket) {
			
		}

		@Override
		public void emergencyQuarantineBucket(Optional<DataBucketBean> bucket,
				String quarantine_duration) {
		}

		@Override
		public void initializeNewContext(String signature) {
			dummySignature = signature;			
		}

		@Override
		public Optional<DataBucketBean> getBucket() {
			return null;
		}

		@Override
		public <S> ICrudService<S> getBucketObjectStore(Class<S> clazz,
				Optional<DataBucketBean> bucket, Optional<String> collection,
				Optional<StateDirectoryType> type) {
			return null;
		}

		@Override
		public <S> Optional<ICrudService<S>> getGlobalEnrichmentModuleObjectStore(
				Class<S> clazz, Optional<String> collection) {
			return null;
		}

		@Override
		public Optional<SharedLibraryBean> getModuleConfig() {
			return null;
		}

		@Override
		public CompletableFuture<?> flushBatchOutput(
				Optional<DataBucketBean> bucket) {
			return null;
		}
		
	}
}
