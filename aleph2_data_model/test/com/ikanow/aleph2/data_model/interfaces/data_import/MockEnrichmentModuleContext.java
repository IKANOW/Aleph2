package com.ikanow.aleph2.data_model.interfaces.data_import;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_import.AnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

public class MockEnrichmentModuleContext implements IEnrichmentModuleContext {

	@Override
	public String getEnrichmentContextSignature(Optional<DataBucketBean> bucket, 
			final Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services) {
		return null;
	}

	@Override
	public <T> T getTopologyEntryPoint(Class<T> clazz, Optional<DataBucketBean> bucket) {
		return null;
	}

	@Override
	public <T> T getTopologyStorageEndpoint(Class<T> clazz, Optional<DataBucketBean> bucket) {
		return null;
	}

	@Override
	public <T> T getTopologyErrorEndpoint(Class<T> clazz, Optional<DataBucketBean> bucket) {
		return null;
	}

	@Override
	public ObjectNode convertToMutable(JsonNode original) {
		return null;
	}

	@Override
	public void emitMutableObject(long id, ObjectNode mutated_json, Optional<AnnotationBean> annotation) {
	}

	@Override
	public void emitImmutableObject(long id, JsonNode original_json, Optional<ObjectNode> mutations, Optional<AnnotationBean> annotations) {
	}

	@Override
	public void storeErroredObject(long id, JsonNode original_json) {
	}

	@Override
	public long getNextUnusedId() {
		return 0;
	}

	@Override
	public <I extends IUnderlyingService> Optional<I> getService(Class<I> service_clazz, Optional<String> service_name) {
		return null;
	}

	@Override
	public <S> ICrudService<S> getBucketObjectStore(Class<S> clazz, Optional<DataBucketBean> bucket, Optional<String> sub_collection,
			boolean auto_apply_prefix) {
		return null;
	}

	@Override
	public Future<DataBucketStatusBean> getBucketStatus(Optional<DataBucketBean> bucket) {
		return null;
	}

	@Override
	public void logStatusForBucketOwner(Optional<DataBucketBean> bucket, BasicMessageBean message, boolean roll_up_duplicates) {

	}

	@Override
	public void logStatusForBucketOwner(Optional<DataBucketBean> bucket, BasicMessageBean message) {

	}

	@Override
	public void emergencyDisableBucket(Optional<DataBucketBean> bucket) {

	}

	@Override
	public void emergencyQuarantineBucket(Optional<DataBucketBean> bucket, String quarantine_duration) {

	}

	@Override
	public void initializeNewContext(String signature) {

	}

	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return null;
	}

	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		return null;
	}

}
