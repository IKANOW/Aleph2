package com.ikanow.aleph2.data_import_manager.batch_enrichment.services;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_import.AnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

public class BatchEnrichmentContext implements IEnrichmentModuleContext {

	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class, Optional<String> driver_options) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getEnrichmentContextSignature(Optional<DataBucketBean> bucket,
			Optional<Set<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>> services) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getTopologyEntryPoint(Class<T> clazz, Optional<DataBucketBean> bucket) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getTopologyStorageEndpoint(Class<T> clazz, Optional<DataBucketBean> bucket) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getTopologyErrorEndpoint(Class<T> clazz, Optional<DataBucketBean> bucket) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getNextUnusedId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ObjectNode convertToMutable(JsonNode original) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void emitMutableObject(long id, ObjectNode mutated_json, Optional<AnnotationBean> annotation) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void emitImmutableObject(long id, JsonNode original_json, Optional<ObjectNode> mutations, Optional<AnnotationBean> annotations) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void storeErroredObject(long id, JsonNode original_json) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <I extends IUnderlyingService> Optional<I> getService(Class<I> service_clazz, Optional<String> service_name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <S> ICrudService<S> getBucketObjectStore(Class<S> clazz, Optional<DataBucketBean> bucket, Optional<String> sub_collection,
			boolean auto_apply_prefix) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Future<DataBucketStatusBean> getBucketStatus(Optional<DataBucketBean> bucket) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void logStatusForBucketOwner(Optional<DataBucketBean> bucket, BasicMessageBean message, boolean roll_up_duplicates) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void logStatusForBucketOwner(Optional<DataBucketBean> bucket, BasicMessageBean message) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void emergencyDisableBucket(Optional<DataBucketBean> bucket) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void emergencyQuarantineBucket(Optional<DataBucketBean> bucket, String quarantine_duration) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initializeNewContext(String signature) {
		// TODO Auto-generated method stub
		
	}


}
