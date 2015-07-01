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

import java.util.Optional;
import java.util.concurrent.Future;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentModuleContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_import.AnnotationBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

public class StreamingEnrichmentContext implements IEnrichmentModuleContext {

	@Override
	public String getEnrichmentContextSignature(Optional<DataBucketBean> bucket) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getTopologyEntryPoint(Class<T> clazz,
			Optional<DataBucketBean> bucket) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getTopologyStorageEndpoint(Class<T> clazz,
			Optional<DataBucketBean> bucket) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getTopologyErrorEndpoint(Class<T> clazz,
			Optional<DataBucketBean> bucket) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ObjectNode convertToMutable(JsonNode original) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void emitMutableObject(long id, ObjectNode mutated_json,
			Optional<AnnotationBean> annotation) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void emitImmutableObject(long id, JsonNode original_json,
			Optional<ObjectNode> mutations, Optional<AnnotationBean> annotations) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void storeErroredObject(long id, JsonNode original_json) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long getNextUnusedId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public <I> Optional<I> getService(Class<I> service_clazz,
			Optional<String> service_name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <S> ICrudService<S> getBucketObjectStore(Class<S> clazz,
			Optional<DataBucketBean> bucket, Optional<String> sub_collection,
			boolean auto_apply_prefix) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Future<DataBucketStatusBean> getBucketStatus(
			Optional<DataBucketBean> bucket) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void logStatusForBucketOwner(Optional<DataBucketBean> bucket,
			BasicMessageBean message, boolean roll_up_duplicates) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void logStatusForBucketOwner(Optional<DataBucketBean> bucket,
			BasicMessageBean message) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void emergencyDisableBucket(Optional<DataBucketBean> bucket) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void emergencyQuarantineBucket(Optional<DataBucketBean> bucket,
			String quarantine_duration) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initializeNewContext(String signature) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <T> T getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		// TODO Auto-generated method stub
		return null;
	}

}
