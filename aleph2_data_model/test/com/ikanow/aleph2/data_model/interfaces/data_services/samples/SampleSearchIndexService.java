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
package com.ikanow.aleph2.data_model.interfaces.data_services.samples;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.Tuples;

public class SampleSearchIndexService implements ISearchIndexService {

	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(
			Class<T> driver_class, Optional<String> driver_options) {
		return null;
	}

	@Override
	public Tuple2<String, List<BasicMessageBean>> validateSchema(SearchIndexSchemaBean schema, final DataBucketBean bucket) {
		return Tuples._2T("", Collections.emptyList());
	}

	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return null;
	}

	public class MockDataService implements IDataServiceProvider.IGenericDataService {

		@Override
		public <O> Optional<IDataWriteService<O>> getWritableDataService(
				Class<O> clazz, DataBucketBean bucket,
				Optional<String> options, Optional<String> secondary_buffer) {
			return null;
		}

		@Override
		public <O> Optional<ICrudService<O>> getReadableCrudService(
				Class<O> clazz, Collection<DataBucketBean> buckets,
				Optional<String> options) {
			return null;
		}

		@Override
		public Set<String> getSecondaryBuffers(DataBucketBean bucket) {
			return null;
		}

		@Override
		public CompletableFuture<BasicMessageBean> switchCrudServiceToPrimaryBuffer(
				DataBucketBean bucket, String secondary_buffer, final Optional<String> new_name_for_ex_primary) {
			return null;
		}

		@Override
		public CompletableFuture<BasicMessageBean> handleAgeOutRequest(
				DataBucketBean bucket) {
			return null;
		}

		@Override
		public CompletableFuture<BasicMessageBean> handleBucketDeletionRequest(
				DataBucketBean bucket, Optional<String> secondary_buffer,
				boolean bucket_getting_deleted) {
			return null;
		}

		@Override
		public Optional<String> getPrimaryBufferName(DataBucketBean bucket) {
			return null;
		}
		
	}
	@Override
	public Optional<IGenericDataService> getDataService() { 
		return Optional.of(new MockDataService());
	}
}
