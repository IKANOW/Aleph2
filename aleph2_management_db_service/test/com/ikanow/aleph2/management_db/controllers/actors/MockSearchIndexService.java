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
package com.ikanow.aleph2.management_db.controllers.actors;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import scala.Tuple2;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.SearchIndexSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.utils.Tuples;

public class MockSearchIndexService implements ISearchIndexService {
	public MockSearchIndexService() {}
	
	public Multimap<String, Tuple2<String, Object>> _handleBucketDeletionRequests = LinkedHashMultimap.create();
	
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return null;
	}

	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(Class<T> driver_class,
			Optional<String> driver_options) {
		return null;
	}

	@Override
	public Tuple2<String, List<BasicMessageBean>> validateSchema(
			SearchIndexSchemaBean schema, DataBucketBean bucket) {
		return null;
	}

	public class MockElasticsearchDataService implements IDataServiceProvider.IGenericDataService {

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
		public Collection<String> getSecondaryBufferList(DataBucketBean bucket) {
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
			_handleBucketDeletionRequests.put("handleBucketDeletionRequest", Tuples._2T(bucket.full_name(), bucket_getting_deleted));
			return CompletableFuture.completedFuture(new BasicMessageBean(new Date(), true, "MockSearchIndexService", "handleBucketDeletionRequest", null, bucket.full_name() + ":" + bucket_getting_deleted, null));
		}

		@Override
		public Optional<String> getPrimaryBufferName(DataBucketBean bucket) {
			return null;
		}
		
	}
	@Override
	public Optional<IGenericDataService> getDataService() { 
		return Optional.of(new MockElasticsearchDataService());
	}
}
