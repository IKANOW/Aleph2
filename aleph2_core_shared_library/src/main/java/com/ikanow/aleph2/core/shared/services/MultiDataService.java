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

package com.ikanow.aleph2.core.shared.services;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Multimap;
import com.ikanow.aleph2.core.shared.utils.DataServiceUtils;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Tuples;

/** This wraps a set of data services so that a set of simple commands can be applied sensibly to all the services within
 *  (including cases where multiple services have the same implementation)
 * @author Alex
 *
 */
public class MultiDataService {

	final protected Multimap<IDataServiceProvider, String> _services;
	
	final protected boolean _doc_write_mode;
	
	// (will use nulls vs optional/setonce for efficiency)
	protected IDataWriteService<JsonNode> _crud_index_service;
	protected IDataWriteService.IBatchSubservice<JsonNode> _batch_index_service;	
	protected IDataWriteService<JsonNode> _crud_doc_service;
	protected IDataWriteService.IBatchSubservice<JsonNode> _batch_doc_service;	
	protected IDataWriteService<JsonNode> _crud_data_warehouse_service;
	protected IDataWriteService.IBatchSubservice<JsonNode> _batch_data_warehouse_service;	
	protected IDataWriteService<JsonNode> _crud_columnar_service;
	protected IDataWriteService.IBatchSubservice<JsonNode> _batch_columnar_service;
	protected IDataWriteService<JsonNode> _crud_temporal_service;
	protected IDataWriteService.IBatchSubservice<JsonNode> _batch_temporal_service;
	protected IDataWriteService<JsonNode> _crud_storage_service;
	protected IDataWriteService.IBatchSubservice<JsonNode> _batch_storage_service;				
	
	/** User c'tor 
	 * @param bucket
	 * @param context
	 * @param maybe_get_buffer_name
	 */
	public MultiDataService(final DataBucketBean bucket, final IServiceContext context, Optional<Function<IGenericDataService, Optional<String>>> maybe_get_buffer_name) {

		// Insert or overwrite mode:
		_doc_write_mode =
				Optionals.of(() -> bucket.data_schema().document_schema())
					.filter(ds -> Optional.ofNullable(ds.enabled()).orElse(true))
					.filter(ds -> (null != ds.deduplication_policy()) 
									|| !Optionals.ofNullable(ds.deduplication_fields()).isEmpty()
									|| !Optionals.ofNullable(ds.deduplication_contexts()).isEmpty()
							) // (ie dedup fields set)
					.isPresent()
				;
		
		_services = DataServiceUtils.selectDataServices(bucket.data_schema(), context);

		_services.asMap().entrySet().stream().forEach(kv -> {
			final Set<String> vals = kv.getValue().stream().collect(Collectors.toSet());
			// (the order doesn't really matter here, so just to "look" sensible:)
			if (vals.contains(DataSchemaBean.SearchIndexSchemaBean.name)) {				
				Tuple2<IDataWriteService<JsonNode>, IDataWriteService.IBatchSubservice<JsonNode>> t2 = getWriters(bucket, kv.getKey(), maybe_get_buffer_name);
				_crud_index_service = t2._1();
				_batch_index_service = t2._2();
			}
			else if (vals.contains(DataSchemaBean.DocumentSchemaBean.name)) {
				Tuple2<IDataWriteService<JsonNode>, IDataWriteService.IBatchSubservice<JsonNode>> t2 = getWriters(bucket, kv.getKey(), maybe_get_buffer_name);
				_crud_doc_service = t2._1();
				_batch_doc_service = t2._2();
			}
			else if (vals.contains(DataSchemaBean.DataWarehouseSchemaBean.name)) {
				Tuple2<IDataWriteService<JsonNode>, IDataWriteService.IBatchSubservice<JsonNode>> t2 = getWriters(bucket, kv.getKey(), maybe_get_buffer_name);
				_crud_columnar_service = t2._1();
				_batch_columnar_service = t2._2();
			}
			else if (vals.contains(DataSchemaBean.ColumnarSchemaBean.name)) {
				Tuple2<IDataWriteService<JsonNode>, IDataWriteService.IBatchSubservice<JsonNode>> t2 = getWriters(bucket, kv.getKey(), maybe_get_buffer_name);
				_crud_temporal_service = t2._1();
				_batch_temporal_service = t2._2();
			}
			else if (vals.contains(DataSchemaBean.TemporalSchemaBean.name)) {
				Tuple2<IDataWriteService<JsonNode>, IDataWriteService.IBatchSubservice<JsonNode>> t2 = getWriters(bucket, kv.getKey(), maybe_get_buffer_name);
				_crud_temporal_service = t2._1();
				_batch_temporal_service = t2._2();
			}
			else if (vals.contains(DataSchemaBean.StorageSchemaBean.name)) {
				Tuple2<IDataWriteService<JsonNode>, IDataWriteService.IBatchSubservice<JsonNode>> t2 = getWriters(bucket, kv.getKey(), maybe_get_buffer_name);
				_crud_storage_service = t2._1();
				_batch_storage_service = t2._2();
			}
		});		
	}
	
	/** Returns a stream of data providers
	 * @return
	 */
	public Collection<IDataServiceProvider> getDataServices() {
		return _services.keys();
	}
	
	public boolean batchWrite(final JsonNode obj_json) {
		boolean mutable_written = false;
		
		if (_batch_index_service!= null) {
			mutable_written = true;
			_batch_index_service.storeObject(obj_json, _doc_write_mode);
		}
		else if (_crud_index_service!= null){ // (super slow)
			mutable_written = true;
			_crud_index_service.storeObject(obj_json, _doc_write_mode);
		}
		if (_batch_doc_service!= null) {
			mutable_written = true;
			_batch_doc_service.storeObject(obj_json, _doc_write_mode);
		}
		else if (_crud_doc_service!= null){ // (super slow)
			mutable_written = true;
			_crud_doc_service.storeObject(obj_json, _doc_write_mode);
		}		
		if (_batch_columnar_service!= null) {
			mutable_written = true;
			_batch_columnar_service.storeObject(obj_json, _doc_write_mode);
		}
		else if (_crud_columnar_service!= null){ // (super slow)
			mutable_written = true;
			_crud_columnar_service.storeObject(obj_json, _doc_write_mode);
		}		
		if (_batch_temporal_service!= null) {
			mutable_written = true;
			_batch_temporal_service.storeObject(obj_json, _doc_write_mode);
		}
		else if (_crud_temporal_service!= null){ // (super slow)
			mutable_written = true;
			_crud_temporal_service.storeObject(obj_json, _doc_write_mode);
		}		
		
		if (_batch_storage_service!= null) {
			mutable_written = true;
			_batch_storage_service.storeObject(obj_json);
		}
		else if (_crud_storage_service!= null){ // (super slow)
			mutable_written = true;
			_crud_storage_service.storeObject(obj_json);
		}		
		
		return mutable_written;
	}
	
	/** Handy utility
	 * @param bucket
	 * @param service_provider
	 * @param maybe_get_buffer_name
	 * @return
	 */
	protected static Tuple2<IDataWriteService<JsonNode>, IDataWriteService.IBatchSubservice<JsonNode>> getWriters(
			final DataBucketBean bucket,			
			IDataServiceProvider service_provider,
			Optional<Function<IGenericDataService, Optional<String>>> maybe_get_buffer_name)
	{
		final IDataWriteService<JsonNode> crud_service;
		final IDataWriteService.IBatchSubservice<JsonNode> batch_storage_service;
		
		batch_storage_service = Optional.ofNullable(
				crud_service = Optional.of(service_provider)
											.flatMap(s -> s.getDataService())
											.flatMap(s -> s.getWritableDataService(JsonNode.class, bucket, Optional.empty(), maybe_get_buffer_name.<String>flatMap(f -> f.apply(s))))
											.orElse(null)
				)
				.flatMap(IDataWriteService::getBatchWriteSubservice)
				.orElse(null)
				;
		
		return Tuples._2T(crud_service, batch_storage_service);
	}
}
