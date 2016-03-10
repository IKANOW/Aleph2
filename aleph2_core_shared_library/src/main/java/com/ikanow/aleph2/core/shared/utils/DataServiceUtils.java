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

package com.ikanow.aleph2.core.shared.utils;

import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDataWarehouseService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;

/** Utilities for helping manage the different operations related to data services in buckets
 * @author Alex
 */
public class DataServiceUtils {
	
	/** Gets a minimal of service instances and associated service names
	 *  (since each service instance can actually implement multiple services)
	 * @param data_schema
	 * @param context
	 * @return
	 */
	@SuppressWarnings({ "static-access", "unchecked" })
	public static Multimap<IDataServiceProvider, String> selectDataServices(final DataSchemaBean data_schema, final IServiceContext context) {		
		final Multimap<IDataServiceProvider, String> mutable_output = HashMultimap.create();
		if (null == data_schema) { //(rare NPE check! - lets calling client invoke bucket.data_schema() without having to worry about it)
			return mutable_output;
		}
		
		// common fields
		final String enabled_ = "enabled";
		final String service_name_ = "service_name";
		
		Stream.of(
				Tuples._2T(data_schema.search_index_schema(), data_schema.search_index_schema().name),
				Tuples._2T(data_schema.storage_schema(), data_schema.storage_schema().name),
				Tuples._2T(data_schema.document_schema(), data_schema.document_schema().name),
				Tuples._2T(data_schema.columnar_schema(), data_schema.columnar_schema().name),
				Tuples._2T(data_schema.temporal_schema(), data_schema.temporal_schema().name),
				Tuples._2T(data_schema.data_warehouse_schema(), data_schema.data_warehouse_schema().name)
				)
			.flatMap(schema_name -> Optional.<Object>ofNullable(schema_name._1())
										.map(schema -> Tuples._2T(BeanTemplateUtils.build(schema), schema_name._2()))
										.map(Stream::of).orElseGet(Stream::empty)
			)
			.forEach(schema_name -> {
				Optional.of(schema_name._1()).filter(s -> Optional.ofNullable(s.<Boolean>get(enabled_)).orElse(true)).ifPresent(schema -> {
					Optional.of(schema)
						.flatMap(s -> getDataServiceInterface(schema_name._2()))
						.flatMap(ds_name -> context.getService((Class<IUnderlyingService>)(Class<?>)ds_name, Optional.ofNullable(schema.<String>get(service_name_))))
						.ifPresent(ds -> mutable_output.put((IDataServiceProvider)ds, schema_name._2()));
						;
				});					
			});
		
		return mutable_output;
	}
	

	/** Simple map of data service provider name to interface
	 * @param data_service
	 * @return
	 */
	public static Optional<Class<? extends IDataServiceProvider>> getDataServiceInterface(final String data_service) {
		return Optional.<Class<? extends IDataServiceProvider>>ofNullable(
				Patterns.match(data_service).<Class<? extends IDataServiceProvider>>andReturn()
					.when(ds -> DataSchemaBean.SearchIndexSchemaBean.name.equals(ds), __ -> ISearchIndexService.class)
					.when(ds -> DataSchemaBean.StorageSchemaBean.name.equals(ds), __ -> IStorageService.class)
					.when(ds -> DataSchemaBean.DocumentSchemaBean.name.equals(ds), __ -> IDocumentService.class)
					.when(ds -> DataSchemaBean.ColumnarSchemaBean.name.equals(ds), __ -> IColumnarService.class)
					.when(ds -> DataSchemaBean.TemporalSchemaBean.name.equals(ds), __ -> ITemporalService.class)
					.when(ds -> DataSchemaBean.DataWarehouseSchemaBean.name.equals(ds), __ -> IDataWarehouseService.class)
					.otherwise(() -> null)
				)
				;
	}

}
