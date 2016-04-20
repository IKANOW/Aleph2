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

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDataWarehouseService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.TemplateHelper;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;

/** Utilities for helping manage the different operations related to data services in buckets
 * @author Alex
 */
public class DataServiceUtils {
	
	// common fields
	private static final String enabled_ = "enabled";
	private static final String service_name_ = "service_name";
	
	/** Gets a minimal of service instances and associated service names
	 *  (since each service instance can actually implement multiple services)
	 * @param data_schema
	 * @param context
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static Multimap<IDataServiceProvider, String> selectDataServices(final DataSchemaBean data_schema, final IServiceContext context) {		
		final Multimap<IDataServiceProvider, String> mutable_output = HashMultimap.create();
		if (null == data_schema) { //(rare NPE check! - lets calling client invoke bucket.data_schema() without having to worry about it)
			return mutable_output;
		}
		
		listDataSchema(data_schema)
			.forEach(schema_name -> {
				Optional.of(schema_name._1())
					.flatMap(s -> getDataServiceInterface(schema_name._2()))
					.flatMap(ds_name -> context.getService((Class<IUnderlyingService>)(Class<?>)ds_name, Optional.ofNullable(schema_name._1().<String>get(service_name_))))
					.ifPresent(ds -> mutable_output.put((IDataServiceProvider)ds, schema_name._2()));
					;
			});
		
		return mutable_output;
	}

	/** Returns a list of interface/optional-non-default-name for the designated data schema
	 *  (WARNING: considers each use of a given data service to be unique, ie returns 2 elements for search/doc service if both implemented by ES) 
	 * @param data_schema
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static List<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>> listUnderlyingServiceProviders(final DataSchemaBean data_schema) {
		return streamDataServiceProviders(data_schema)
					.<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>>
						map(t2 -> Tuples._2T((Class<? extends IUnderlyingService>)(Class<?>) t2._1(), t2._2()))
					.collect(Collectors.toList());		
	}
	
	/** Returns a list of interface/optional-non-default-name for the designated data schema
	 *  (WARNING: considers each use of a given data service to be unique, ie returns 2 elements for search/doc service if both implemented by ES) 
	 * @param data_schema
	 * @return
	 */
	public static List<Tuple2<Class<? extends IDataServiceProvider>, Optional<String>>> listDataServiceProviders(final DataSchemaBean data_schema) {
		return streamDataServiceProviders(data_schema).collect(Collectors.toList());
	}
	
	/** Returns a list of interface/optional-non-default-name for the designated data schema
	 *  (WARNING: considers each use of a given data service to be unique, ie returns 2 elements for search/doc service if both implemented by ES) 
	 * @param data_schema
	 * @return
	 */
	protected static Stream<Tuple2<Class<? extends IDataServiceProvider>, Optional<String>>> streamDataServiceProviders(final DataSchemaBean data_schema) {
		if (null == data_schema) { //(rare NPE check! - lets calling client invoke bucket.data_schema() without having to worry about it)
			return Stream.empty();
		}
		return listDataSchema(data_schema).<Optional<Tuple2<Class<? extends IDataServiceProvider>, Optional<String>>>>map(schema_name -> {
			return 
			Optional.of(schema_name._1())
				.flatMap(s -> getDataServiceInterface(schema_name._2()))
				.map(ds -> Tuples._2T(ds, Optional.ofNullable(schema_name._1().<String>get(service_name_))))
				;			
		})
		.flatMap(maybe -> StreamUtils.stream(maybe))	
		;
	}

	/** Common utility function for selectDataServices, listDataServiceProviders
	 * @param data_schema
	 * @return
	 */
	@SuppressWarnings({ "static-access" })
	protected static Stream<Tuple2<TemplateHelper<Object>, String>> listDataSchema(final DataSchemaBean data_schema) {
		return Stream.of(
				Tuples._2T(data_schema.search_index_schema(), data_schema.search_index_schema().name),
				Tuples._2T(data_schema.storage_schema(), data_schema.storage_schema().name),
				Tuples._2T(data_schema.document_schema(), data_schema.document_schema().name),
				Tuples._2T(data_schema.columnar_schema(), data_schema.columnar_schema().name),
				Tuples._2T(data_schema.temporal_schema(), data_schema.temporal_schema().name),
				Tuples._2T(data_schema.data_warehouse_schema(), data_schema.data_warehouse_schema().name),
				Tuples._2T(data_schema.graph_schema(), data_schema.graph_schema().name)
				)
			.flatMap(schema_name -> Optional.<Object>ofNullable(schema_name._1())
										.map(schema -> Tuples._2T(BeanTemplateUtils.build(schema), schema_name._2()))
										.map(Stream::of).orElseGet(Stream::empty)
			)
			.filter(schema_name -> Optional.ofNullable(schema_name._1().<Boolean>get(enabled_)).orElse(true))
			;
	}
	
	/** Simple map of data service provider name to interface (underlying service mode)
	 * @param data_service
	 * @return
	 */	
	@SuppressWarnings("unchecked")
	public static Optional<Class<? extends IUnderlyingService>> getUnderlyingServiceInterface(final String data_service) {
		return getDataServiceInterface(data_service).map(s -> (Class<? extends IUnderlyingService>)(Class<?>)s);
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
					.when(ds -> DataSchemaBean.GraphSchemaBean.name.equals(ds), __ -> IGraphService.class)
					.when(ds -> DataSchemaBean.DataWarehouseSchemaBean.name.equals(ds), __ -> IDataWarehouseService.class)
					.otherwise(() -> null)
				)
				;
	}

}
