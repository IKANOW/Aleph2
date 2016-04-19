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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.mockito.Mockito;

import scala.Tuple2;

import com.google.common.collect.Multimap;
import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDataWarehouseService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IGraphService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;

/**
 * @author Alex
 *
 */
public class TestDataServiceUtils {
	
	@Test
	public void test_beans() {

		final DataSchemaBean.SearchIndexSchemaBean test =
				BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
					.with(DataSchemaBean.SearchIndexSchemaBean::service_name, "test")
				.done().get();
	
		BeanTemplateUtils.TemplateHelper<?> test_converted =  BeanTemplateUtils.build(test);
		
		assertEquals("test", test_converted.get("service_name"));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test_listDataServiceProviders() {
		
		// Normal test
		{
			final MockServiceContext mock_service_context = new MockServiceContext();
			Stream.of(
					ISearchIndexService.class,
					IStorageService.class,
					IDocumentService.class,
					IColumnarService.class,
					ITemporalService.class,
					IGraphService.class,
					IDataWarehouseService.class
					)
					.forEach(s -> mock_service_context.addService((Class<IUnderlyingService>)(Class<?>)s, Optional.empty(), Mockito.mock(s)))					
					;
			
			final DataSchemaBean schema = 
					BeanTemplateUtils.build(DataSchemaBean.class)
						.with(DataSchemaBean::search_index_schema, BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class).done().get())
						.with(DataSchemaBean::storage_schema, BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.class).done().get())
						.with(DataSchemaBean::document_schema, BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class).done().get())
						.with(DataSchemaBean::columnar_schema, BeanTemplateUtils.build(DataSchemaBean.ColumnarSchemaBean.class).done().get())
						.with(DataSchemaBean::temporal_schema, BeanTemplateUtils.build(DataSchemaBean.TemporalSchemaBean.class).done().get())
						.with(DataSchemaBean::graph_schema, BeanTemplateUtils.build(DataSchemaBean.GraphSchemaBean.class).done().get())
						.with(DataSchemaBean::data_warehouse_schema, BeanTemplateUtils.build(DataSchemaBean.DataWarehouseSchemaBean.class).done().get())
					.done().get()
					;
			
			{
				final List<Tuple2<Class<? extends IDataServiceProvider>, Optional<String>>> ret_val = DataServiceUtils.listDataServiceProviders(schema);			
				assertEquals(7, ret_val.size());
			}
			// Coverage:
			{
				final List<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>> ret_val = DataServiceUtils.listUnderlyingServiceProviders(schema);			
				assertEquals(7, ret_val.size());
			}
		}
		// Check non-standard service:
		{
			final MockServiceContext mock_service_context = new MockServiceContext();
			Stream.of(
					ISearchIndexService.class
					)
					.forEach(s -> mock_service_context.addService((Class<IUnderlyingService>)(Class<?>)s, Optional.of("non_standard"), Mockito.mock(s)))					
					;
			
			final DataSchemaBean schema = 
					BeanTemplateUtils.build(DataSchemaBean.class)
						.with(DataSchemaBean::search_index_schema, BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class)
								.with(DataSchemaBean.SearchIndexSchemaBean::service_name, "non_standard")
								.done().get())
						.done().get()
						;
			
			{
				final List<Tuple2<Class<? extends IDataServiceProvider>, Optional<String>>> ret_val = DataServiceUtils.listDataServiceProviders(schema);			
				assertEquals(1, ret_val.size());
				assertEquals(Optional.of("non_standard"), ret_val.get(0)._2());
			}			
			// Coverage:
			{
				final List<Tuple2<Class<? extends IUnderlyingService>, Optional<String>>> ret_val_2 = DataServiceUtils.listUnderlyingServiceProviders(schema);
				assertEquals(1, ret_val_2.size());
				assertEquals(Optional.of("non_standard"), ret_val_2.get(0)._2());
			}
		}
		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test_selectDataServices() {
		
		// Simple test, returns 1 per
		{
			final MockServiceContext mock_service_context = new MockServiceContext();
			Stream.of(
					ISearchIndexService.class,
					IStorageService.class,
					IDocumentService.class,
					IColumnarService.class,
					ITemporalService.class,
					IGraphService.class,
					IDataWarehouseService.class
					)
					.forEach(s -> mock_service_context.addService((Class<IUnderlyingService>)(Class<?>)s, Optional.empty(), Mockito.mock(s)))
					;
			
			final DataSchemaBean schema = 
					BeanTemplateUtils.build(DataSchemaBean.class)
						.with(DataSchemaBean::search_index_schema, BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class).done().get())
						.with(DataSchemaBean::storage_schema, BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.class).done().get())
						.with(DataSchemaBean::document_schema, BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class).done().get())
						.with(DataSchemaBean::columnar_schema, BeanTemplateUtils.build(DataSchemaBean.ColumnarSchemaBean.class).done().get())
						.with(DataSchemaBean::temporal_schema, BeanTemplateUtils.build(DataSchemaBean.TemporalSchemaBean.class).done().get())
						.with(DataSchemaBean::graph_schema, BeanTemplateUtils.build(DataSchemaBean.GraphSchemaBean.class).done().get())
						.with(DataSchemaBean::data_warehouse_schema, BeanTemplateUtils.build(DataSchemaBean.DataWarehouseSchemaBean.class).done().get())
					.done().get()
					;
			
			final Multimap<IDataServiceProvider, String> res = DataServiceUtils.selectDataServices(schema, mock_service_context);
			
			assertEquals(7, res.size());
			assertEquals(7, res.asMap().size());
			
			Stream.of(
					Tuples._2T(mock_service_context.getService(ISearchIndexService.class, Optional.empty()).get(), DataSchemaBean.SearchIndexSchemaBean.name),
					Tuples._2T(mock_service_context.getService(IStorageService.class, Optional.empty()).get(), DataSchemaBean.StorageSchemaBean.name),
					Tuples._2T(mock_service_context.getService(IDocumentService.class, Optional.empty()).get(), DataSchemaBean.DocumentSchemaBean.name),
					Tuples._2T(mock_service_context.getService(IColumnarService.class, Optional.empty()).get(), DataSchemaBean.ColumnarSchemaBean.name),
					Tuples._2T(mock_service_context.getService(ITemporalService.class, Optional.empty()).get(), DataSchemaBean.TemporalSchemaBean.name),
					Tuples._2T(mock_service_context.getService(IGraphService.class, Optional.empty()).get(), DataSchemaBean.GraphSchemaBean.name),
					Tuples._2T(mock_service_context.getService(IDataWarehouseService.class, Optional.empty()).get(), DataSchemaBean.DataWarehouseSchemaBean.name)
					)
					.forEach(t2 -> assertEquals(t2._2(), res.asMap().get(t2._1()).stream().findFirst().orElse("FAIL")));
		}
		// Check with a few missing, and a few shared
		{
			final MockServiceContext mock_service_context = new MockServiceContext();
			final ISearchIndexService shared_service = Mockito.mock(ISearchIndexService.class);
			Stream.of(
					ISearchIndexService.class,
					IDocumentService.class,
					ITemporalService.class,
					IDataWarehouseService.class
					)
					.forEach(s -> mock_service_context.addService((Class<IUnderlyingService>)(Class<?>)s, Optional.empty(), shared_service))
					;
			Stream.of(
					IStorageService.class
					)
					.forEach(s -> mock_service_context.addService(s, Optional.empty(), Mockito.mock(s)))
					;
			
			final DataSchemaBean schema = 
					BeanTemplateUtils.build(DataSchemaBean.class)
						.with(DataSchemaBean::search_index_schema, BeanTemplateUtils.build(DataSchemaBean.SearchIndexSchemaBean.class).done().get())
						.with(DataSchemaBean::storage_schema, BeanTemplateUtils.build(DataSchemaBean.StorageSchemaBean.class).done().get())
						.with(DataSchemaBean::document_schema, BeanTemplateUtils.build(DataSchemaBean.DocumentSchemaBean.class).done().get())
						.with(DataSchemaBean::columnar_schema, BeanTemplateUtils.build(DataSchemaBean.ColumnarSchemaBean.class).done().get())
						.with(DataSchemaBean::data_warehouse_schema, BeanTemplateUtils.build(DataSchemaBean.DataWarehouseSchemaBean.class).done().get())
					.done().get()
					;
			
			final Multimap<IDataServiceProvider, String> res = DataServiceUtils.selectDataServices(schema, mock_service_context);
			
			assertEquals(4, res.size());
			assertEquals(2, res.asMap().size());
			
			assertEquals(Arrays.asList("data_warehouse_service", "document_service", "search_index_service"),
					res.get(shared_service).stream().sorted().collect(Collectors.toList())
					);
			
			assertEquals("storage_service", res.get(mock_service_context.getService(IStorageService.class, Optional.empty()).get()).stream().findFirst().orElse("FAIL"));
		}
		
	}
}
