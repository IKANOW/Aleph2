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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;






import org.junit.Test;
import org.mockito.Mockito;






import com.fasterxml.jackson.databind.JsonNode;
import com.ikanow.aleph2.data_model.interfaces.data_services.IColumnarService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDataWarehouseService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IDocumentService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ISearchIndexService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.data_services.ITemporalService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataWriteService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.MockServiceContext;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService;

/**
 * @author Alex
 *
 */
public class TestMultiDataService {

	@Test
	public void test_MultiDataService() {
		
	}
	
	@Test
	public void test_MultiDataService_empty() {
		
	}
	
	protected Map<String, Integer> _crud_responses = new HashMap<>();
	protected Map<String, Integer> _batch_responses = new HashMap<>();
	
	/**
	 * @param mode: 0 => no data service provider (or no writable), 1 => no batch provider
	 * @return
	 */
	public MockServiceContext getPopulatedServiceContext(int mode) {
		MockServiceContext mock_service_context = new MockServiceContext();
		
		final ISearchIndexService search_index = Mockito.mock(ISearchIndexService.class);
		final IDocumentService document = Mockito.mock(IDocumentService.class);
		final IColumnarService columnar = Mockito.mock(IColumnarService.class);
		final ITemporalService temporal = Mockito.mock(ITemporalService.class);
		final IDataWarehouseService data_warehouse = Mockito.mock(IDataWarehouseService.class);
		final IStorageService storage = Mockito.mock(IStorageService.class);
		
		Stream.<IDataServiceProvider>of(search_index, document, columnar, temporal, data_warehouse, storage)
			.forEach(ds -> {
				if (0 == mode) {
					boolean tiebreak = 0 == (ds.getClass().toString().hashCode() % 2); //(get some code coverage)
					if (tiebreak) {
						Mockito.when(ds.getDataService()).thenReturn(Optional.empty());
					}
					else {
						final IGenericDataService gds = Mockito.mock(IGenericDataService.class);
						Mockito.when(gds.getWritableDataService(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Optional.empty());
						Mockito.when(ds.getDataService()).thenReturn(Optional.of(gds));
					}
				}
				else if (1 == mode) { // use slow write mode
					final IGenericDataService gds = Mockito.mock(IGenericDataService.class);
					
					@SuppressWarnings("unchecked")
					final IDataWriteService<JsonNode> dws = (IDataWriteService<JsonNode>)Mockito.mock(IDataWriteService.class);
					Mockito.when(dws.getBatchWriteSubservice()).thenReturn(Optional.empty());
					//TODO: also add store object
							
					Mockito.when(gds.getWritableDataService(JsonNode.class, Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Optional.of(dws));
					Mockito.when(ds.getDataService()).thenReturn(Optional.of(gds));
					//TODO
				}
				else { //full mapper
					
				}
			});
			;
		
		
		mock_service_context.addService(ISearchIndexService.class, Optional.empty(), search_index);
		mock_service_context.addService(IDocumentService.class, Optional.empty(), document);
		mock_service_context.addService(IColumnarService.class, Optional.empty(), columnar);
		mock_service_context.addService(ITemporalService.class, Optional.empty(), temporal);
		mock_service_context.addService(IDataWarehouseService.class, Optional.empty(), data_warehouse);
		mock_service_context.addService(IStorageService.class, Optional.empty(), storage);
		
		return mock_service_context;
	}
}
