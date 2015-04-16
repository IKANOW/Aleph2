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
package com.ikanow.aleph2.data_model.interfaces.data_layers;

import java.util.Optional;

import com.ikanow.aleph2.data_model.interfaces.shared.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStateAndStatusBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

public interface IManagementDbService {

	//TODO javadocs
	
	//TODO what about a search index interface for these? Is that optional or ...?
	
	////////////////////////////////////
	
	// 1] Access
	
	// 1.1] JAR Library, includes:
	//      - Harvest Technologies
	//		- Harvest modules (no interface)
	//		- Enrichment modules
	//		- Analytics technologies
	//		- Analytics modules
	//		- Analytics utilities (no interface)
	//		- Access modules
	//		- Acccess utilities (no interface)
	
	ICrudService<SharedLibraryBean> getSharedLibraryStore();
	
	//TODO: shared schemas
	
	//TODO: other "shares"
	
	//TODO: security and other things that we'll initially handle from IKANOW v1
	
	////////////////////////////////////
	
	// 2] Imports
	
	// 2.1] Buckets	
	
	//TODO need a higher level interface to this? Or do I just put all the logic in the CRUD service implementation
	
	ICrudService<DataSchemaBean> getDataBucketStore();
	
	ICrudService<DataBucketStateAndStatusBean> getDataBucketStatusStore();
	
	//TODO per bucket state?
	
	////////////////////////////////////
	
	// Analytics

	//TODO: analytics threads	
	
	//TODO: analytics state
	
	/** USE WITH CARE: this returns the driver to the underlying technology
	 *  shouldn't be used unless absolutely necessary!
	 * @param driver_class the class of the driver
	 * @param a string containing options in some technology-specific format
	 * @return a driver to the underlying technology. Will exception if you pick the wrong one!
	 */
	<T> T getUnderlyingPlatformDriver(Class<T> driver_class, Optional<String> driver_options);
}
