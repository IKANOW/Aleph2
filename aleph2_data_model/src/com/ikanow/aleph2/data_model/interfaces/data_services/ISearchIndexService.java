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
package com.ikanow.aleph2.data_model.interfaces.data_services;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

/** The interface for the search index service
 * @author alex
 */
public interface ISearchIndexService {
	
	/** Validate the schema for this service
	 * @param schema - the schema to validate
	 * @return a list of errors, empty if none
	 */
	List<BasicMessageBean> validateSchema(final DataSchemaBean.SearchIndexSchemaBean schema, final DataBucketBean bucket);	

	/** Returns a CRUD service (including simple searching) for the specified bucket or multi-bucket
	 * @param clazz The class of the bean or object type desired (needed so the repo can reason about the type when deciding on optimizations etc)
	 * @param bucket The data bucket or multi-bucket
	 * @return the CRUD service
	 */
	<O> ICrudService<O> getCrudService(final Class<O> clazz, final DataBucketBean bucket);
	
	/** Returns a CRUD service (including simple searching) for the specified buckets or multi-buckets
	 * @param clazz The class of the bean or object type desired (needed so the repo can reason about the type when deciding on optimizations etc)
	 * @param buckets A collection of bucket paths or aliases (including supporting wildcarding)
	 * @return the CRUD service
	 */
	<O> ICrudService<O> getCrudService(final Class<O> clazz, final Collection<String> buckets);
	
	/** USE WITH CARE: this returns the driver to the underlying technology
	 *  shouldn't be used unless absolutely necessary!
	 * @param driver_class the class of the driver
	 * @param a string containing options in some technology-specific format
	 * @return a driver to the underlying technology. Will exception if you pick the wrong one!
	 */
	<T> Optional<T> getUnderlyingPlatformDriver(final Class<T> driver_class, final Optional<String> driver_options);
}
