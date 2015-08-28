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

import java.util.List;

import scala.Tuple2;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;

/** The interface for the storage service
 * @author alex
 */
public interface IStorageService extends IUnderlyingService, IDataServiceProvider {

	public static final String BUCKET_SUFFIX = "/managed_bucket/"; 
	public static final String STORED_DATA_SUFFIX = "/managed_bucket/import/stored/"; 
	public static final String ANALYTICS_TEMP_DATA_SUFFIX = "/managed_bucket/import/analytics/temp/"; // (then name) 
	
	/** Validate the schema for this service
	 * @param schema - the schema to validate
	 * @return firstly the storage signature for this bucket, then a list of errors, empty if none
	 */
	Tuple2<String, List<BasicMessageBean>> validateSchema(final DataSchemaBean.StorageSchemaBean schema, final DataBucketBean bucket);
	
	/** Returns the root path for all Aleph2 DB related activities
	 * @return the root path, in a URI that is supported by the underlying file system (see getUnderlyingPlatformDriver)
	 */
	String getRootPath();
}
