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

	/** The system is only interested in files under this sub-directory of each bucket path
	 */
	public static final String BUCKET_SUFFIX = "/managed_bucket/";
	
	/** This is the directory into which the harvester pushes files during batch enrichment
	 */
	public static final String TO_IMPORT_DATA_SUFFIX = "/managed_bucket/import/ready/";
	
	/** This is for temp data, eg for spooling
	 */
	public static final String TEMP_DATA_SUFFIX = "/managed_bucket/import/temp/";
	
	/** This is the top level directory for data that has actually been processed
	 */
	public static final String STORED_DATA_SUFFIX = "/managed_bucket/import/stored/";
	
	/** This is the directory where the raw data is stored (ie copied from the /ready/ directory without being changed)
	 *  (Batch only)
	 */
	public static final String STORED_DATA_SUFFIX_RAW = "/managed_bucket/import/stored/raw/";
	
	/** For non JSON input files (CSV/binary), the data immediately after it has been converted to JSON but before any other enrichment has occurred  
	 *  (It is not expected that this directory will be commonly used)
	 *  (Batch only)
	 */
	public static final String STORED_DATA_SUFFIX_JSON = "/managed_bucket/import/stored/json/";
	
	/** Data from batch or streaming data after all enrichment has occurred 
	 */
	public static final String STORED_DATA_SUFFIX_PROCESSED = "/managed_bucket/import/stored/processed/";
	
	/** For analytics buckets, the output data of intermediate jobs is stored here, under the name of the job
	 *  (or the content UUID of the job if no name is specified)
	 */
	public static final String ANALYTICS_TEMP_DATA_SUFFIX = "/managed_bucket/analytics/temp/"; // (then name)
	
	/** The storage service  retains data from 0-3 different stages of the processing:
	 *  "raw" - is the unprocessed data, "json" is the data after serialization but otherwise untouched (eg before enrichment/analytics), "processed" is post-processing/analytics
	 */
	public enum StorageStage { raw, json, processed };
	
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
