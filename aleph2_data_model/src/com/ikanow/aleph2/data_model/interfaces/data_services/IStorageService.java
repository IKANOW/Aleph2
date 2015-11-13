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
 *******************************************************************************/
package com.ikanow.aleph2.data_model.interfaces.data_services;

import java.util.List;
import java.util.Optional;

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

	/** Can be used with getUnderlyingPlatformDriver(FileContext, LOCAL_FS) to return a local FS (that knows about the remote one
	 *  ie can be safely used to pass data around) 
	 */
	public static final Optional<String> LOCAL_FS = Optional.of("local");
	
	/** The system is only interested in files under this sub-directory of each bucket path
	 */
	public static final String BUCKET_SUFFIX = "/managed_bucket/";
	
	/** This is the directory into which the harvester pushes files during batch enrichment
	 */
	public static final String TO_IMPORT_DATA_SUFFIX = "/managed_bucket/import/ready/";
	
	/** This is for temp data, eg for spooling
	 */
	public static final String TEMP_DATA_SUFFIX = "/managed_bucket/import/temp/";
	
	/** This is for transient data, eg for intermediate analytic steps - data is stored in TRANSIENT_DATA_SUFFIX_SECONDARY + <job_name>/<secondary_buffer-or-"current/">
	 *  (where secondary buffers are stored, has no primary string constant because of the job name infix)
	 */
	public static final String TRANSIENT_DATA_SUFFIX_SECONDARY = "/managed_bucket/import/transient/";
	
	/** This is the top level directory for data that has actually been processed
	 */
	public static final String STORED_DATA_SUFFIX = "/managed_bucket/import/stored/";
	
	/** This is the directory where the raw data is stored (ie copied from the /ready/ directory without being changed)
	 *  (Batch only)
	 */
	public static final String STORED_DATA_SUFFIX_RAW = "/managed_bucket/import/stored/raw/current/";
	
	/** This is the directory where the raw data is stored (ie copied from the /ready/ directory without being changed)
	 *  (where secondary buffers are stored)
	 *  (Batch only)
	 */
	public static final String STORED_DATA_SUFFIX_RAW_SECONDARY = "/managed_bucket/import/stored/raw/";
	
	/** For non JSON input files (CSV/binary), the data immediately after it has been converted to JSON but before any other enrichment has occurred  
	 *  (It is not expected that this directory will be commonly used)
	 *  (Batch only)
	 */
	public static final String STORED_DATA_SUFFIX_JSON = "/managed_bucket/import/stored/json/current/";
	
	/** For non JSON input files (CSV/binary), the data immediately after it has been converted to JSON but before any other enrichment has occurred  
	 *  (It is not expected that this directory will be commonly used)
	 *  (where secondary buffers are stored)
	 *  (Batch only)
	 */
	public static final String STORED_DATA_SUFFIX_JSON_SECONDARY = "/managed_bucket/import/stored/json/";
	
	/** Data from batch or streaming data after all enrichment has occurred 
	 */
	public static final String STORED_DATA_SUFFIX_PROCESSED = "/managed_bucket/import/stored/processed/current/";
	
	/** Data from batch or streaming data after all enrichment has occurred (where secondary buffers are stored) 
	 */
	public static final String STORED_DATA_SUFFIX_PROCESSED_SECONDARY = "/managed_bucket/import/stored/processed/";
	
	/** This suffix is where data is placed if there are no temporal considerations
	 */
	public static final String NO_TIME_SUFFIX = "/all_time/";
		
	/** The suffix (included in other constants unless otherwise specified) that points to the current set of data 
	 *  where there are multiple buffers (eg for ping/pong type operations)
	 */
	public static final String PRIMARY_BUFFER_SUFFIX = "/current/";
	
	/** A recommended secondary name for the optional "original primary" if it is moved to the secondary  
	 */
	public static final String EX_PRIMARY_BUFFER_SUFFIX = "/former_current/";
	
	/** The storage service  retains data from 0-3 different stages of the processing:
	 *  "raw" - is the unprocessed data, "json" is the data after serialization but otherwise untouched (eg before enrichment/analytics), "processed" is post-processing/analytics
	 *  "transient_output" - for analytic engines, temporary storage shared between jobs within an analytic thread (and with external analytic threads)
	 */
	public enum StorageStage { raw, json, processed, transient_output };
	
	/** Validate the schema for this service
	 * @param schema - the schema to validate
	 * @return firstly the storage signature for this bucket, then a list of errors, empty if none
	 */
	Tuple2<String, List<BasicMessageBean>> validateSchema(final DataSchemaBean.StorageSchemaBean schema, final DataBucketBean bucket);
	
	/** Returns the root path for all Aleph2 DB related activities (use getBucketRootPath for the root of all bucket directories) 
	 * @return the root path, in a URI that is supported by the underlying file system (see getUnderlyingPlatformDriver)
	 */
	String getRootPath();
	
	/** Returns the root path for all Aleph2 DB _bucket_ related activities (use getRootPath for the higher level) 
	 * @return the root path, in a URI that is supported by the underlying file system (see getUnderlyingPlatformDriver)
	 */
	String getBucketRootPath();
}
