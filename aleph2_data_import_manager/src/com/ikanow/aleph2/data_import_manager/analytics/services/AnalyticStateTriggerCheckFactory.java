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
package com.ikanow.aleph2.data_import_manager.analytics.services;

import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;



import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean.AnalyticThreadComplexTriggerBean.TriggerType;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;



import scala.Tuple2;

/** Factory class for generating
 * @author Alex
 */
public class AnalyticStateTriggerCheckFactory {
	protected static final Logger _logger = LogManager.getLogger();	
	
	public final static Optional<String> storage_service = Optional.of("storage_service");
	public final static Optional<String> search_index_service = Optional.of("search_index_service");
	public final static Optional<String> document_service = Optional.of("document_service");
	
	protected final IServiceContext _service_context;
	protected final SetOnce<FileContext> _file_context = new SetOnce<>();
	
	protected final Map<String, DataBucketBean> _bucket_cache = new ConcurrentHashMap<>();
	protected final Map<String, DataBucketStatusBean> _bucket_status_cache = new ConcurrentHashMap<>();
	
	/** User c'tor
	 */
	public AnalyticStateTriggerCheckFactory() {
		_service_context = null;
	}
	
	/** Guice c'tor
	 */
	@Inject
	public AnalyticStateTriggerCheckFactory(IServiceContext service_context) {
		_service_context = service_context;
	}
	
	/** Clears the bucket and bucket status cache available across all the checkers
	 */
	public void resetCache() {
		_bucket_cache.clear();
		_bucket_status_cache.clear();
	}
	
	/** Retrieves/caches bucket
	 * @param name (bucket full_name)
	 * @return
	 */
	public DataBucketBean getBucket(final String name) {
		return _bucket_cache.computeIfAbsent(name, n -> {
			return _service_context.getCoreManagementDbService().readOnlyVersion().getDataBucketStore()
						.getObjectBySpec(CrudUtils.allOf(DataBucketBean.class).when(DataBucketBean::full_name, n))
						.join().orElse(null)
			;
		});
	}
	/** Retrieves/caches bucket status
	 * @param name (bucket full_name)
	 * @return
	 */
	public DataBucketStatusBean getBucketStatus(final String name) {
		return _bucket_status_cache.computeIfAbsent(name, n -> {
			return _service_context.getCoreManagementDbService().readOnlyVersion().getDataBucketStatusStore()
						.getObjectBySpec(CrudUtils.allOf(DataBucketStatusBean.class).when(DataBucketStatusBean::bucket_path, n))
						.join().orElse(null)
			;
		});
	}
	
	/** Interface for performing the trigger check
	 * @author Alex
	 */
	public static interface AnalyticStateChecker {
		/** Interface for checking if a given trigger is satisfied
		 * @param bucket - the bucket to check
		 * @param job - if applicable, the job being checked
		 * @param trigger - the trigger metadata
		 * @param at - the time to check against (ie now - this is passed in to ensure that a single global "now" is used across all operations)
		 * @return - tuple: _1 is whether this trigger is now active, _2 is the triggering resource
		 */
		CompletableFuture<Tuple2<Boolean, Long>> check(final DataBucketBean bucket, final Optional<AnalyticThreadJobBean> job, final AnalyticTriggerStateBean trigger, final Date at);
	}
	
	/** Gets the checker for the trigger/data service pair
	 * @param trigger_type
	 * @param data_service
	 * @return
	 */
	public AnalyticStateChecker getChecker(final TriggerType trigger_type, final Optional<String> data_service) {
		return Patterns.match().<AnalyticStateChecker>andReturn()
				.when(__ -> TriggerType.file == trigger_type, __ -> new InputFileChecker())
				.when(__ -> TriggerType.time == trigger_type, __ -> new AlwaysChecker())
				.when(__ -> (TriggerType.bucket == trigger_type) && !data_service.isPresent(), __ -> new NeverChecker())
				.when(__ -> (TriggerType.bucket == trigger_type) && storage_service.equals(data_service), __ -> new BucketStorageChecker())
				.when(__ -> (TriggerType.bucket == trigger_type) && search_index_service.equals(data_service), __ -> new CrudChecker())
				.when(__ -> (TriggerType.bucket == trigger_type) && document_service.equals(data_service), __ -> new CrudChecker())
				.otherwise(__ -> new NeverChecker()) // (not currently supported, eg custom)
				;
	}
	
	/** Checks if files are present in this bucket's input directory
	 *  TODO (ALEPH-12): hmm I think the idea was that this would also point to external
	 *  and then you'd use a bucket check
	 * @author alex
	 */
	protected class InputFileChecker implements AnalyticStateChecker {

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_import_manager.analytics.services.AnalyticStateTriggerCheckFactory.AnalyticStateChecker#check(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean, java.util.Date)
		 */
		@Override
		public CompletableFuture<Tuple2<Boolean, Long>> check(
				DataBucketBean bucket, Optional<AnalyticThreadJobBean> job,
				AnalyticTriggerStateBean trigger, final Date at)
		{
			try {
				if (!_file_context.isSet()) {
					_file_context.set(_service_context.getStorageService().getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get());
				}
				
				// Count the files
				
				//TODO (ALEPH-12): need to check if this is permitted by security in general (the input paths are pre-checked though)
				
				final long files = Lambdas.get(Lambdas.wrap_u(() -> {
				
					final String path_name = 
							_service_context.getStorageService().getBucketRootPath() 
								+ trigger.input_resource_name_or_id()
								+ IStorageService.TO_IMPORT_DATA_SUFFIX
								;
					
					final Path path = new Path(path_name);
					
					if (_file_context.get().util().exists(path)) {						
						FileStatus[] status = _file_context.get().util().listStatus(path);				
						
						if (status.length > 0) {
							_logger.info(ErrorUtils.get("For bucket:job {0}:{1}, found {2} files in bucket {3}", bucket.full_name(), 
									job.map(j -> j.name()).orElse("(no job)"),
									status.length, trigger.input_resource_name_or_id()
									));
						}
						
						return (long) status.length;
					}
					else return 0L;
				}));
				return CompletableFuture.completedFuture(Tuples._2T(files > 0, files));
			}
			catch (Exception e) {
				if (_logger.isDebugEnabled()) _logger.debug(ErrorUtils.getLongForm("{0}", e)); 

				//DEBUG - leave this in for a while
				_logger.warn(ErrorUtils.getLongForm("{0}", e));
				
				return CompletableFuture.completedFuture(Tuples._2T(false, trigger.curr_resource_size()));
			}
		}		
	}
	
	/** Checks the size of CRUD-accessible services, like the search index service and document service
	 * @author alex
	 */
	protected class CrudChecker implements AnalyticStateChecker {

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_import_manager.analytics.services.AnalyticStateTriggerCheckFactory.AnalyticStateChecker#check(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean, java.util.Date)
		 */
		@Override
		public CompletableFuture<Tuple2<Boolean, Long>> check(
				DataBucketBean bucket, Optional<AnalyticThreadJobBean> job,
				AnalyticTriggerStateBean trigger, final Date at) {
			
			//TODO (ALEPH-12): implement this:
			return CompletableFuture.completedFuture(Tuples._2T(false, trigger.curr_resource_size()));
		}
		
	}
	
	//TODO (ALEPH-12): ugh there's a) <bucket>:raw|json|processed
	// and also b) <bucket>:<job-name> (ie transient storage .. hmm i think you want to do bucket for that?
	// and then also c) <bucket-import> and d) random files handled above
	
	/** Checks whether files are present in the bucket's output directory
	 * @author alex
	 */
	protected class BucketStorageChecker implements AnalyticStateChecker {

		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_import_manager.analytics.services.AnalyticStateTriggerCheckFactory.AnalyticStateChecker#check(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean, java.util.Date)
		 */
		@Override
		public CompletableFuture<Tuple2<Boolean, Long>> check(
				DataBucketBean bucket, Optional<AnalyticThreadJobBean> job,
				AnalyticTriggerStateBean trigger, final Date at) {
			
			
			// OK there's a few things going on here
			// 1) need to distinguish between raw/json/processed
			// 2) search the first level of directories for time > now
			// 3) within each matching dir, search for files with time > now
			// 4) sum size of files
			
			//TODO: what about multi-buckets? could in theory support
			
			// TODO Auto-generated method stub
			return CompletableFuture.completedFuture(Tuples._2T(false, trigger.curr_resource_size()));
		}
		
	}
	
	/** Always returns rue
	 *  Used for pure time triggers
	 * @author Alex
	 *
	 */
	protected class AlwaysChecker implements AnalyticStateChecker {
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_import_manager.analytics.services.AnalyticStateTriggerCheckFactory.AnalyticStateChecker#check(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean, java.util.Date)
		 */
		@Override
		public CompletableFuture<Tuple2<Boolean, Long>> check(
				DataBucketBean bucket, Optional<AnalyticThreadJobBean> job,
				AnalyticTriggerStateBean trigger, final Date at)
		{
			return CompletableFuture.completedFuture(Tuples._2T(true, trigger.curr_resource_size()));
		}		
	}
	
	/** Always returns false
	 *  (eg for pure bucket/job checking, where the curr_resource_size is incremented by 
	 *   the triggering logic ... later on what would be nice to handle batch import buckets
	 *   via some status)
	 * @author alex
	 */
	protected class NeverChecker implements AnalyticStateChecker {
		
		/* (non-Javadoc)
		 * @see com.ikanow.aleph2.data_import_manager.analytics.services.AnalyticStateTriggerCheckFactory.AnalyticStateChecker#check(com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean, java.util.Optional, com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean, java.util.Date)
		 */
		@Override
		public CompletableFuture<Tuple2<Boolean, Long>> check(
				DataBucketBean bucket, Optional<AnalyticThreadJobBean> job,
				AnalyticTriggerStateBean trigger, final Date at)
		{
			return CompletableFuture.completedFuture(Tuples._2T(false, trigger.curr_resource_size()));
		}		
	}
}
