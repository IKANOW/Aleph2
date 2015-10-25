/*******************************************************************************
* Copyright 2015, The IKANOW Open Source Project.
* 
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
* 
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Affero General Public License for more details.
* 
* You should have received a copy of the GNU Affero General Public License
* along with this program. If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
package com.ikanow.aleph2.data_import_manager.analytics.services;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

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
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Patterns;
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
	
	protected IServiceContext _service_context;
	protected FileContext _file_context;
	
	/** User c'tor
	 */
	public AnalyticStateTriggerCheckFactory() {}
	
	/** Guice c'tor
	 */
	@Inject
	public AnalyticStateTriggerCheckFactory(IServiceContext service_context) {
		_service_context = service_context;
	}
	
	/** Interface for performing the trigger check
	 * @author Alex
	 */
	public static interface AnalyticStateChecker {
		CompletableFuture<Tuple2<Boolean, Long>> check(final DataBucketBean bucket, final Optional<AnalyticThreadJobBean> job, final AnalyticTriggerStateBean trigger);
	}
	
	/** Gets the checker for the trigger/data service pair
	 * @param trigger_type
	 * @param data_service
	 * @return
	 */
	public AnalyticStateChecker getChecker(final TriggerType trigger_type, final Optional<String> data_service) {
		return Patterns.match().<AnalyticStateChecker>andReturn()
				.when(__ -> TriggerType.file == trigger_type, __ -> new InputFileChecker())
				.when(__ -> (TriggerType.bucket == trigger_type) && !data_service.isPresent(), __ -> new NeverChecker())
				.when(__ -> (TriggerType.bucket == trigger_type) && storage_service.equals(data_service), __ -> new BucketStorageChecker())
				.when(__ -> (TriggerType.bucket == trigger_type) && search_index_service.equals(data_service), __ -> new SearchIndexChecker())
				.otherwise(__ -> new NeverChecker()) // (not currently supported, eg custom)
				;
	}
	
	/** Checks if files are present in this bucket's input directory
	 *  TODO (ALEPH-12): hmm I think the idea was that this would also point to external
	 *  and then you'd use a bucket check
	 * @author alex
	 */
	protected class InputFileChecker implements AnalyticStateChecker {

		@Override
		public CompletableFuture<Tuple2<Boolean, Long>> check(
				DataBucketBean bucket, Optional<AnalyticThreadJobBean> job,
				AnalyticTriggerStateBean trigger)
		{
			try {
				if (null == _file_context) {
					_file_context = _service_context.getStorageService().getUnderlyingPlatformDriver(FileContext.class, Optional.empty()).get();
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
					
					if (_file_context.util().exists(path)) {						
						FileStatus[] status = _file_context.util().listStatus(path);				
						
						if (status.length > 0) {
							_logger.info(ErrorUtils.get("For bucket:job {0}:{1}, foound {2} files in bucket {3}", bucket.full_name(), 
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
	
	/** Checks the size of the search index service
	 * @author alex
	 */
	protected class SearchIndexChecker implements AnalyticStateChecker {

		@Override
		public CompletableFuture<Tuple2<Boolean, Long>> check(
				DataBucketBean bucket, Optional<AnalyticThreadJobBean> job,
				AnalyticTriggerStateBean trigger) {
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

		@Override
		public CompletableFuture<Tuple2<Boolean, Long>> check(
				DataBucketBean bucket, Optional<AnalyticThreadJobBean> job,
				AnalyticTriggerStateBean trigger) {
			// TODO Auto-generated method stub
			return CompletableFuture.completedFuture(Tuples._2T(false, trigger.curr_resource_size()));
		}
		
	}
	
	/** Always returns false
	 *  (eg for pure bucket/job checking, where the curr_resource_size is incremented by 
	 *   the triggering logic ... later on what would be nice to handle batch import buckets
	 *   via some status)
	 * @author alex
	 */
	protected class NeverChecker implements AnalyticStateChecker {
		
		@Override
		public CompletableFuture<Tuple2<Boolean, Long>> check(
				DataBucketBean bucket, Optional<AnalyticThreadJobBean> job,
				AnalyticTriggerStateBean trigger)
		{
			return CompletableFuture.completedFuture(Tuples._2T(false, trigger.curr_resource_size()));
		}		
	}
}
