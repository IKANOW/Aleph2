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
package com.ikanow.aleph2.data_import_manager.harvest.actors;

import java.util.Optional;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_import_manager.harvest.utils.HarvestErrorUtils;
import com.ikanow.aleph2.data_import_manager.harvest.utils.HostInformationUtils;
import com.ikanow.aleph2.data_import_manager.services.DataImportManagerActorContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionOfferMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.NewBucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionIgnoredMessage;

import fj.Unit;
import fj.data.Either;
import scala.PartialFunction;
import scala.Tuple2;
import scala.runtime.BoxedUnit;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.japi.pf.ReceiveBuilder;

/** This actor is responsible for supervising the job of handling changes to data
 *  buckets on the "data import manager" end
 * @author acp
 */
public class DataBucketChangeActor extends AbstractActor {

	///////////////////////////////////////////

	// Services
	
	protected final DataImportManagerActorContext _context;
	protected final IManagementDbService _management_db;
	protected final ICoreDistributedServices _core_distributed_services;
	protected final ActorSystem _actor_system;
	
	/** The actor constructor - at some point all these things should be inserted by injection
	 */
	public DataBucketChangeActor() {
		_context = DataImportManagerActorContext.get(); 
		_core_distributed_services = _context.getDistributedServices();
		_actor_system = _core_distributed_services.getAkkaSystem();
		_management_db = _context.getServiceContext().getManagementDbService();
	}
	
	///////////////////////////////////////////

	// Stateless actor
	
	 /* (non-Javadoc)
	 * @see akka.actor.AbstractActor#receive()
	 */
	@Override
	 public PartialFunction<Object, BoxedUnit> receive() {
	    return ReceiveBuilder
	    		.match(BucketActionOfferMessage.class, 
		    		m -> {
		    			Either<BasicMessageBean, Tuple2<DataBucketBean, SharedLibraryBean>> validated_bucket =
		    					Optional.of(m.bucket())
		    						.map(b -> validateBucket(b, this.self(), m, _management_db))
		    						.get(); // (never .empty() because of construction)

		    			validated_bucket.either(
		    					// Case 1: we received an error, just treat that like an ignore, this is an edge case anyway
		    					// since the bucket should have been validated before it's gotten to this point
		    					__ -> {
				    				this.sender().tell(new BucketActionIgnoredMessage(HostInformationUtils.getProcessUuid()),  this.sender());	
				    				return Unit.unit();
		    					},
		    					// Case 2: cache/get-the-cached harvest technology, load it into the classpath, check if we should handle or ignore
		    					vb -> {
		    						//TODO: see above!
				    				return Unit.unit();
		    					}
		    					);
		    			
		    			// 1) Handle the harvest technology cache
		    			//JarCacheUtils
		    		})
	    		.match(NewBucketActionMessage.class, 
		    		m -> {
		    			//TODO: handle a new bucket (similar to above but also need to bundle up the harvest technology
		    			//      modules + context, potentially create a streaming enrichment pipeline etc)
		    		})
		    	//TODO (ALEPH-19): other message types
	    		.build();
	 }
	
	/** Performs an initial sanity check of the bucket contents
	 * @param bucket - the bucket to check
	 * @return either an error encapsulated in a BasicMessageBean, or the tuple containing the original bucket and the shared library bean
	 */
	protected static Either<BasicMessageBean, Tuple2<DataBucketBean, SharedLibraryBean>> validateBucket(
			final @NonNull DataBucketBean bucket, final @NonNull ActorRef handler, final @NonNull BucketActionMessage message,
			final @NonNull IManagementDbService management_db)
	{
		if (null == bucket.harvest_technology_name_or_id()) {
			return Either.left(HarvestErrorUtils.buildErrorMessage(handler.toString(), message,
					HarvestErrorUtils.NO_TECHNOLOGY_NAME_OR_ID, bucket.full_name()
					));
		}
		try {
			final QueryComponent<SharedLibraryBean> query = CrudUtils.anyOf(SharedLibraryBean.class)
																.when(SharedLibraryBean::_id, bucket.harvest_technology_name_or_id())
																.when(SharedLibraryBean::path_name, bucket.harvest_technology_name_or_id());
										
			final Optional<SharedLibraryBean> shared_library = management_db.getSharedLibraryStore().getObjectBySpec(query).get();
			if (shared_library.isPresent()) {
				return Either.right(Tuples._2T(bucket, shared_library.get()));
			}
			else {
				return Either.left(
						HarvestErrorUtils.buildErrorMessage(handler.toString(), message,
								HarvestErrorUtils.HARVEST_TECHNOLOGY_NAME_NOT_FOUND, 
									bucket.harvest_technology_name_or_id(), bucket.full_name()
								)); 
			}
		}
		catch (Exception e) {
			return Either.left(
					HarvestErrorUtils.buildErrorMessage(handler.toString(), message, HarvestErrorUtils.getLongForm("{0}", e)));
		}
	}
	
}
