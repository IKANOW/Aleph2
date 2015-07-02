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
package com.ikanow.aleph2.data_import_manager.stream_enrichment.actors;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;



import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



import scala.PartialFunction;
import scala.Tuple2;
import scala.runtime.BoxedUnit;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.japi.pf.ReceiveBuilder;



import com.ikanow.aleph2.data_import_manager.harvest.utils.HarvestErrorUtils;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.utils.ClassloaderUtils;
import com.ikanow.aleph2.data_import_manager.utils.JarCacheUtils;
import com.ikanow.aleph2.data_import_manager.utils.StormControllerUtil;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestTechnologyModule;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.MethodNamingHelper;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionOfferMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionHandlerMessage;



import fj.data.Validation;

/** This actor is responsible for supervising the job of handling changes to data
 *  buckets on the "data import manager" end - specifically vs streaming enrichment (see harvest.DataBucketChangeActor for harvest related control)
 * @author acp
 */
@SuppressWarnings("unused")
public class DataBucketChangeActor extends AbstractActor {
	private static final Logger _logger = LogManager.getLogger();	
	
	///////////////////////////////////////////

	// Services
	
	protected final DataImportActorContext _context;
	protected final IManagementDbService _management_db;
	protected final ICoreDistributedServices _core_distributed_services;
	protected final ActorSystem _actor_system;
	protected final GlobalPropertiesBean _globals;
	protected final IStorageService _fs;
	
	/** The actor constructor - at some point all these things should be inserted by injection
	 */
	public DataBucketChangeActor() {
		_context = DataImportActorContext.get(); 
		_core_distributed_services = _context.getDistributedServices();
		_actor_system = _core_distributed_services.getAkkaSystem();
		_management_db = _context.getServiceContext().getCoreManagementDbService();
		_globals = _context.getGlobalProperties();
		_fs = _context.getServiceContext().getStorageService();
	}
	
	///////////////////////////////////////////

	// Stateless actor
	
	 /* (non-Javadoc)
	 * @see akka.actor.AbstractActor#receive()
	 */
	@Override
	 public PartialFunction<Object, BoxedUnit> receive() {
	    return ReceiveBuilder
	    		.match(BucketActionMessage.class, 
	    				m -> !m.handling_clients().isEmpty() && !m.handling_clients().contains(_context.getInformationService().getHostname()),
	    				__ -> {}) // (do nothing if it's not for me)
	    		.match(BucketActionOfferMessage.class,
	    			m -> {
		    			_logger.info(ErrorUtils.get("Actor {0} received offer message {1} from {2}", this.self(), m.getClass().getSimpleName(), this.sender()));

		    			final ActorRef closing_sender = this.sender();
		    			final ActorRef closing_self = this.self();		    			
						
	    				final String hostname = _context.getInformationService().getHostname();
		    			
						// (this isn't async so doesn't require any futures)
						
	    				//TODO: check if STORM is available here (in practice shouldn't register vs message bus if not, but doesn't hurt to ask)
						final boolean accept_or_ignore = true;
						
						final BucketActionReplyMessage reply = 						
							accept_or_ignore
									? new BucketActionReplyMessage.BucketActionWillAcceptMessage(hostname)
									: new BucketActionReplyMessage.BucketActionIgnoredMessage(hostname);
									
						closing_sender.tell(reply,  closing_self);
	    			})
	    		.match(BucketActionMessage.class, 
		    		m -> {
		    			_logger.info(ErrorUtils.get("Actor {0} received message {1} from {2}", this.self(), m.getClass().getSimpleName(), this.sender()));
		    			
		    			final ActorRef closing_sender = this.sender();
		    			final ActorRef closing_self = this.self();
		    					    			
	    				final String hostname = _context.getInformationService().getHostname();
	    				
	    				//TODO more stuff
	    				DataBucketChangeActor.talkToStream(m.bucket(), m, _globals.local_yarn_config_dir());
	    				
	    				//TODO: create future in which you attempt to launch STORM, eg ending
	    				// thenAccept(reply -> {
	    				//	closing_sender.tell(reply,  closing_self);
	    				// })
    					//.exceptionally(e -> { // another bit of error handling that shouldn't ever be called but is a useful backstop
		    			//	final BasicMessageBean error_bean = 
		    			//			StreamErrorUtils.buildErrorMessage(hostname, m,
		    			//					ErrorUtils.getLongForm(StreamErrorUtils.HARVEST_UNKNOWN_ERROR, e, m.bucket().full_name())
		    			//					);
		    			//	closing_sender.tell(new BucketActionHandlerMessage(hostname, error_bean), closing_self);			    				
    					//	return null;
    					//})
	    				
	    				// For now just error:
	    				//TODO: remove when done:
		    			final BasicMessageBean error_bean = 
		    						HarvestErrorUtils.buildErrorMessage(hostname, m,
		    								ErrorUtils.get("Not yet implemented: {0}", m.bucket().full_name())
		    								);
		    			closing_sender.tell(new BucketActionHandlerMessage(hostname, error_bean), closing_self);			    				
		    		})
	    		.build();
	 }
	
	////////////////////////////////////////////////////////////////////////////
	
	// Functional code

	//TODO:	
	protected static void talkToStream(
			final DataBucketBean bucket, 
			final BucketActionMessage m, String yarn_config_dir
			)
	{
		Patterns.match(m).andAct()
			.when(BucketActionMessage.BucketActionOfferMessage.class, msg -> {
				
			})
			.when(BucketActionMessage.DeleteBucketActionMessage.class, msg -> {
				StormControllerUtil.stopJob( bucket, yarn_config_dir);
			})
			.when(BucketActionMessage.NewBucketActionMessage.class, msg -> {
				StormControllerUtil.startJob(bucket, yarn_config_dir);
			})
			.when(BucketActionMessage.UpdateBucketActionMessage.class, msg -> {
				StormControllerUtil.restartJob(bucket, yarn_config_dir);
			})
			.when(BucketActionMessage.UpdateBucketStateActionMessage.class, msg -> {
				if ( msg.is_suspended() )
					StormControllerUtil.stopJob(bucket, yarn_config_dir);
				else
					StormControllerUtil.startJob(bucket, yarn_config_dir);
			})
			.otherwise(msg -> {
				
			});
	}
}
