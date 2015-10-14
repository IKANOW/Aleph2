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
package com.ikanow.aleph2.management_db.controllers.actors;

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.concurrent.duration.Duration;
import akka.actor.Cancellable;
import akka.actor.UntypedActor;

import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ISecurityService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;

/** 
 * @author jfreydank
 */
public class SecurityCacheInvalidationSingletonActor extends UntypedActor {
    private static final Logger _logger = LogManager.getLogger(SecurityCacheInvalidationSingletonActor.class);

	protected GlobalPropertiesBean _global_properties_Bean = null;
	protected IServiceContext serviceContext = null; 

	public static String MSG_START = "start";
	public static String MSG_STOP = "stop";
	public static String MSG_MONITOR_ACTION = "action";
	protected final ManagementDbActorContext _actor_context;
	protected final IServiceContext _context;
	protected final IManagementDbService _core_management_db;
	
	protected final SetOnce<Cancellable> _ticker = new SetOnce<>();
	protected final ISecurityService _securityService;
	
	/** Akka c'tor
	 */
	public SecurityCacheInvalidationSingletonActor() {	

	    _actor_context = ManagementDbActorContext.get();
		
		_context = _actor_context.getServiceContext();
		_core_management_db = Lambdas.get(() -> { try { return _context.getCoreManagementDbService(); } catch (Exception e) { return null; } });
		_securityService = _context.getSecurityService();
		
		if (null != _core_management_db) {
			_ticker.set(getContext()
			.system()
			.scheduler()
			.schedule(Duration.create(1000, TimeUnit.MILLISECONDS),
					Duration.create(8000, TimeUnit.MILLISECONDS), getSelf(),
					MSG_MONITOR_ACTION, getContext().dispatcher(), null));				
			
			_logger.info("SecurityCacheInvalidationSingletonActor has started on this node.");						
		}		
	}
    
	
    
	@Override
	public void postStop() {
		_logger.debug("postStop");
		if ( _ticker.isSet()) {
			_ticker.get().cancel();
		}
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (MSG_MONITOR_ACTION.equals(message)) {
			_logger.debug("action message received");
			if(checkInvalidation()){
				_securityService.invalidateCache();
			}
		}
		 else {
				_logger.debug("unhandeld message:"+message);
			unhandled(message);
		}
	}



	protected boolean checkInvalidation() {
		_logger.debug("checkInvalidation");
		return _securityService.isCacheInvalid();
	}

}
