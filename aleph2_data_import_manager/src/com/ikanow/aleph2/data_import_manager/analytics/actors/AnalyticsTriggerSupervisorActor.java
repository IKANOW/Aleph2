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
package com.ikanow.aleph2.data_import_manager.analytics.actors;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import com.ikanow.aleph2.data_import_manager.analytics.utils.AnalyticTriggerCrudUtils;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage.AnalyticsTriggerEventBusWrapper;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerStateBean;
import com.ikanow.aleph2.management_db.services.ManagementDbActorContext;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.UntypedActor;
import akka.event.japi.LookupEventBus;

/** Supervisor actor - just responsible for distributing scheduled tasks out across the workers
 * @author Alex
 *
 */
public class AnalyticsTriggerSupervisorActor extends UntypedActor {
	protected static final Logger _logger = LogManager.getLogger();
	

	protected final ManagementDbActorContext _actor_context;
	protected final SetOnce<Cancellable> _ticker = new SetOnce<>();
	
	protected final LookupEventBus<AnalyticsTriggerEventBusWrapper, ActorRef, String> _analytics_trigger_bus;
	
	// These aren't currently used, but might want to make the logic more sophisticated in the future 
	protected final IServiceContext _context;
	protected final IManagementDbService _core_management_db;
	protected final SetOnce<ICrudService<AnalyticTriggerStateBean>> _analytics_trigger_state = new SetOnce<>();
	
	protected final AtomicLong _ping_count = new AtomicLong(0);
	
	/** Akka c'tor
	 */
	public AnalyticsTriggerSupervisorActor() {		
		_actor_context = ManagementDbActorContext.get();
		_analytics_trigger_bus = _actor_context.getAnalyticsTriggerBus();
		
		_context = _actor_context.getServiceContext();
		_core_management_db = Lambdas.get(() -> { 
			try { 
				return _context.getCoreManagementDbService(); 
			} 
			catch (Throwable e) { 
				_logger.warn(ErrorUtils.getLongForm("Failed to load core management db service: {0}", e));
				return null; 
			} 
		});

		if (null != _core_management_db) {
			final FiniteDuration poll_delay = Duration.create(1, TimeUnit.SECONDS);
			final FiniteDuration poll_frequency = Duration.create(5, TimeUnit.SECONDS);
			_ticker.set(this.context().system().scheduler()
						.schedule(poll_delay, poll_frequency, this.self(), "Tick", this.context().system().dispatcher(), null));
			
			_logger.info("AnalyticsTriggerSupervisorActor has started on this node.");						
		}		
	}
	
	/** For some reason can run into guice problems with doing this in the c'tor
	 *  so do it here instead
	 */
	protected void setup() {
		try {
			if ((null == _core_management_db) || (_analytics_trigger_state.isSet())) {
				return;
			}
			_analytics_trigger_state.set(_core_management_db.getAnalyticBucketTriggerState(AnalyticTriggerStateBean.class));
	
			// ensure analytic queue is optimized:
			AnalyticTriggerCrudUtils.optimizeQueries(_analytics_trigger_state.get());
			
			_logger.info("Initialized AnalyticsTriggerSupervisorActor");
		}
		catch (Throwable t) {
			_logger.error(ErrorUtils.getLongForm("AnalyticsTriggerSupervisorActor initialize error: {0}", t));									
		}
	}
	
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
	 */
	@Override
	public void onReceive(Object message) throws Exception {
		setup();
		
		final ActorRef self = this.self();
		if (String.class.isAssignableFrom(message.getClass())) { // tick!
			_ping_count.incrementAndGet();
			
			final AnalyticTriggerMessage msg = new AnalyticTriggerMessage(new AnalyticTriggerMessage.AnalyticsTriggerActionMessage());
			
			//TODO (ALEPH-12): at some point add a slower-time "resync" in case lost messages result in a gradual loss of state consistency 
			
			// Send a message to a worker:
			_analytics_trigger_bus.publish(new AnalyticTriggerMessage.AnalyticsTriggerEventBusWrapper(self, msg));
		}
	}
	
	/* (non-Javadoc)
	 * @see akka.actor.UntypedActor#postStop()
	 */
	@Override
	public void postStop() {
		if (_ticker.isSet()) {
			_ticker.get().cancel();
		}
		_logger.info("AnalyticsTriggerSupervisorActor has stopped on this node, processed " + _ping_count.get() + " pings");								
	}
}
