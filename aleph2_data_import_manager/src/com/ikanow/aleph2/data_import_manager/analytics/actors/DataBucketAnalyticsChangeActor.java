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
package com.ikanow.aleph2.data_import_manager.analytics.actors;

import java.io.File;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;

import org.apache.logging.log4j.Level;

import scala.PartialFunction;
import scala.Tuple2;
import scala.runtime.BoxedUnit;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.japi.pf.ReceiveBuilder;

import com.codepoetics.protonpack.StreamUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.ikanow.aleph2.analytics.services.AnalyticsContext;
import com.ikanow.aleph2.core.shared.utils.SharedErrorUtils;
import com.ikanow.aleph2.data_import_manager.analytics.utils.AnalyticsErrorUtils;
import com.ikanow.aleph2.data_import_manager.data_model.DataImportConfigurationBean;
import com.ikanow.aleph2.data_import_manager.harvest.actors.DataBucketHarvestChangeActor;
import com.ikanow.aleph2.data_import_manager.harvest.utils.HarvestErrorUtils;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.utils.ActorNameUtils;
import com.ikanow.aleph2.data_import_manager.utils.LibraryCacheUtils;
import com.ikanow.aleph2.data_import_manager.utils.NodeRuleUtils;
import com.ikanow.aleph2.core.shared.utils.ClassloaderUtils;
import com.ikanow.aleph2.core.shared.utils.JarCacheUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsContext;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyService;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentStreamingModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentStreamingTopology;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestTechnologyModule;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ILoggingService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadTriggerBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean.MasterEnrichmentType;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Lambdas;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.SetOnce;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.MethodNamingHelper;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.management_db.data_model.AnalyticTriggerMessage;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionOfferMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionAnalyticJobMessage.JobMessageType;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionHandlerMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionCollectedRepliesMessage;

import fj.Unit;
import fj.data.Either;
import fj.data.Validation;

/** This actor is responsible for supervising the job of handling changes to data
 *  buckets on the "data import manager" end - specifically vs streaming enrichment (see harvest.DataBucketChangeActor for harvest related control)
 * @author acp
 */
@SuppressWarnings("unused")
public class DataBucketAnalyticsChangeActor extends AbstractActor {
//	private static final Logger _logger = LogManager.getSystemLogger();	
	
	///////////////////////////////////////////

	private static final EnumSet<MasterEnrichmentType> _streaming_types = EnumSet.of(MasterEnrichmentType.streaming, MasterEnrichmentType.streaming_and_batch);	
	private static final EnumSet<MasterEnrichmentType> _batch_types = EnumSet.of(MasterEnrichmentType.batch, MasterEnrichmentType.streaming_and_batch);		
	
	// Services
	protected final DataImportActorContext _context;
	protected final IManagementDbService _management_db;
	protected final ICoreDistributedServices _core_distributed_services;
	protected final ActorSystem _actor_system;
	protected final GlobalPropertiesBean _globals;
	protected final IStorageService _fs;
	protected final ILoggingService _logging_service;
	protected final Optional<IAnalyticsTechnologyService> _stream_analytics_tech;
	protected final Optional<IAnalyticsTechnologyService> _batch_analytics_tech;
	protected final SetOnce<AnalyticsContext> _stream_analytics_context = new SetOnce<>();
	protected final SetOnce<AnalyticsContext> _batch_analytics_context = new SetOnce<>();
	protected final ActorSelection _trigger_sibling; 
	
	// Streaming enrichment handling:
	public static final Optional<String> STREAMING_ENRICHMENT_DEFAULT = Optional.of("StreamingEnrichmentService");
	public static final Optional<String> BATCH_ENRICHMENT_DEFAULT = Optional.of("BatchEnrichmentService");
	public static final String STREAMING_ENRICHMENT_TECH_NAME = STREAMING_ENRICHMENT_DEFAULT.get();
	public static final String BATCH_ENRICHMENT_TECH_NAME = BATCH_ENRICHMENT_DEFAULT.get();
	
	/** The actor constructor - at some point all these things should be inserted by injection
	 */
	public DataBucketAnalyticsChangeActor() {
		_context = DataImportActorContext.get(); 
		_core_distributed_services = _context.getDistributedServices();
		_actor_system = _core_distributed_services.getAkkaSystem();
		_management_db = _context.getServiceContext().getCoreManagementDbService().readOnlyVersion();
		_globals = _context.getGlobalProperties();
		_fs = _context.getServiceContext().getStorageService();
		_logging_service = _context.getServiceContext().getService(ILoggingService.class, Optional.empty()).get();
		_stream_analytics_tech = _context.getServiceContext().getService(IAnalyticsTechnologyService.class, STREAMING_ENRICHMENT_DEFAULT);
		_batch_analytics_tech = _context.getServiceContext().getService(IAnalyticsTechnologyService.class, BATCH_ENRICHMENT_DEFAULT);
		
		// My local analytics trigger engine:
		_trigger_sibling = _actor_system.actorSelection("/user/" + _context.getInformationService().getHostname() + ActorNameUtils.ANALYTICS_TRIGGER_WORKER_SUFFIX);
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
	    			m -> isEnrichmentRequest(m),
	    			m -> {
	    				// Streaming enrichment special case:
	    				if (!_stream_analytics_context.isSet()) {
	    					_stream_analytics_context.trySet(_context.getNewAnalyticsContext());
	    					_batch_analytics_context.trySet(_stream_analytics_context.get());
	    				}	    				
	    				
	    				if ( shouldLog(m))
	    					_logging_service.getSystemLogger(m.bucket()).log(Level.INFO, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"receive", ()->null, ()->ErrorUtils.get("Actor {0} received message {1} from {2} bucket {3}", this.self(), m.getClass().getSimpleName(), this.sender(), m.bucket().full_name()), ()->Collections.emptyMap()));	    							

		    			final ActorRef closing_sender = this.sender();
		    			final ActorRef closing_self = this.self();		    			
						
	    				final String hostname = _context.getInformationService().getHostname();
		    			
						// (this isn't async so doesn't require any futures)
						
	    				final boolean accept_or_ignore = 
	    	    				isEnrichmentType(m.bucket()) // ie has to be one of the 2
	    	    				&&
	    						(!isStreamingEnrichmentType(m.bucket()) // not streaming or streaming enabled
	    								||
	    								_stream_analytics_tech.map(tech -> {
	    									tech.onInit(_stream_analytics_context.get());
	    									return tech.canRunOnThisNode(m.bucket(), Collections.emptyList(), _stream_analytics_context.get());
	    								})
	    								.orElseGet(() -> { // (shouldn't ever happen because shouldn't register itself on the listen bus unless available to handle requests)
	    									_logging_service.getSystemLogger(m.bucket()).log(Level.WARN, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"receive", ()->null, ()->ErrorUtils.get("Actor {0} received streaming enrichment offer for {1} but it is not configured on this node", this.self(), m.bucket().full_name()), ()->Collections.emptyMap()));
	    									return false;
	    								})
	    						)
	    						&&
	    						(!isBatchEnrichmentType(m.bucket()) // not batch or batch enabled
	    	    						||
	    	    						_batch_analytics_tech.map(tech -> {
	    	    							tech.onInit(_batch_analytics_context.get());
	    	    							return tech.canRunOnThisNode(m.bucket(), Collections.emptyList(), _batch_analytics_context.get());		    					 
	    	    						})
	    								.orElseGet(() -> { // (shouldn't ever happen because shouldn't register itself on the listen bus unless available to handle requests)
	    									_logging_service.getSystemLogger(m.bucket()).log(Level.WARN, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"receive", ()->null, ()->ErrorUtils.get("Actor {0} received batch enrichment offer for {1} but it is not configured on this node", this.self(), m.bucket().full_name()), ()->Collections.emptyMap()));
	    	    							return false;
	    	    						})
	    	    				)    		
	    						;
	    				
						final BucketActionReplyMessage reply = 						
							accept_or_ignore
									? new BucketActionReplyMessage.BucketActionWillAcceptMessage(hostname)
									: new BucketActionReplyMessage.BucketActionIgnoredMessage(hostname);
									
						closing_sender.tell(reply,  closing_self);
	    			})
	    		.match(BucketActionMessage.class, 
		    		m -> {
		    			if ( shouldLog(m))
		    				_logging_service.getSystemLogger(m.bucket()).log(Level.INFO, ErrorUtils.lazyBuildMessage(true, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"receive", ()->null, ()->ErrorUtils.get("Actor {0} received message {1} from {2} bucket {3}", this.self(), m.getClass().getSimpleName(), this.sender(), m.bucket().full_name()), ()->Collections.emptyMap()));

		    			final ActorRef closing_self = this.self();		    			
		    			
		    			// We're not going to reply this to my sibling if it's from me
		    			final boolean is_analytic_message_from_sibling =
		    					BucketActionMessage.BucketActionAnalyticJobMessage.class.isAssignableFrom(m.getClass());
		    			
		    			final BucketActionMessage final_msg = 
		    					Patterns.match(m).<BucketActionMessage>andReturn()
		    					
		    						// Enrichment message, convert to analytics message
		    						.when(__ -> isEnrichmentRequest(m), __ -> convertEnrichmentToAnalytics(m))
		    						
		    						// Update message telling me this analytic tech has been removed, so send stop messages to all jobs
		    						.when(BucketActionMessage.UpdateBucketActionMessage.class, 
		    								__ -> Optionals.of(() -> m.bucket().analytic_thread().jobs()).map(j -> j.isEmpty()).orElse(false),
		    									msg -> convertEmptyAnalyticsMessageToStop(msg))
		    									
		    						// Standard case
		    						.otherwise(__ -> m);
		    			
		    			if (BucketActionMessage.DeleteBucketActionMessage.class.isAssignableFrom(m.getClass())) {
		    				// notify siblings of deletes immediately)
		    				_trigger_sibling.tell(final_msg, closing_self);
		    			}
		    			//(other sibling notifications only occur after a successful interaction with the underlying technology)
		    			
		    			handleActionRequest(final_msg);
		    		})
	    		.build();
	 }

	////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////
	
	// TOP LEVEL MESSAGE PROCESSING
	/** Handy utility for deciding when to log
	 * @param message
	 * @return
	 */
	private static boolean shouldLog(final BucketActionMessage message) {
		return Patterns.match(message).<Boolean>andReturn()
					.when(BucketActionMessage.BucketActionOfferMessage.class, 
							msg -> Patterns.match(Optional.ofNullable(msg.message_type()).orElse("")).<Boolean>andReturn()
										.when(type -> BucketActionMessage.PollFreqBucketActionMessage.class.toString().equals(type), __ -> false)
										.when(type -> type.isEmpty(), __ -> false) // (leave "" as a catch all for "don't log")
										.otherwise(__ -> true))
					.when(BucketActionMessage.PollFreqBucketActionMessage.class, __ -> false)
					.when(BucketActionMessage.BucketActionAnalyticJobMessage.class, msg -> (JobMessageType.check_completion != msg.type()))
					.otherwise(__ -> true);		
	}
	
	
	/** Send the specified message to the specified analytic technology
	 * @param message - the message to process
	 */
	void handleActionRequest(final BucketActionMessage message) {
		final ActorRef closing_sender = this.sender();
		final ActorRef closing_self = this.self();
				    			
		final String hostname = _context.getInformationService().getHostname();
		final boolean analytic_tech_only = message instanceof BucketActionOfferMessage;
			
		// (cacheJars can't throw checked or unchecked in this thread, only from within exceptions)
		LibraryCacheUtils.cacheJars(message.bucket(), getQuery(message.bucket(), analytic_tech_only), _management_db, _globals, _fs, _context.getServiceContext(), hostname, message)
			.thenCompose(err_or_map -> {
				
				final AnalyticsContext a_context = _context.getNewAnalyticsContext();
				a_context.setBucket(message.bucket());
				
				// set the library bean - note if here then must have been set, else IAnalyticsTechnologyModule wouldn't exist
				final String technology_name_or_id = getAnalyticsTechnologyName(message.bucket()).get(); // (exists by construction)
				
				// handles system classpath and streaming enrichment special cases
				final Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> err_or_tech_module =
						getAnalyticsTechnology(message.bucket(), technology_name_or_id, analytic_tech_only, 
								_stream_analytics_tech.map(s -> (IAnalyticsTechnologyModule)s), 
								_batch_analytics_tech.map(s -> (IAnalyticsTechnologyModule)s), 
								message, hostname, _context.getDataImportConfigurationBean(), err_or_map)
								.bind(t2 -> checkNodeAffinityMatches(message.bucket(), t2, a_context))
								;
				
				err_or_map.forEach(map ->									
					Optional.ofNullable(map.get(technology_name_or_id))
						.map(lib -> a_context.setTechnologyConfig(lib._1()))
						// Else just build a dummy shared library
						.orElseGet(() -> a_context.setTechnologyConfig(
											BeanTemplateUtils.build(SharedLibraryBean.class)
												.with(SharedLibraryBean::path_name, "/" + technology_name_or_id)
											.done().get()))
				);
				
				// One final system classpath/streaming enrichment fix:
				final DataBucketBean final_bucket = finalBucketConversion(technology_name_or_id, message.bucket(), err_or_map);
				
				final CompletableFuture<BucketActionReplyMessage> ret = talkToAnalytics(final_bucket, message, hostname, a_context, _context, Tuples._2T(closing_self, _trigger_sibling), 
																			err_or_map.toOption().orSome(Collections.emptyMap()), 
																			err_or_tech_module, _logging_service);
				
				return handleTechnologyErrors(final_bucket, message, hostname, err_or_tech_module, ret);
				
			})
			.thenAccept(reply -> { // (reply can contain an error or successful reply, they're the same bean type)
				
				if (!(reply instanceof BucketActionReplyMessage.BucketActionNullReplyMessage)) {
					
					// Some information logging:
					Patterns.match(reply).andAct()
						.when(BucketActionReplyMessage.BucketActionCollectedRepliesMessage.class, msg -> {
							if ( shouldLog(message) )
								_logging_service.getSystemLogger(message.bucket()).log(Level.INFO, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"handleActionRequest", ()->null, ()->ErrorUtils.get("Standard aggregated reply to message={0} bucket={1} num_replies={2} num_failed={3}",
										message.getClass().getSimpleName(), message.bucket().full_name(), msg.replies().size(), msg.replies().stream().filter(r -> !r.success()).count()), ()->Collections.emptyMap()));
						})
						.when(BucketActionHandlerMessage.class, msg -> {
							if ( shouldLog(message) | !msg.reply().success())
								_logging_service.getSystemLogger(message.bucket()).log(Level.INFO, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"handleActionRequest", ()->null, ()->ErrorUtils.get("Standard reply to message={0}, bucket={1}, success={2} error={3}", 
										message.getClass().getSimpleName(), message.bucket().full_name(), msg.reply().success(), 
										msg.reply().success() ? "(no error)": msg.reply().message()), ()->Collections.emptyMap()));						
						})
						.when(BucketActionReplyMessage.BucketActionWillAcceptMessage.class, msg -> {
							if ( shouldLog(message) )
								_logging_service.getSystemLogger(message.bucket()).log(Level.INFO, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"handleActionRequest", ()->null, ()->ErrorUtils.get("Standard reply to message={0}, bucket={1}", message.getClass().getSimpleName(), message.bucket().full_name()), ()->Collections.emptyMap()));
						})
						.when(BucketActionReplyMessage.BucketActionIgnoredMessage.class, msg -> {
							if ( shouldLog(message) )
								_logging_service.getSystemLogger(message.bucket()).log(Level.INFO, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"handleActionRequest", ()->null, ()->ErrorUtils.get("Standard reply to message={0}, bucket={1}", message.getClass().getSimpleName(), message.bucket().full_name()), ()->Collections.emptyMap()));
						})
						.otherwise(msg -> {
							//(always log errors)
							_logging_service.getSystemLogger(message.bucket()).log(Level.INFO, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"handleActionRequest", ()->null, ()->ErrorUtils.get("Unusual reply to message={0}, type={2}, bucket={1}", 
									message.getClass().getSimpleName(), message.bucket().full_name(), msg.getClass().getSimpleName()), ()->Collections.emptyMap()));
						});

					//DEBUG
					// Example code to check if a message is serializable - should consider putting this somewhere? 
//					try {
//						java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
//						ObjectOutputStream out = new ObjectOutputStream(baos);
//						out.writeObject(reply);
//						out.writeObject(message);
//					}
//					catch (Exception e) {
//					}
					
					closing_sender.tell(reply,  closing_self);
				}
			})
			.exceptionally(e -> { // another bit of error handling that shouldn't ever be called but is a useful backstop
				// Some information logging:
				_logging_service.getSystemLogger(message.bucket()).log(Level.WARN, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"handleActionRequest", ()->null, ()->ErrorUtils.get("Unexpected error replying to {0}: error = {1}, bucket={2}", BeanTemplateUtils.toJson(message).toString(), ErrorUtils.getLongForm("{0}", e), message.bucket().full_name()), ()->Collections.emptyMap()));
//				_logger.warn(ErrorUtils.get("Unexpected error replying to {0}: error = {1}, bucket={2}", BeanTemplateUtils.toJson(message).toString(), ErrorUtils.getLongForm("{0}", e), message.bucket().full_name()));
				
				final BasicMessageBean error_bean = 
						SharedErrorUtils.buildErrorMessage(hostname, message,
								ErrorUtils.getLongForm(AnalyticsErrorUtils.STREAM_UNKNOWN_ERROR, e, message.bucket().full_name())
								);
				closing_sender.tell(new BucketActionHandlerMessage(hostname, error_bean), closing_self);			    				
				return null;
			})
			;		
	}
	
	////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////

	// ENRICHMENT SPECIAL CASE
	
	/** Determines whether this bucket should be handled as 
	 * @param bucket - the bucket
	 * @param m - the request, currently not used but probably at some point will need to make it explicit vs infer from the bucket parameters
	 * @return
	 */
	protected static boolean isEnrichmentRequest(final BucketActionMessage message) {
		return (null == message.bucket().analytic_thread());
	}
	
	
	/** Converts an enrichment request into an equivalent analytics request
	 * @param message
	 * @return
	 */
	protected BucketActionMessage convertEnrichmentToAnalytics(final BucketActionMessage message) {
		
		return BeanTemplateUtils.clone(message)
					.with(BucketActionMessage::bucket, convertEnrichmentToAnalyticBucket(message.bucket()))
					.done();
	}

	/** Convert an update message where the new bucket has no jobs to a stop message for the previous version of the bucket
	 * @param message
	 * @return
	 */
	protected BucketActionMessage convertEmptyAnalyticsMessageToStop(final BucketActionMessage.UpdateBucketActionMessage message) {
		return new BucketActionMessage.BucketActionAnalyticJobMessage(message.old_bucket(), 
				Optionals.of(() -> message.old_bucket().analytic_thread().jobs()).orElse(Collections.emptyList()), 
				JobMessageType.deleting);
	}
	
	/** Converts a bucket with only streaming enrichment settings into one that has an analytic thread dervied
	 * @param bucket
	 * @return
	 */
	protected static DataBucketBean convertEnrichmentToAnalyticBucket(final DataBucketBean bucket) {
		
		if ((null != bucket.streaming_enrichment_topology()) && isStreamingEnrichmentType(bucket)) {
			final EnrichmentControlMetadataBean enrichment =  Optional.ofNullable(bucket.streaming_enrichment_topology().enabled()).orElse(false)
					? bucket.streaming_enrichment_topology()
					: BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
						.done().get()
					;			
			
			final AnalyticThreadJobBean.AnalyticThreadJobInputBean input =
					new AnalyticThreadJobBean.AnalyticThreadJobInputBean(
							true, //(enabled)
							null, //(name)
							"", // (myself) 
							"stream", 
							null, // (no filter)
							null // (no extra config)
							);		
			
			final AnalyticThreadJobBean.AnalyticThreadJobOutputBean output =
					new AnalyticThreadJobBean.AnalyticThreadJobOutputBean(
							true, // (not used for streaming) 
							false, // (not transient, ie final output) 
							null,  // (no sub-bucket path)
							DataBucketBean.MasterEnrichmentType.streaming // (not used for non-transient)
							);					
	
			final AnalyticThreadJobBean job = new AnalyticThreadJobBean(
					Optional.ofNullable(enrichment.name()).orElse("streaming_enrichment"), //(name) 
					true, // (enabled)
					STREAMING_ENRICHMENT_TECH_NAME, //(technology name or id)
					enrichment.module_name_or_id(),
					enrichment.library_names_or_ids(), //(additional modules)
					enrichment.entry_point(), // if the user specifies an overide 
					Maps.newLinkedHashMap(Optional.ofNullable(enrichment.config()).orElse(Collections.emptyMap())), //(config)
					DataBucketBean.MasterEnrichmentType.streaming, // (type) 
					Collections.emptyList(), //(node rules)
					false, //(multi node enabled)
					false, //(lock to nodes)
					Collections.emptyList(), // (dependencies) 
					Arrays.asList(input), 
					null, //(global input config)
					output
					);
			
			return BeanTemplateUtils.clone(bucket)
					.with(DataBucketBean::analytic_thread,
							BeanTemplateUtils.build(AnalyticThreadBean.class)
								.with(AnalyticThreadBean::jobs, Arrays.asList(job))
							.done().get()
					)
					.done();
		}
		else if ((null != bucket.batch_enrichment_configs()) && isBatchEnrichmentType(bucket)) {
			final AnalyticThreadJobBean.AnalyticThreadJobInputBean input =
					new AnalyticThreadJobBean.AnalyticThreadJobInputBean(
							true, //(enabled) 
							null, //(name)
							"", // (myself) 
							"batch", 
							null, // (no filter)
							null // (no extra config)
							);		
			
			final AnalyticThreadJobBean.AnalyticThreadJobOutputBean output =
					new AnalyticThreadJobBean.AnalyticThreadJobOutputBean(
							true, // (preserve existing data by default) 
							false, // (not transient, ie final output) 
							null,  // (no sub-bucket path)
							null // (not used for non-transient)
							);					
	
			//(needed below)
			final ObjectMapper object_mapper = BeanTemplateUtils.configureMapper(Optional.empty());
			
			final AnalyticThreadJobBean job = new AnalyticThreadJobBean(
					"batch_enrichment", //(name) 
					true, // (enabled)
					BATCH_ENRICHMENT_TECH_NAME, //(technology name or id)
					null, // no concept of a single module for batch enrichment
					bucket // collect _all_ the libraries and modules into the classpath, the BE logic will have to figure out how to sort them out later
						.batch_enrichment_configs().stream()
						.flatMap(cfg -> 
							Stream.concat(Optional.ofNullable(cfg.module_name_or_id()).map(Stream::of).orElseGet(Stream::empty),
											Optional.ofNullable(cfg.library_names_or_ids()).orElse(Collections.emptyList()).stream()
										)
						)
						.collect(Collectors.toList())
					,
					null, // no concept of a single entry point for batch enrichment 
					Maps.<String, Object>newLinkedHashMap(
							ImmutableMap.<String, Object>builder().put(
									EnrichmentControlMetadataBean.ENRICHMENT_PIPELINE, 
									bucket.batch_enrichment_configs().stream().map(cfg -> object_mapper.convertValue(cfg, LinkedHashMap.class)).collect(Collectors.toList()))
									.build()
					)
					//(config)
					,									
					DataBucketBean.MasterEnrichmentType.batch, // (type) 
					Collections.emptyList(), //(node rules)
					false, //(multi node enabled)
					false, // (lock to nodes)
					Collections.emptyList(), // (dependencies) 
					Arrays.asList(input), 
					null, //(global input config)
					output
					);
			
			return BeanTemplateUtils.clone(bucket)
					.with(DataBucketBean::analytic_thread,
							BeanTemplateUtils.build(AnalyticThreadBean.class)
								.with(AnalyticThreadBean::jobs, Arrays.asList(job))
								.with(AnalyticThreadBean::trigger_config, 
										BeanTemplateUtils.build(AnalyticThreadTriggerBean.class)
											.with(AnalyticThreadTriggerBean::auto_calculate, true)
										.done().get())
							.done().get()
					)
					.done();
		}
		else {
			return bucket;
		}
		
	}
	
	/** Quick utility to determine if a bucket has a streaming type
	 * @param bucket
	 * @return
	 */
	private static boolean isStreamingEnrichmentType(final DataBucketBean bucket) {
		return _streaming_types.contains(Optional.ofNullable(bucket.master_enrichment_type()).orElse(MasterEnrichmentType.none));
	}
	
	/** Quick utility to determine if a bucket has a batch type
	 * @param bucket
	 * @return
	 */
	private static boolean isBatchEnrichmentType(final DataBucketBean bucket) {
		return _batch_types.contains(Optional.ofNullable(bucket.master_enrichment_type()).orElse(MasterEnrichmentType.none));
	}
	/** Quick utility to determine if a bucket job has a batch type
	 * @param bucket
	 * @return
	 */
	private static boolean isBatchEnrichmentType(final AnalyticThreadJobBean job) {
		return _batch_types.contains(Optional.ofNullable(job.analytic_type()).orElse(MasterEnrichmentType.none));
	}	
	
	/** Quick utility to determine if a bucket has a batch or streaming type
	 * @param bucket
	 * @return
	 */
	private static boolean isEnrichmentType(final DataBucketBean bucket) {
		return MasterEnrichmentType.none != Optional.ofNullable(bucket.master_enrichment_type()).orElse(MasterEnrichmentType.none);
	}
	
	/** Fills in the jobs' entry points in the streaming enrichment case
	 * @param technology
	 * @param bucket
	 * @return
	 */
	protected static final DataBucketBean finalBucketConversion(
			final String technology, 
			final DataBucketBean bucket, 
			final Validation<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>> err_or_libs)
	{
		//TODO (ALEPH-12 also handle the system classpath case, using some lookup engine)
		return err_or_libs.validation(
				fail -> bucket
				,
				libs -> { 
					if (STREAMING_ENRICHMENT_TECH_NAME.equals(technology) // enrichment is specified
							&& (null != bucket.streaming_enrichment_topology()) // there is a streaming topology specified
							&& Optional.ofNullable(bucket.streaming_enrichment_topology().enabled()).orElse(true) // it's enabled (otherwise entry_point==null)
							&& isStreamingEnrichmentType(bucket) // it is a streaming enrichment bucket
							) 
					{
						// Check all modules and libs...
						return Stream.concat(
									Optional.ofNullable(bucket.streaming_enrichment_topology().module_name_or_id()).map(Stream::of).orElse(Stream.empty())
									,
									Optional.ofNullable(bucket.streaming_enrichment_topology().library_names_or_ids()).map(List::stream).orElse(Stream.empty())
								)
								.map(name -> libs.get(name)) //...to see if we can find the corresponding shared library...
								.filter(t2 -> t2 != null)
								.map(t2 -> t2._1())
								.map(lib -> Optional.ofNullable(bucket.streaming_enrichment_topology().entry_point()) 
												.map(Optional::of)
												.orElse(Optional.ofNullable(lib.streaming_enrichment_entry_point()))
												.orElse(lib.misc_entry_point())
								)
								.filter(entry_point -> entry_point != null) //...that has a valid entry point...
								.findFirst() 
								.map(entry_point -> { // ... grab the first and ...
									return BeanTemplateUtils.clone(bucket)
											.with(DataBucketBean::analytic_thread,
												BeanTemplateUtils.clone(bucket.analytic_thread())
													.with(AnalyticThreadBean::jobs,
														bucket.analytic_thread().jobs().stream().map(job ->
															BeanTemplateUtils.clone(job)
																.with(AnalyticThreadJobBean::entry_point, entry_point) //...set that entry point in all the jobs...
															.done()
														)
														.collect(Collectors.toList())
														)
												.done())
											.done();									
								})
								.orElse(bucket); // (if anything fails just return the bucket)					
					}
					else if (BATCH_ENRICHMENT_TECH_NAME.equals(technology) // enrichment is specified
							&& (null != bucket.batch_enrichment_configs()) // there is a batch topology specified
							&& bucket.batch_enrichment_configs().stream().filter(cfg -> Optional.ofNullable(cfg.enabled()).orElse(true)).findAny().isPresent()
							&& isBatchEnrichmentType(bucket) // it is a batch enrichment bucket
							) 
					{
						return bucket; // nothing to do here the entry points are inferred from the configurations
					}
					else return bucket;
				})
				;
	}
	
	////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////
	
	// GENERAL ANALYTICS CASE

	/** Handy utiltiy for code used in a couple of places
	 * @param bucket
	 * @return
	 */
	protected static Optional<String> getAnalyticsTechnologyName(final DataBucketBean bucket) {
		return Optional.ofNullable(bucket.analytic_thread().jobs())
				.flatMap(jobs -> jobs.stream().findFirst())
				.map(job -> job.analytic_technology_name_or_id());		
	}
	
	// Functional code - control logic

	/** Talks to the analytic tech module - this top level function just sets the classloader up and creates the module,
	 *  then calls talkToHarvester_actuallyTalk to do the talking
	 * @param bucket
	 * @param libs
	 * @param analytic_tech_only
	 * @param m
	 * @param source
	 * @return
	 */
	protected static Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> getAnalyticsTechnology(
			final DataBucketBean bucket, 
			final String technology_name_or_id,
			final boolean analytic_tech_only,
			final Optional<IAnalyticsTechnologyModule> streaming_enrichment,
			final Optional<IAnalyticsTechnologyModule> batch_enrichment,
			final BucketActionMessage m, 
			final String source,
			final DataImportConfigurationBean config,
			final Validation<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>> err_or_libs // "pipeline element"
			)
	{
		try {
			return err_or_libs.<Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>>>validation(
					//Error:
					error -> Validation.fail(error)
					,
					// Normal
					libs -> {
						
						// Special case: system-registered classpaths
						final Tuple2<String, String> entrypoint_path = Lambdas.get(() -> {
							return Optional.ofNullable(libs.get(technology_name_or_id)) 
									.map(bean_path -> Tuples._2T(bean_path._1().misc_entry_point(), bean_path._2()))
									.orElseGet(() -> {
										return Patterns.match(technology_name_or_id).<Tuple2<String, String>>andReturn()
											.when(t -> config.registered_technologies().containsKey(t), t -> {												
												return Tuples._2T(config.registered_technologies().get(t), null); 
											})
											.when(t -> STREAMING_ENRICHMENT_TECH_NAME.equals(t), __ -> {
												try { 
													return Tuples._2T(streaming_enrichment.get().getClass().getName(), null); 
												} catch (Throwable t) { return null; }
											})
											.when(t -> BATCH_ENRICHMENT_TECH_NAME.equals(t), __ -> {
												try { 
													return Tuples._2T(batch_enrichment.get().getClass().getName(), null); 
												} catch (Throwable t) { return null; }
											})
											.otherwise(__ -> null);
									});
						});

						if ((null == entrypoint_path) || (null == entrypoint_path._1())) { // Nice easy error case, probably can't ever happen (note ._2() can be null)
							return Validation.fail(
									SharedErrorUtils.buildErrorMessage(source, m,
											SharedErrorUtils.SHARED_LIBRARY_NAME_NOT_FOUND, bucket.full_name(), technology_name_or_id));
						}
						
						final List<String> other_libs = analytic_tech_only 
								? Collections.emptyList() 
								: libs.values().stream().map(lp -> lp._2()).collect(Collectors.toList());
						
						final Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> ret_val = 
								ClassloaderUtils.getFromCustomClasspath_withClassloader(IAnalyticsTechnologyModule.class, 
										entrypoint_path._1(), 
										Optional.ofNullable(entrypoint_path._2()),
										other_libs,
										source, m);
						
						return ret_val;
					});
		}
		catch (Throwable t) {			
			return Validation.fail(
					SharedErrorUtils.buildErrorMessage(source, m,
						ErrorUtils.getLongForm(SharedErrorUtils.ERROR_LOADING_CLASS, t, bucket.full_name())));  
			
		}
	}

	/** Quickly check if the node affinity vs lock_to_nodes match up
	 * @param bucket
	 * @param technology_classloader
	 * @param context
	 * @return
	 */
	protected static Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> checkNodeAffinityMatches(
			final DataBucketBean bucket,
			final Tuple2<IAnalyticsTechnologyModule, ClassLoader> technology_classloader,
			final IAnalyticsContext context
			) 
	{
		// By construction, all the jobs have the same setting, so:
		final boolean lock_to_nodes = Optionals.of(() -> bucket.analytic_thread().jobs().stream().findAny().map(j -> j.lock_to_nodes()).get()).orElse(false);
		
		Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> x = 
			Optional.of(lock_to_nodes)
				.filter(lock -> lock != technology_classloader._1().applyNodeAffinity(bucket, context))
				.map(still_here -> Validation.<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>>fail(
						ErrorUtils.buildErrorMessage(DataBucketHarvestChangeActor.class.getSimpleName(), "applyNodeAffinity", 
														HarvestErrorUtils.MISMATCH_BETWEEN_TECH_AND_BUCKET_NODE_AFFINITY, 
														bucket.full_name(), technology_classloader._1().getClass().getSimpleName())))
				.orElse(Validation.<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>>success(technology_classloader))
				;
		return x;
	}	
	
	/** We'll only start batch jobs if they have no associated dependencies
	 *  Here's a more detailed overview of the logic
	 *  1) For new and test messages - don't do anything unless there's a bucket-wide _manual_ trigger
	 *  2) For new and test messages - bypass jobs with dependencies
	 *  (For new/test messages, have already bypassed jobs based on their activity)
	 *  3a) For update message, global activate - always process "suspended jobs"
	 *  3b) For update message, global activate - ignore active jobs if there's a bucket-wide _manual_ trigger
	 *  3c) For update message, global active - otherwise process jobs with no dependencies
	 *  4) For update message, global suspend - process all jobs
	 *  5) For delete message - process all jobs
	 * @param job
	 * @return
	 */
	private static final boolean isBatchJobWithDependencies(final DataBucketBean bucket, 
															final AnalyticThreadJobBean job,
															final Tuple2<Boolean, Boolean> existingbucket_bucketactive)
	{
		if (!isBatchEnrichmentType(job)) {
			return false; //(nice easy bypass)
		}
		// if present then trigger isn't manual unless disabled
		final boolean trigger_non_manual = Optionals.of(() -> bucket.analytic_thread().trigger_config())
											.map(cfg -> Optional.ofNullable(cfg.enabled()).orElse(true))
											.orElse(false)
											; 
		
		final boolean job_has_dependency = !Optional.ofNullable(job.dependencies()).orElse(Collections.emptyList()).isEmpty();
			//(we've already checked the job is batch)
		
		final boolean existing_bucket = existingbucket_bucketactive._1();
		final boolean bucket_active = existingbucket_bucketactive._2();
		final boolean job_active = Optional.ofNullable(job.enabled()).orElse(true);
		
		// true: means will skip, false: means will process
		return Patterns.match().<Boolean>andReturn()
			.when(__ -> !bucket_active, __ -> false) // 4 + 5, PASS
			.when(__ -> !existing_bucket && trigger_non_manual, __ -> true) //1, STOP
			.when(__ -> !existing_bucket && !trigger_non_manual, __ -> job_has_dependency) //2 PASS<=>no_dependency
			.when(__ -> existing_bucket && !job_active, __ -> false) //3a PASS
			.when(__ -> existing_bucket && trigger_non_manual, __ -> true) //3b FAIL
			.when(__ -> existing_bucket && !trigger_non_manual, __ -> job_has_dependency) //3c PASS<=>no_dependency
			.otherwise(__ -> true) //(shouldn't be possible)
			;
	}
	
	/** Make various requests of the analytics module based on the message type
	 * @param bucket
	 * @param tech_module
	 * @param m
	 * @return - a future containing the reply or an error (they're the same type at this point hence can discard the Validation finally)
	 */
	protected static CompletableFuture<BucketActionReplyMessage> talkToAnalytics(
			final DataBucketBean bucket,
			final BucketActionMessage m,			
			final String source,
			final AnalyticsContext context,
			final DataImportActorContext dim_context,
			final Tuple2<ActorRef, ActorSelection> me_sibling,
			final Map<String, Tuple2<SharedLibraryBean, String>> libs, // (if we're here then must be valid)
			final Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> err_or_tech_module, // "pipeline element"
			final ILoggingService _logging_service
			)
	{
		final List<AnalyticThreadJobBean> jobs = bucket.analytic_thread().jobs();
		
		final BiFunction<Stream<AnalyticThreadJobBean>, Tuple2<Boolean, Boolean>, Stream<AnalyticThreadJobBean>> perJobSetup = 
				(job_stream, existingbucket_bucketactive) -> {
					return job_stream
							.filter(job -> existingbucket_bucketactive._1() || Optional.ofNullable(job.enabled()).orElse(true))
							.filter(job -> !isBatchJobWithDependencies(bucket, job, existingbucket_bucketactive))
							.peek(job -> setPerJobContextParams(job, context, libs)); //(WARNING: mutates context)
				};
		
		final ClassLoader saved_current_classloader = Thread.currentThread().getContextClassLoader();		
		try {			
			return err_or_tech_module.<CompletableFuture<BucketActionReplyMessage>>validation(
				//Error:
				error -> CompletableFuture.completedFuture(new BucketActionHandlerMessage(source, error))
				,
				// Normal
				techmodule_classloader -> {
					final IAnalyticsTechnologyModule tech_module = techmodule_classloader._1();
					
					if ( shouldLog(m) )
						_logging_service.getSystemLogger(bucket).log(Level.INFO, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->"Set active classloader=" + techmodule_classloader._2() + " class=" + tech_module.getClass() + " message=" + m.getClass().getSimpleName() + " bucket=" + bucket.full_name(), ()->Collections.emptyMap()));
					Thread.currentThread().setContextClassLoader(techmodule_classloader._2());
										
					tech_module.onInit(context);
					
					// One final check before we do anything: are we allowed to run multi-node if we're trying
					// By construction, all the jobs have the same setting, so:
					final boolean multi_node_enabled = jobs.stream().findFirst().map(j -> j.multi_node_enabled()).orElse(false);					
					if (multi_node_enabled) {
						if (!tech_module.supportsMultiNode(bucket, jobs, context)) {
							return CompletableFuture.completedFuture(
									new BucketActionHandlerMessage(source, SharedErrorUtils.buildErrorMessage(source, m,
										ErrorUtils.get(AnalyticsErrorUtils.TRIED_TO_RUN_MULTI_NODE_ON_UNSUPPORTED_TECH, bucket.full_name(), tech_module.getClass().getSimpleName()))));
						}
					}
					
					return Patterns.match(m).<CompletableFuture<BucketActionReplyMessage>>andReturn()
						.when(BucketActionMessage.BucketActionOfferMessage.class, msg -> {
							final boolean accept_or_ignore = 
									NodeRuleUtils.canRunOnThisNode(jobs.stream().map(j -> Optional.ofNullable(j.node_list_rules())), dim_context) &&
									tech_module.canRunOnThisNode(bucket, jobs, context);
							
							return CompletableFuture.completedFuture(accept_or_ignore
									? new BucketActionReplyMessage.BucketActionWillAcceptMessage(source)
									: new BucketActionReplyMessage.BucketActionIgnoredMessage(source));
						})
						.when(BucketActionMessage.DeleteBucketActionMessage.class, msg -> {
							//(note have already told the sibling about this)
							
							final CompletableFuture<BasicMessageBean> top_level_result = tech_module.onDeleteThread(bucket, jobs, context);
							final List<Tuple2<AnalyticThreadJobBean, CompletableFuture<BasicMessageBean>>> job_results = 
									perJobSetup.apply(jobs.stream(), Tuples._2T(true, false))
														.map(job -> Tuples._2T(job, (CompletableFuture<BasicMessageBean>)
																tech_module.stopAnalyticJob(bucket, jobs, job, context)))
														.collect(Collectors.toList());
							
							//(no need to call the context.completeJobOutput since we're deleting the bucket)
							sendOnTriggerEventMessages(job_results, msg.bucket(), __ -> Optional.of(JobMessageType.stopping), me_sibling, _logging_service);									
							
							return combineResults(top_level_result, job_results.stream().map(jf -> jf._2()).collect(Collectors.toList()), source);
						})
						.when(BucketActionMessage.NewBucketActionMessage.class, msg -> {
							final CompletableFuture<BasicMessageBean> top_level_result = tech_module.onNewThread(bucket, jobs, context, !msg.is_suspended());
							
							return top_level_result.thenCompose(ret_val -> {							
								if (!ret_val.success()) {
									return combineResults(top_level_result, Arrays.asList(), source);
								}
								else { // success, carry on
									// Firstly, tell the sibling
									if (null != me_sibling) me_sibling._2().tell(msg, me_sibling._1());
									
									final boolean starting_thread = 
											msg.is_suspended()
												? false
												: perJobSetup.apply(jobs.stream(), Tuples._2T(false, true))
														.anyMatch(job -> _batch_types.contains(job.analytic_type()))
												;
									
									if (starting_thread) {
										BasicMessageBean thread_start_result = tech_module.onThreadExecute(bucket, jobs, Collections.emptyList(), context).join(); // (wait for completion before doing anything else)
										_logging_service.getSystemLogger(bucket).log(thread_start_result.success() ? Level.INFO : Level.WARN, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->ErrorUtils.get("Executing thread for bucket {0}, success={1} (error={2})",bucket.full_name(), thread_start_result.success(),
												thread_start_result.success() ? "none" : thread_start_result.message()), ()->Collections.emptyMap()));
									}
									
									final List<Tuple2<AnalyticThreadJobBean, CompletableFuture<BasicMessageBean>>> job_results = 
											msg.is_suspended()
											? Collections.emptyList()
											: perJobSetup.apply(jobs.stream(), Tuples._2T(false, true))
																.map(job -> Tuples._2T(job, (CompletableFuture<BasicMessageBean>)
																		tech_module.startAnalyticJob(bucket, jobs, job, context)))
																.collect(Collectors.toList());
									
									// Only send on trigger events for messages that started
									sendOnTriggerEventMessages(job_results, msg.bucket(), 
																j_r -> {
																	_logging_service.getSystemLogger(bucket).log(j_r._2().success() ? Level.INFO : Level.WARN, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->ErrorUtils.get("Starting bucket:job {0}:{1} success={2}{3}", 
																			bucket.full_name(), j_r._1().name(), j_r._2().success(),
																			j_r._2().success() ? "" : (" error = " + j_r._2().message())
																			), ()->Collections.emptyMap()));
															
																	return j_r._2().success() ? Optional.of(JobMessageType.starting) : Optional.empty(); 
																},
																me_sibling, _logging_service);									
											
									return combineResults(top_level_result, job_results.stream().map(jf -> jf._2()).collect(Collectors.toList()), source);
								}
							});
						})
						.when(BucketActionMessage.UpdateBucketActionMessage.class, msg -> {
							final CompletableFuture<BasicMessageBean> top_level_result = tech_module.onUpdatedThread(msg.old_bucket(), bucket, jobs, msg.is_enabled(), Optional.empty(), context);
							
							return top_level_result.thenCompose(ret_val -> {							
								if (!ret_val.success()) {
									return combineResults(top_level_result, Arrays.asList(), source);
								}
								else { // success, carry on
									// Firstly, tell the sibling
									if (null != me_sibling) me_sibling._2().tell(msg, me_sibling._1());
									
									final boolean starting_thread = 
											!msg.is_enabled()
												? false
												: perJobSetup.apply(jobs.stream(), Tuples._2T(true, true))
														.filter(job -> Optional.ofNullable(job.enabled()).orElse(true))
														.anyMatch(job -> _batch_types.contains(job.analytic_type()))
												;
									
									if (starting_thread) {
										BasicMessageBean thread_start_result = tech_module.onThreadExecute(bucket, jobs, Collections.emptyList(), context).join(); // (wait for completion before doing anything else)
										_logging_service.getSystemLogger(bucket).log(Level.INFO, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->ErrorUtils.get("Executing thread for bucket {0}, success={1} (error={2})",
												bucket.full_name(), thread_start_result.success(),
												thread_start_result.success() ? "none" : thread_start_result.message()), ()->Collections.emptyMap()));
									}
									//(don't need the analog for stopping because the trigger will give me the notification once all jobs are completed)
									
									final List<Tuple2<AnalyticThreadJobBean, CompletableFuture<BasicMessageBean>>> job_results = 
											perJobSetup.apply(jobs.stream(), Tuples._2T(true, msg.is_enabled()))
												.map(job -> Tuples._2T(job, (CompletableFuture<BasicMessageBean>)
														((msg.is_enabled() && Optional.ofNullable(job.enabled()).orElse(true))
															? tech_module.resumeAnalyticJob(bucket, jobs, job, context)
															: tech_module.suspendAnalyticJob(bucket, jobs, job, context)
															)))
												.collect(Collectors.toList());
									
									// Send all stop messages, and start messages for jobs that succeeeded
									sendOnTriggerEventMessages(job_results, msg.bucket(), 
																j_r -> {
																	if (msg.is_enabled() && Optional.ofNullable(j_r._1().enabled()).orElse(true)) {
																		_logging_service.getSystemLogger(bucket).log(Level.INFO, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->ErrorUtils.get("Starting bucket:job {0}:{1} success={2}{3}", 
																				bucket.full_name(), j_r._1().name(), j_r._2().success(),
																				j_r._2().success() ? "" : (" error = " + j_r._2().message())
																				), ()->Collections.emptyMap()));														
																		return j_r._2().success() ? Optional.of(JobMessageType.starting) : Optional.empty();
																	}
																	else { // either stopping all, or have disabled certain jobs
																		_logging_service.getSystemLogger(bucket).log(Level.INFO, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->ErrorUtils.get("Stopping bucket:job {0}:{1}", bucket.full_name(), j_r._1().name()), ()->Collections.emptyMap()));
																		if (msg.is_enabled()) { //(else stopping the entire bucket)
																			context.completeJobOutput(msg.bucket(), j_r._1());																	
																		}
																		return Optional.of(JobMessageType.stopping);
																	}
																},
																me_sibling, _logging_service);									
									
									return combineResults(top_level_result, job_results.stream().map(jf -> jf._2()).collect(Collectors.toList()), source);
								}
							});
						})
						.when(BucketActionMessage.PurgeBucketActionMessage.class, msg -> {
							final CompletableFuture<BasicMessageBean> top_level_result = tech_module.onPurge(bucket, jobs, context);							
							// (don't need to tell the sibling about this)
							
							return combineResults(top_level_result, Collections.emptyList(), source);
						})
						.when(BucketActionMessage.TestBucketActionMessage.class, msg -> {
							final CompletableFuture<BasicMessageBean> top_level_result = tech_module.onTestThread(bucket, jobs, msg.test_spec(), context);
							return top_level_result.thenCompose(ret_val -> {							
								if (!ret_val.success()) {
									return combineResults(top_level_result, Arrays.asList(), source);
								}
								else { // success, carry on
									// Firstly, tell the sibling
									if (null != me_sibling) me_sibling._2().tell(msg, me_sibling._1());
									
									final List<Tuple2<AnalyticThreadJobBean, CompletableFuture<BasicMessageBean>>> job_results = 
											perJobSetup.apply(jobs.stream(), Tuples._2T(false, true))
												.map(job -> Tuples._2T(job, (CompletableFuture<BasicMessageBean>)
														tech_module.startAnalyticJobTest(bucket, jobs, job, msg.test_spec(), context)))
												.collect(Collectors.toList());
									
									// Only send on trigger events for messages that started
									sendOnTriggerEventMessages(job_results, msg.bucket(), 
																j_r -> {
																	_logging_service.getSystemLogger(bucket).log(Level.INFO, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->ErrorUtils.get("Starting test bucket:job {0}:{1} success={2}{3}", 
																			bucket.full_name(), j_r._1().name(), j_r._2().success(),
																			j_r._2().success() ? "" : (" error = " + j_r._2().message())																	
																			), ()->Collections.emptyMap()));
																	return j_r._2().success() ? Optional.of(JobMessageType.starting) : Optional.empty(); 
																},
																me_sibling, _logging_service);																		
									
									return combineResults(top_level_result, job_results.stream().map(jf -> jf._2()).collect(Collectors.toList()), source);
								}
							});
						})
						.when(BucketActionMessage.PollFreqBucketActionMessage.class, msg -> {
							final CompletableFuture<BasicMessageBean> top_level_result = tech_module.onPeriodicPoll(bucket, jobs, context);
							
							//(don't need to tell trigger sibling about this)
							
							return combineResults(top_level_result, Collections.emptyList(), source);
						})
						// Finally, a bunch of analytic messages (don't tell trigger sibling about any of these)
						.when(BucketActionMessage.BucketActionAnalyticJobMessage.class,
								msg -> (JobMessageType.check_completion == msg.type()), 
								msg -> {
									// Check whether these jobs are complete, send message back to sibling asynchronously

									//(note: don't use perJobSetup for these explicity analytic event messages)
									final List<Tuple2<AnalyticThreadJobBean, CompletableFuture<Boolean>>> job_results = 
											Optionals.ofNullable(msg.jobs()).stream()
												.peek(job -> setPerJobContextParams(job, context, libs)) //(WARNING: mutates context)
												.map(job -> Tuples._2T(job, (CompletableFuture<Boolean>)
														tech_module.checkAnalyticJobProgress(msg.bucket(), msg.jobs(), job, context)))
												.collect(Collectors.toList());
									
									// In addition (for now) just log the management results
									job_results.stream().forEach(jr -> {
											if (jr._2() instanceof ManagementFuture) {
												ManagementFuture<Boolean> jr2 = (ManagementFuture<Boolean>)jr._2();
												jr2.thenAccept(result -> {
													if (result) {
														jr2.getManagementResults().thenAccept(mgmt_results -> {
															List<String> errs = mgmt_results.stream().filter(res -> !res.success()).map(res -> res.message()).collect(Collectors.toList());
															if (!errs.isEmpty()) {
																_logging_service.getSystemLogger(bucket).log(Level.INFO, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->ErrorUtils.get("Completed bucket:job {0}:{1} had errors: {2}",
																		bucket.full_name(), jr._1().name(), errs.stream().collect(Collectors.joining(";"))), ()->Collections.emptyMap()));																
															}
														});
													}
												});
											}
											//(it will always be)
									});
									
									sendOnTriggerEventMessages(job_results, msg.bucket(), t2 -> {
										if (t2._2()) {
											_logging_service.getSystemLogger(bucket).log(Level.INFO, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->ErrorUtils.get("Completed: bucket:job {0}:{1}", bucket.full_name(), t2._1().name()), ()->Collections.emptyMap()));
											context.completeJobOutput(msg.bucket(), t2._1());
										}
										return t2._2() ? Optional.of(JobMessageType.stopping) : Optional.empty();
									}, me_sibling, _logging_service);									
									
									// Send a status message (Which will be ignored)
									
									return CompletableFuture.completedFuture(new BucketActionReplyMessage.BucketActionNullReplyMessage());
								})
						.when(BucketActionMessage.BucketActionAnalyticJobMessage.class,
								msg -> (JobMessageType.starting == msg.type()) && (null == msg.jobs()), 
								msg -> {
									// Received a start notification for the bucket

									//TODO (ALEPH-12): get the matching triggers into the message
									final CompletableFuture<BasicMessageBean> top_level_result = tech_module.onThreadExecute(msg.bucket(), jobs, Collections.emptyList(), context);
																
									//(ignore the reply apart from logging - failures will be identified by triggers)
									top_level_result.thenAccept(reply -> {
										if (!reply.success()) {
											_logging_service.getSystemLogger(bucket).log(Level.WARN, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->ErrorUtils.get("Error starting analytic thread {0}: message={1}", bucket.full_name(), reply.message()), ()->Collections.emptyMap()));
										}
										else {
											_logging_service.getSystemLogger(bucket).log(Level.INFO, ErrorUtils.lazyBuildMessage(true, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->ErrorUtils.get("Started analytic thread {0}", bucket.full_name()), ()->Collections.emptyMap()));
										}
									}); 
																		
									// Now start any enabled jobs that have no dependencies
									final List<Tuple2<AnalyticThreadJobBean, CompletableFuture<BasicMessageBean>>> job_results = 
											jobs.stream()
												.filter(job -> Optional.ofNullable(job.enabled()).orElse(true))
												.filter(job -> Optionals.ofNullable(job.dependencies()).isEmpty())
												.peek(job -> setPerJobContextParams(job, context, libs)) //(WARNING: mutates context)
												.map(job -> Tuples._2T(job, (CompletableFuture<BasicMessageBean>)
														tech_module.startAnalyticJob(msg.bucket(), jobs, job, context)))
												.collect(Collectors.toList());

									// Only send on trigger events for messages that started
									sendOnTriggerEventMessages(job_results, msg.bucket(), 
																j_r -> {
																	_logging_service.getSystemLogger(bucket).log(Level.INFO, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->ErrorUtils.get("Starting bucket:job {0}:{1} success={2}{3}", 
																			bucket.full_name(), j_r._1().name(), j_r._2().success(),
																			j_r._2().success() ? "" : (" error = " + j_r._2().message())), ()->Collections.emptyMap()));
																	return j_r._2().success() ? Optional.of(JobMessageType.starting) : Optional.empty();																		
																}, 
																me_sibling, _logging_service);									
									
									// Send a status message (Which will be ignored)
									
									return CompletableFuture.completedFuture(new BucketActionReplyMessage.BucketActionNullReplyMessage());
								})
						.when(BucketActionMessage.BucketActionAnalyticJobMessage.class,
								msg -> (JobMessageType.starting == msg.type()) && (null != msg.jobs()), 
								msg -> {
									// Received a start notification for 1+ of the jobs
									
									//(note: don't use perJobSetup for these explicity analytic event messages)
									final List<Tuple2<AnalyticThreadJobBean, CompletableFuture<BasicMessageBean>>> job_results = 
											msg.jobs().stream()
												.peek(job -> setPerJobContextParams(job, context, libs)) //(WARNING: mutates context)
												.map(job -> Tuples._2T(job, (CompletableFuture<BasicMessageBean>)
														tech_module.startAnalyticJob(msg.bucket(), jobs, job, context)))
												.collect(Collectors.toList());
									
									//(ignore the reply apart from logging - failures will be identified by triggers)
									job_results.forEach(job_res -> {
										job_res._2().thenAccept(res -> {
											if (!res.success()) {
												_logging_service.getSystemLogger(bucket).log(Level.WARN, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->ErrorUtils.get("Error starting analytic job {0}:{1}: message={2}", bucket.full_name(), job_res._1().name(), res.message()), ()->Collections.emptyMap()));
											} else {
												_logging_service.getSystemLogger(bucket).log(Level.INFO, ErrorUtils.lazyBuildMessage(true, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->ErrorUtils.get("Started analytic job {0}:{1}", bucket.full_name(), job_res._1().name()), ()->Collections.emptyMap()));
											}											
										});
									});
									
									// Send a status message (Which will be ignored)
									
									return CompletableFuture.completedFuture(new BucketActionReplyMessage.BucketActionNullReplyMessage());
								})
						.when(BucketActionMessage.BucketActionAnalyticJobMessage.class,
								msg -> (JobMessageType.stopping == msg.type()) && (null == msg.jobs()), 
								msg -> {
									// Received a stop notification for the bucket

									// Complete the job output
									context.completeBucketOutput(msg.bucket());
									
									final CompletableFuture<BasicMessageBean> top_level_result = tech_module.onThreadComplete(msg.bucket(), jobs, context);
																
									//(ignore the reply apart from logging - failures will be identified by triggers)
									top_level_result.thenAccept(reply -> {
										if (!reply.success()) {
											_logging_service.getSystemLogger(bucket).log(Level.WARN, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->ErrorUtils.get("Error stopping analytic thread {0}: message={1}", bucket.full_name(), reply.message()), ()->Collections.emptyMap()));
										} else {
											_logging_service.getSystemLogger(bucket).log(Level.INFO, ErrorUtils.lazyBuildMessage(true, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->ErrorUtils.get("Stopping analytic thread {0}", bucket.full_name()), ()->Collections.emptyMap()));
										}
									}); 
									
									// Send a status message (Which will be ignored)
									
									return CompletableFuture.completedFuture(new BucketActionReplyMessage.BucketActionNullReplyMessage());
								})
						.when(BucketActionMessage.BucketActionAnalyticJobMessage.class,
								msg -> (JobMessageType.stopping == msg.type()) && (null != msg.jobs()), 
								msg -> {
									final List<Tuple2<AnalyticThreadJobBean, CompletableFuture<BasicMessageBean>>> job_results = 
											msg.jobs().stream()
												.peek(job -> setPerJobContextParams(job, context, libs)) //(WARNING: mutates context)
												.map(job -> Tuples._2T(job, (CompletableFuture<BasicMessageBean>)
														tech_module.suspendAnalyticJob(msg.bucket(), jobs, job, context)))
												.collect(Collectors.toList());
								
									//(ignore the reply apart from logging - failures will be identified by triggers)
									job_results.forEach(job_res -> {
										job_res._2().thenAccept(res -> {
											if (!res.success()) {
												_logging_service.getSystemLogger(bucket).log(Level.WARN, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->ErrorUtils.get("Error stopping analytic job {0}:{1}: message={2}", bucket.full_name(), job_res._1().name(), res.message()), ()->Collections.emptyMap()));
											}
											else {
												_logging_service.getSystemLogger(bucket).log(Level.INFO, ErrorUtils.lazyBuildMessage(true, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"talkToAnalytics", ()->null, ()->ErrorUtils.get("Stopping analytic job {0}:{1}", bucket.full_name(), job_res._1().name()), ()->Collections.emptyMap()));
											}											
										});
									});
									
									// Send a status message (Which will be ignored)
									
									return CompletableFuture.completedFuture(new BucketActionReplyMessage.BucketActionNullReplyMessage());
								})
						.when(BucketActionMessage.BucketActionAnalyticJobMessage.class,
								msg -> (JobMessageType.deleting == msg.type()), 
								msg -> {
									// This is different because it happens as part of a user action related to buckets, whereas stopping occurs based on trigger related actions
									
									final CompletableFuture<BasicMessageBean> top_level_result = CompletableFuture.completedFuture(
											ErrorUtils.buildSuccessMessage(DataBucketAnalyticsChangeActor.class.getSimpleName(), "BucketActionAnalyticJobMessage:deleting", ""));
									
									final List<CompletableFuture<BasicMessageBean>> job_results = 
											Optionals.ofNullable(msg.jobs()).stream()
												.map(job -> tech_module.suspendAnalyticJob(bucket, jobs, job, context))
												.collect(Collectors.toList());
									
									// Hence do return a legit reply message here
									
									return combineResults(top_level_result, job_results, source);
								})
						.otherwise(msg -> { // return "command not recognized" error
							return CompletableFuture.completedFuture(
									new BucketActionHandlerMessage(source, SharedErrorUtils.buildErrorMessage(source, m,
										AnalyticsErrorUtils.MESSAGE_NOT_RECOGNIZED, 
											bucket.full_name(), m.getClass().getSimpleName())));
						});
				});
		}
		catch (Throwable e) { // (trying to use Validation to avoid this, but just in case...)
			return CompletableFuture.completedFuture(
					new BucketActionHandlerMessage(source, SharedErrorUtils.buildErrorMessage(source, m,
						ErrorUtils.getLongForm(SharedErrorUtils.ERROR_LOADING_CLASS, e, err_or_tech_module.success()._1().getClass()))));
		}		
		finally {
			Thread.currentThread().setContextClassLoader(saved_current_classloader);
		}
	}
			
	/** Utility to set the per modules settings for the context 
	 * @param job
	 * @param context
	 * @param libs
	 */
	protected final static void setPerJobContextParams(
			final AnalyticThreadJobBean job, 
			final AnalyticsContext context,
			final Map<String, Tuple2<SharedLibraryBean, String>> libs
			)
	{
		context.resetJob(job);
		context.resetLibraryConfigs(
				Stream.concat(
						Optional.ofNullable(job.module_name_or_id()).map(Stream::of).orElseGet(Stream::empty),
						Optional.ofNullable(job.library_names_or_ids()).map(l -> l.stream()).orElseGet(Stream::empty))
					.map(lib -> libs.get(lib))
					.filter(lib -> null != lib)
					.map(lib -> lib._1())
					.flatMap(lib -> Arrays.asList(Tuples._2T(lib._id(), lib), Tuples._2T(lib.path_name(), lib)).stream())
					.collect(Collectors.toMap(
								t2 -> t2._1(),
								t2 -> t2._2(),
								(a, b) -> a, // (can't happen(
								() -> new LinkedHashMap<String, SharedLibraryBean>()))
				);
	}
	
	/** Combine the analytic thread level results and the per-job results into a single reply
	 * @param top_level
	 * @param per_job
	 * @param source
	 * @return
	 */
	protected final static CompletableFuture<BucketActionReplyMessage> combineResults(
			final CompletableFuture<BasicMessageBean> top_level,
			final List<CompletableFuture<BasicMessageBean>> per_job,
			final String source
			)
	{
		if (per_job.isEmpty()) {
			return top_level.thenApply(reply -> new BucketActionHandlerMessage(source, reply));
		}
		else { // slightly more complex:
		
			// First off wait for them all to complete:
			final CompletableFuture<?>[] futures = per_job.toArray(new CompletableFuture<?>[0]);
			
			return top_level.thenCombine(CompletableFuture.allOf(futures), (thread, __) -> {
				List<BasicMessageBean> replies = Stream.concat(
						Lambdas.get(() -> {
							if (thread.success() 
									&& ((null == thread.message()) || thread.message().isEmpty()))
							{
								// Ignore top level, it's not very interesting
								return Stream.empty();
							}
							else return Stream.of(thread);
						})
						,
						per_job.stream().map(cf -> cf.join())
					
				)
				.collect(Collectors.toList())
				;
				
				return (BucketActionReplyMessage) new BucketActionCollectedRepliesMessage(source, replies, Collections.emptySet(), Collections.emptySet());
			})
			.exceptionally(t -> {
				return (BucketActionReplyMessage) new BucketActionHandlerMessage(source, ErrorUtils.buildErrorMessage(DataBucketAnalyticsChangeActor.class.getSimpleName(), source, ErrorUtils.getLongForm("{0}", t)));					
			})
			;					
		}
	}
	
	/** Wraps the communications with the tech module so that calls to completeExceptionally are handled
	 * @param bucket
	 * @param m
	 * @param source
	 * @param context
	 * @param err_or_tech_module - the tech module (is ignored unless the user code got called ie implies err_or_tech_module.isRight)
	 * @param return_value - either the user return value or a wrap of the exception
	 * @return
	 */
	public static final CompletableFuture<BucketActionReplyMessage> handleTechnologyErrors(
			final DataBucketBean bucket, 
			final BucketActionMessage m,
			final String source,
			final Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> err_or_tech_module, 
			final CompletableFuture<BucketActionReplyMessage> return_value // "pipeline element"
					)
	{
		if (return_value.isCompletedExceptionally()) { // Harvest Tech developer called completeExceptionally, ugh
			try {				
				return_value.get(); // (causes an exception)
			}
			catch (Throwable t) { // e.getCause() is the exception we want
				// Note if we're here then err_or_tech_module must be "right"
				return CompletableFuture.completedFuture(
						new BucketActionHandlerMessage(source, SharedErrorUtils.buildErrorMessage(source, m,
							ErrorUtils.getLongForm(AnalyticsErrorUtils.NO_TECHNOLOGY_NAME_OR_ID, t.getCause(), m.bucket().full_name(), err_or_tech_module.success()._1().getClass()))));
			}
		}
		//(else fall through to...)
		return return_value;
	}
	
	//TODO (ALEPH-12): Some other things to include here
	// - all the custom trigger module names and ids
	// - document deduplication names and ids (TODO: also need to port this over in the harvest technology logic)
	
	/** Creates a query component to get all the shared library beans i need
	 * @param bucket
	 * @param cache_tech_jar_only
	 * @return
	 */
	protected static QueryComponent<SharedLibraryBean> getQuery(
			final DataBucketBean bucket, 
			final boolean cache_tech_jar_only)
	{
		final String technology = getAnalyticsTechnologyName(bucket).get(); //(non-empty by construction)
		final SingleQueryComponent<SharedLibraryBean> tech_query = 
				CrudUtils.anyOf(SharedLibraryBean.class)
					.when(SharedLibraryBean::_id, technology)
					.when(SharedLibraryBean::path_name, technology);
		
		final Stream<SingleQueryComponent<SharedLibraryBean>> other_libs = cache_tech_jar_only 
			? Stream.empty()
			: Optionals.ofNullable(bucket.analytic_thread().jobs()).stream()
				.flatMap(a_job ->
						Stream.concat(
								Optional.ofNullable(a_job.module_name_or_id()).map(Stream::of).orElse(Stream.empty())
								,
								Optionals.ofNullable(a_job.library_names_or_ids()).stream()
						))
				.map(name -> {
					return CrudUtils.anyOf(SharedLibraryBean.class)
							.when(SharedLibraryBean::_id, name)
							.when(SharedLibraryBean::path_name, name);
				});

		return CrudUtils.<SharedLibraryBean>anyOf(Stream.concat(Stream.of(tech_query), other_libs));
	}	

	/** Inefficient but safe utility for sending update events to the trigger sibling
	 * @param job_results
	 * @param bucket
	 * @param grouping_lambda - returns the job type based on the job and return value
	 * @param me_sibling
	 */
	protected static <T> void sendOnTriggerEventMessages(final List<Tuple2<AnalyticThreadJobBean, CompletableFuture<T>>> job_results,
												final DataBucketBean bucket,
												final Function<Tuple2<AnalyticThreadJobBean, T>, Optional<JobMessageType>> grouping_lambda,
												final Tuple2<ActorRef, ActorSelection> me_sibling,
												final ILoggingService _logging_service)
	{
		if (null == me_sibling) return; // (just keeps bw compatibility with the various test cases we currently have - won't get encountered in practice)

		// Perform the processing even if 1+ if the jobs fails - that job will just be flat wrapped out
		CompletableFuture.allOf(job_results.stream().map(j_f -> j_f._2()).toArray(CompletableFuture<?>[]::new)).thenAccept(__ -> {			
			sendOnTriggerEventMessages_phase2(job_results, bucket, grouping_lambda, me_sibling, _logging_service);
		})
		.exceptionally(__ -> {			
			sendOnTriggerEventMessages_phase2(job_results, bucket, grouping_lambda, me_sibling, _logging_service);
			return null;
		})
		;
	}	

	/** Inefficient but safe utility for sending update events to the trigger sibling
	 * @param job_results
	 * @param bucket
	 * @param grouping_lambda - returns the job type based on the job and return value
	 * @param me_sibling
	 */
	protected static <T> void sendOnTriggerEventMessages_phase2(final List<Tuple2<AnalyticThreadJobBean, CompletableFuture<T>>> job_results,
			final DataBucketBean bucket,
			final Function<Tuple2<AnalyticThreadJobBean, T>, Optional<JobMessageType>> grouping_lambda,
			final Tuple2<ActorRef, ActorSelection> me_sibling,
			final ILoggingService _logging_service)
	{
		final Map<Optional<JobMessageType>, List<Tuple2<AnalyticThreadJobBean, T>>> completed_jobs = 
				job_results.stream()
					.filter(j_f -> _batch_types.contains(j_f._1().analytic_type())) // (never allow streaming types to go to the triggers)
					.flatMap(Lambdas.flatWrap_i(j_f -> Tuples._2T(j_f._1(), j_f._2().get())))
					.collect(Collectors.
								groupingBy((Tuple2<AnalyticThreadJobBean, T> j_f) -> grouping_lambda.apply(j_f)))
					;
		
		completed_jobs.entrySet().stream()
					.filter(kv -> kv.getKey().isPresent())
					.forEach(kv -> {
						if (!kv.getValue().isEmpty()) {
							_logging_service.getSystemLogger(bucket).log(Level.INFO, ErrorUtils.lazyBuildMessage(false, ()->DataBucketAnalyticsChangeActor.class.getSimpleName(), ()->"sendOnTriggerEventMessages_phase2", ()->null, ()->ErrorUtils.get("Forwarding bucket information to {0}: bucket {1} event {2}",
									me_sibling._2(), bucket.full_name(), kv.getKey().get()
									), ()->Collections.emptyMap()));							
							
							final BucketActionMessage.BucketActionAnalyticJobMessage fwd_msg = 
									new BucketActionMessage.BucketActionAnalyticJobMessage(bucket, 
											kv.getValue().stream().map(j_f -> j_f._1()).collect(Collectors.toList()), 
											kv.getKey().get());
							me_sibling._2().tell(new AnalyticTriggerMessage(fwd_msg), me_sibling._1());
						}				
					});								
	}
}
