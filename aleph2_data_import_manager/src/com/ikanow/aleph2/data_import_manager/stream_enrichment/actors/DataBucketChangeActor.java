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

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.LinkedList;
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


























import com.google.common.collect.Maps;
import com.ikanow.aleph2.analytics.services.AnalyticsContext;
import com.ikanow.aleph2.core.shared.utils.SharedErrorUtils;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.stream_enrichment.utils.StreamErrorUtils;
import com.ikanow.aleph2.data_import_manager.utils.LibraryCacheUtils;
import com.ikanow.aleph2.core.shared.utils.ClassloaderUtils;
import com.ikanow.aleph2.core.shared.utils.JarCacheUtils;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyModule;
import com.ikanow.aleph2.data_model.interfaces.data_analytics.IAnalyticsTechnologyService;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentStreamingModule;
import com.ikanow.aleph2.data_model.interfaces.data_import.IEnrichmentStreamingTopology;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadBean;
import com.ikanow.aleph2.data_model.objects.data_analytics.AnalyticThreadJobBean;
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
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionOfferMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionHandlerMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionCollectedRepliesMessage;

import fj.data.Either;
import fj.data.Validation;

/** This actor is responsible for supervising the job of handling changes to data
 *  buckets on the "data import manager" end - specifically vs streaming enrichment (see harvest.DataBucketChangeActor for harvest related control)
 * @author acp
 */
/**
 * @author Alex
 *
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
	protected final Optional<IAnalyticsTechnologyService> _stream_analytics_tech;
	protected final SetOnce<AnalyticsContext> _stream_analytics_context = new SetOnce<>();
	
	// Streaming enrichment handling:
	public static final Optional<String> STREAMING_ENRICHMENT_DEFAULT = Optional.of("StreamingEnrichmentService");
	public static final String STREAMING_ENRICHMENT_TECH_NAME = STREAMING_ENRICHMENT_DEFAULT.get();
	
	/** The actor constructor - at some point all these things should be inserted by injection
	 */
	public DataBucketChangeActor() {
		_context = DataImportActorContext.get(); 
		_core_distributed_services = _context.getDistributedServices();
		_actor_system = _core_distributed_services.getAkkaSystem();
		_management_db = _context.getServiceContext().getCoreManagementDbService().readOnlyVersion();
		_globals = _context.getGlobalProperties();
		_fs = _context.getServiceContext().getStorageService();
		
		_stream_analytics_tech = _context.getServiceContext().getService(IAnalyticsTechnologyService.class, STREAMING_ENRICHMENT_DEFAULT);
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
	    				}	    				
	    				
		    			_logger.info(ErrorUtils.get("Actor {0} received message {1} from {2} bucket {3}", this.self(), m.getClass().getSimpleName(), this.sender(), m.bucket().full_name()));

		    			final ActorRef closing_sender = this.sender();
		    			final ActorRef closing_self = this.self();		    			
						
	    				final String hostname = _context.getInformationService().getHostname();
		    			
						// (this isn't async so doesn't require any futures)
						
	    				final boolean accept_or_ignore =
		    				 _stream_analytics_tech.map(tech -> {
		    					 tech.onInit(_stream_analytics_context.get());
		    					 return tech.canRunOnThisNode(m.bucket(), Collections.emptyList(), _stream_analytics_context.get());
		    				 })
		    				 .orElseGet(() -> {
		    					 _logger.warn(ErrorUtils.get("Actor {0} received streaming enrichment offer for {1} but it is not configured on this node", this.self(), m.bucket().full_name()));
		    					 return false;
		    				 });
	    				
						final BucketActionReplyMessage reply = 						
							accept_or_ignore
									? new BucketActionReplyMessage.BucketActionWillAcceptMessage(hostname)
									: new BucketActionReplyMessage.BucketActionIgnoredMessage(hostname);
									
						closing_sender.tell(reply,  closing_self);
	    			})
	    		.match(BucketActionMessage.class, 
		    		m -> {
		    			
		    			_logger.info(ErrorUtils.get("Actor {0} received message {1} from {2} bucket {3}", this.self(), m.getClass().getSimpleName(), this.sender(), m.bucket().full_name()));
		    			
		    			final BucketActionMessage final_msg = Lambdas.get(() -> {
		    				if (isEnrichmentRequest(m)) {
		    					return convertEnrichmentToAnalytics(m);
		    				}
		    				else return m;
		    			});
		    			handleActionRequest(final_msg);
		    		})
	    		.build();
	 }

	////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////
	
	// TOP LEVEL MESSAGE PROCESSING

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
								message, hostname, err_or_map);
				
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
				
				final CompletableFuture<BucketActionReplyMessage> ret = talkToAnalytics(final_bucket, message, hostname, a_context, 
																			err_or_map.toOption().orSome(Collections.emptyMap()), 
																			err_or_tech_module);
				
				return handleTechnologyErrors(final_bucket, message, hostname, err_or_tech_module, ret);
				
			})
			.thenAccept(reply -> { // (reply can contain an error or successful reply, they're the same bean type)	    						
				// Some information logging:
				Patterns.match(reply).andAct()
					.when(BucketActionHandlerMessage.class, msg -> _logger.info(ErrorUtils.get("Standard reply to message={0}, bucket={1}, success={2}", 
							message.getClass().getSimpleName(), message.bucket().full_name(), msg.reply().success())))
					.when(BucketActionReplyMessage.BucketActionWillAcceptMessage.class, 
							msg -> _logger.info(ErrorUtils.get("Standard reply to message={0}, bucket={1}", message.getClass().getSimpleName(), message.bucket().full_name())))
					.otherwise(msg -> _logger.info(ErrorUtils.get("Unusual reply to message={0}, type={2}, bucket={1}", message.getClass().getSimpleName(), message.bucket().full_name(), msg.getClass().getSimpleName())));
				
				closing_sender.tell(reply,  closing_self);		    						
			})
			.exceptionally(e -> { // another bit of error handling that shouldn't ever be called but is a useful backstop
				// Some information logging:
				_logger.warn(ErrorUtils.get("Unexpected error replying to '{0}': error = {1}, bucket={2}", BeanTemplateUtils.toJson(message).toString(), ErrorUtils.getLongForm("{0}", e), message.bucket().full_name()));
				
				final BasicMessageBean error_bean = 
						SharedErrorUtils.buildErrorMessage(hostname, message,
								ErrorUtils.getLongForm(StreamErrorUtils.STREAM_UNKNOWN_ERROR, e, message.bucket().full_name())
								);
				closing_sender.tell(new BucketActionHandlerMessage(hostname, error_bean), closing_self);			    				
				return null;
			})
			;		
	}
	
	////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////

	// STREAMING ENRICHMENT SPECIAL CASE
	
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
					.with(BucketActionMessage::bucket, convertStreamingEnrichmentToAnalyticBucket(message.bucket()))
					.done();
	}

	/** Converts a bucket with only streaming enrichment settings into one that has an analytic thread dervied
	 * @param bucket
	 * @return
	 */
	protected static DataBucketBean convertStreamingEnrichmentToAnalyticBucket(final DataBucketBean bucket) {
		
		if ((null == bucket.streaming_enrichment_topology()) || !isEnrichmentType(bucket))
		{
			return bucket;
		}
		else {
			final EnrichmentControlMetadataBean enrichment =  Optional.ofNullable(bucket.streaming_enrichment_topology().enabled()).orElse(false)
					? bucket.streaming_enrichment_topology()
					: BeanTemplateUtils.build(EnrichmentControlMetadataBean.class)
						.done().get()
					;			
			
			final AnalyticThreadJobBean.AnalyticThreadJobInputBean input =
					new AnalyticThreadJobBean.AnalyticThreadJobInputBean(
							true, //(enabled) 
							"", // (myself) 
							"stream", 
							null, // (no filter)
							null // (no extra config)
							);		
			
			final AnalyticThreadJobBean.AnalyticThreadJobOutputBean output =
					new AnalyticThreadJobBean.AnalyticThreadJobOutputBean(
							false, // (not used for streaming) 
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
	}
	
	/** Quick utility to determine if a bucket has a streaming type
	 * @param bucket
	 * @return
	 */
	private static boolean isEnrichmentType(final DataBucketBean bucket) {
		return _streaming_types.contains(Optional.ofNullable(bucket.master_enrichment_type()).orElse(MasterEnrichmentType.none));
	}
	private static final EnumSet<MasterEnrichmentType> _streaming_types = EnumSet.of(MasterEnrichmentType.streaming, MasterEnrichmentType.streaming_and_batch);
	
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
							&& isEnrichmentType(bucket) // it is an enrichment bucket
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
			final BucketActionMessage m, 
			final String source,
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
						
						// Special case: streaming enrichment classpath
						//TODO (ALEPH-12: handle general "on classpath" case)
						final Tuple2<String, String> entrypoint_path = Lambdas.get(() -> {
							return Optional.ofNullable(libs.get(technology_name_or_id)) 
										.map(bean_path -> Tuples._2T(bean_path._1().misc_entry_point(), bean_path._2()))
									.orElseGet(() -> {
										return Patterns.match(technology_name_or_id).<Tuple2<String, String>>andReturn()
											.when(t -> STREAMING_ENRICHMENT_TECH_NAME.equals(t), __ -> {
												try { 
													return Tuples._2T(streaming_enrichment.get().getClass().getName(), null); 
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
			final Map<String, Tuple2<SharedLibraryBean, String>> libs, // (if we're here then must be valid)
			final Validation<BasicMessageBean, Tuple2<IAnalyticsTechnologyModule, ClassLoader>> err_or_tech_module // "pipeline element"
			)
	{
		final List<AnalyticThreadJobBean> jobs = bucket.analytic_thread().jobs();
		
		final ClassLoader saved_current_classloader = Thread.currentThread().getContextClassLoader();		
		try {			
			return err_or_tech_module.<CompletableFuture<BucketActionReplyMessage>>validation(
				//Error:
				error -> CompletableFuture.completedFuture(new BucketActionHandlerMessage(source, error))
				,
				// Normal
				techmodule_classloader -> {
					final IAnalyticsTechnologyModule tech_module = techmodule_classloader._1();
					_logger.info("Set active classloader=" + techmodule_classloader._2() + " class=" + tech_module.getClass() + " message=" + m.getClass().getSimpleName() + " bucket=" + bucket.full_name());
					Thread.currentThread().setContextClassLoader(techmodule_classloader._2());
					
					return Patterns.match(m).<CompletableFuture<BucketActionReplyMessage>>andReturn()
						.when(BucketActionMessage.BucketActionOfferMessage.class, msg -> {
							tech_module.onInit(context);
							final boolean accept_or_ignore = tech_module.canRunOnThisNode(bucket, jobs, context);
							return CompletableFuture.completedFuture(accept_or_ignore
									? new BucketActionReplyMessage.BucketActionWillAcceptMessage(source)
									: new BucketActionReplyMessage.BucketActionIgnoredMessage(source));
						})
						.when(BucketActionMessage.DeleteBucketActionMessage.class, msg -> {
							tech_module.onInit(context);
							
							final CompletableFuture<BasicMessageBean> top_level_result = tech_module.onDeleteThread(bucket, jobs, context);
							final List<CompletableFuture<BasicMessageBean>> job_results = jobs.stream()
									.peek(job -> setPerJobContextParams(job, context, libs)) //(WARNING: mutates context)
									.map(job -> tech_module.stopAnalyticJob(bucket, jobs, job, context))
									.collect(Collectors.toList());
							
							return combineResults(top_level_result, job_results, source);
						})
						.when(BucketActionMessage.NewBucketActionMessage.class, msg -> {
							tech_module.onInit(context);

							final CompletableFuture<BasicMessageBean> top_level_result = tech_module.onNewThread(bucket, jobs, context, !msg.is_suspended());
							final List<CompletableFuture<BasicMessageBean>> job_results =
									msg.is_suspended()
									? Collections.emptyList()
									: jobs.stream()
										.peek(job -> setPerJobContextParams(job, context, libs)) //(WARNING: mutates context)
										.map(job -> tech_module.startAnalyticJob(bucket, jobs, job, context))
										.collect(Collectors.toList());
							
							return combineResults(top_level_result, job_results, source);
						})
						.when(BucketActionMessage.UpdateBucketActionMessage.class, msg -> {
							tech_module.onInit(context);

							final CompletableFuture<BasicMessageBean> top_level_result = tech_module.onUpdatedThread(msg.old_bucket(), bucket, jobs, msg.is_enabled(), Optional.empty(), context);
							final List<CompletableFuture<BasicMessageBean>> job_results =
									jobs.stream()
										.peek(job -> setPerJobContextParams(job, context, libs)) //(WARNING: mutates context)
										.map(job -> msg.is_enabled()												
											? tech_module.resumeAnalyticJob(bucket, jobs, job, context)
											: tech_module.suspendAnalyticJob(bucket, jobs, job, context)
											)
										.collect(Collectors.toList());
							
							return combineResults(top_level_result, job_results, source);
						})
						.when(BucketActionMessage.PurgeBucketActionMessage.class, msg -> {
							tech_module.onInit(context);
							
							final CompletableFuture<BasicMessageBean> top_level_result = tech_module.onPurge(bucket, jobs, context);
							
							return combineResults(top_level_result, Collections.emptyList(), source);
						})
						.when(BucketActionMessage.TestBucketActionMessage.class, msg -> {
							tech_module.onInit(context);
							
							final CompletableFuture<BasicMessageBean> top_level_result = tech_module.onTestThread(bucket, jobs, msg.test_spec(), context);
							final List<CompletableFuture<BasicMessageBean>> job_results = jobs.stream()
										.peek(job -> setPerJobContextParams(job, context, libs)) //(WARNING: mutates context)
										.map(job -> tech_module.startAnalyticJobTest(bucket, jobs, job, msg.test_spec(), context))
										.collect(Collectors.toList());
							
							return combineResults(top_level_result, job_results, source);
						})
						.otherwise(msg -> { // return "command not recognized" error
							tech_module.onInit(context);
							return CompletableFuture.completedFuture(
									new BucketActionHandlerMessage(source, SharedErrorUtils.buildErrorMessage(source, m,
										StreamErrorUtils.MESSAGE_NOT_RECOGNIZED, 
											bucket.full_name(), m.getClass().getSimpleName())));
						});
				});
		}
		catch (Throwable e) { // (trying to use Validation to avoid this, but just in case...)
			return CompletableFuture.completedFuture(
					new BucketActionHandlerMessage(source, SharedErrorUtils.buildErrorMessage(source, m,
						ErrorUtils.getLongForm(SharedErrorUtils.ERROR_LOADING_CLASS, e, err_or_tech_module.success().getClass()))));
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
		Optional.ofNullable(job.module_name_or_id())
				.map(module_name -> libs.get(module_name))
				.map(t2 -> {
					return context.resetModuleConfig(t2._1());
				})
		.orElseGet(() -> context.resetModuleConfig(null)); // (unsets if can't find the module)
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
				
				return (BucketActionReplyMessage) new BucketActionCollectedRepliesMessage(source, replies, Collections.emptySet());
			})
			.exceptionally(t -> {
				return (BucketActionReplyMessage) new BucketActionHandlerMessage(source, ErrorUtils.buildErrorMessage(DataBucketChangeActor.class.getSimpleName(), source, ErrorUtils.getLongForm("{0}", t)));					
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
							ErrorUtils.getLongForm(StreamErrorUtils.NO_TECHNOLOGY_NAME_OR_ID, t.getCause(), m.bucket().full_name(), err_or_tech_module.success()._1().getClass()))));
			}
		}
		//(else fall through to...)
		return return_value;
	}
	
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
}
