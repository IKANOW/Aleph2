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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ikanow.aleph2.core.shared.utils.SharedErrorUtils;
import com.ikanow.aleph2.data_import.services.HarvestContext;
import com.ikanow.aleph2.data_import_manager.data_model.DataImportConfigurationBean;
import com.ikanow.aleph2.data_import_manager.harvest.utils.HarvestErrorUtils;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.utils.BeanDiffUtils;
import com.ikanow.aleph2.data_import_manager.utils.LibraryCacheUtils;
import com.ikanow.aleph2.data_import_manager.utils.PatternUtils;
import com.ikanow.aleph2.core.shared.utils.ClassloaderUtils;
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
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionOfferMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionHandlerMessage;

import fj.data.Validation;
import scala.PartialFunction;
import scala.Tuple2;
import scala.runtime.BoxedUnit;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.japi.pf.ReceiveBuilder;

/** This actor is responsible for supervising the job of handling changes to data
 *  buckets on the "data import manager" end - specifically vs harvest (see stream_enrichment.DataBucketChangeActor for streaming enrichment related control)
 * @author acp
 */
public class DataBucketHarvestChangeActor extends AbstractActor {
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
	public DataBucketHarvestChangeActor() {
		_context = DataImportActorContext.get(); 
		_core_distributed_services = _context.getDistributedServices();
		_actor_system = _core_distributed_services.getAkkaSystem();
		_management_db = _context.getServiceContext().getCoreManagementDbService().readOnlyVersion();
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
	    		.match(BucketActionMessage.class, 
		    		m -> {
		    			_logger.info(ErrorUtils.get("Actor {0} received message {1} from {2} bucket {3}", this.self(), m.getClass().getSimpleName(), this.sender(), m.bucket().full_name()));
		    			
		    			final ActorRef closing_sender = this.sender();
		    			final ActorRef closing_self = this.self();
		    					    			
	    				final String hostname = _context.getInformationService().getHostname();
	    				final boolean harvest_tech_only = m instanceof BucketActionOfferMessage;
		    				
	    				// (cacheJars can't throw checked or unchecked in this thread, only from within exceptions)
	    				LibraryCacheUtils.cacheJars(m.bucket(), getQuery(m.bucket(), harvest_tech_only), _management_db, _globals, _fs, _context.getServiceContext(), hostname, m)
	    					.thenCompose(err_or_map -> {
	    						
								final HarvestContext h_context = _context.getNewHarvestContext();
								
								final Validation<BasicMessageBean, IHarvestTechnologyModule> err_or_tech_module = 
										getHarvestTechnology(m.bucket(), harvest_tech_only, m, hostname, err_or_map);

								// set the library bean - note if here then must have been set, else IHarvestTechnologyModule wouldn't exist 
								err_or_map.forEach(map -> {								
									Optional.ofNullable(map.get(m.bucket().harvest_technology_name_or_id()))
										.ifPresent(lib -> h_context.setTechnologyConfig(lib._1()));
									
									// Set module configs:
									final Map<String, SharedLibraryBean> module_configs = Optional.ofNullable(m.bucket().harvest_configs())
												.orElse(Collections.emptyList())
												.stream()
												.filter(hcfg -> null != hcfg.module_name_or_id())
												.map(hcfg -> Tuples._2T(hcfg.module_name_or_id(), map.get(hcfg.module_name_or_id())))
												.collect(Collectors.toMap(t2 -> t2._1(), t2 -> t2._2()._1()));
									
									h_context.setLibraryConfigs(module_configs);
								});
								
								final CompletableFuture<BucketActionReplyMessage> ret = talkToHarvester(m.bucket(), m, hostname, h_context, err_or_tech_module, _context);
								return handleTechnologyErrors(m.bucket(), m, hostname, err_or_tech_module, ret);
								
	    					})
	    					.thenAccept(reply -> { // (reply can contain an error or successful reply, they're the same bean type)	    						
	    						// Some information logging:
	    						Patterns.match(reply).andAct()
	    							.when(BucketActionHandlerMessage.class, __ -> m instanceof BucketActionOfferMessage, 
	    									msg -> _logger.warn(ErrorUtils.get("Unusual reply to BucketActionOfferMessage: bucket={0}, success={1} error={2}", 
	    	    									m.bucket().full_name(), msg.reply().success(), msg.reply().message())))
	    							.when(BucketActionHandlerMessage.class, msg -> _logger.info(ErrorUtils.get("Standard reply to message={0}, bucket={1}, success={2}", 
	    									m.getClass().getSimpleName(), m.bucket().full_name(), msg.reply().success())))
	    							.when(BucketActionReplyMessage.BucketActionWillAcceptMessage.class, 
	    									msg -> _logger.info(ErrorUtils.get("Standard reply to message={0}, bucket={1}", m.getClass().getSimpleName(), m.bucket().full_name())))
	    							.otherwise(msg -> _logger.info(ErrorUtils.get("Unusual reply to message={0}, type={2}, bucket={1}", m.getClass().getSimpleName(), m.bucket().full_name(), msg.getClass().getSimpleName())));
	    						
								closing_sender.tell(reply,  closing_self);		    						
	    					})
	    					.exceptionally(e -> { // another bit of error handling that shouldn't ever be called but is a useful backstop
	    						// Some information logging:
	    						_logger.warn("Unexpected error replying to '{0}': error = {1}, bucket={2}", BeanTemplateUtils.toJson(m).toString(), ErrorUtils.getLongForm("{0}", e), m.bucket().full_name());
	    						
			    				final BasicMessageBean error_bean = 
			    						SharedErrorUtils.buildErrorMessage(hostname, m,
			    								ErrorUtils.getLongForm(HarvestErrorUtils.HARVEST_UNKNOWN_ERROR, e, m.bucket().full_name())
			    								);
			    				closing_sender.tell(new BucketActionHandlerMessage(hostname, error_bean), closing_self);			    				
	    						return null;
	    					})
	    					;
		    		})
	    		.build();
	 }
	
	////////////////////////////////////////////////////////////////////////////
	
	// Functional code
	
	/** Talks to the harvest tech module - this top level function just sets the classloader up and creates the module,
	 *  then calls talkToHarvester_actuallyTalk to do the talking
	 * @param bucket
	 * @param libs
	 * @param harvest_tech_only
	 * @param m
	 * @param source
	 * @return
	 */
	protected static Validation<BasicMessageBean, IHarvestTechnologyModule> getHarvestTechnology(
			final DataBucketBean bucket, 
			boolean harvest_tech_only,
			final BucketActionMessage m, 
			final String source,
			final Validation<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>> err_or_libs // "pipeline element"
			)
	{
		try {
			return err_or_libs.<Validation<BasicMessageBean, IHarvestTechnologyModule>>validation(
					//Error:
					error -> Validation.fail(error)
					,
					// Normal
					libs -> {
						final Tuple2<SharedLibraryBean, String> libbean_path = libs.get(bucket.harvest_technology_name_or_id());
						if ((null == libbean_path) || (null == libbean_path._2())) { // Nice easy error case, probably can't ever happen
							return Validation.fail(
									SharedErrorUtils.buildErrorMessage(source, m,
											SharedErrorUtils.SHARED_LIBRARY_NAME_NOT_FOUND, bucket.full_name(), bucket.harvest_technology_name_or_id()));
						}
						
						final List<String> other_libs = harvest_tech_only 
								? Collections.emptyList() 
								: libs.values().stream().map(lp -> lp._2()).collect(Collectors.toList());
						
						final Validation<BasicMessageBean, IHarvestTechnologyModule> ret_val = 
								ClassloaderUtils.getFromCustomClasspath(IHarvestTechnologyModule.class, 
										libbean_path._1().misc_entry_point(), 
										Optional.of(libbean_path._2()),
										other_libs,
										source, m);
						
						return ret_val;
					});
		}
		catch (Throwable t) {
			return Validation.fail(
					SharedErrorUtils.buildErrorMessage(source, m,
						ErrorUtils.getLongForm(SharedErrorUtils.ERROR_LOADING_CLASS, t, bucket.harvest_technology_name_or_id())));  
			
		}
	}
	
	//
	
	/** Make various requests of the harvester based on the message type
	 * @param bucket
	 * @param tech_module
	 * @param m
	 * @param context 
	 * @return - a future containing the reply or an error (they're the same type at this point hence can discard the Validation finally)
	 */
	protected static CompletableFuture<BucketActionReplyMessage> talkToHarvester(
			final DataBucketBean bucket, 
			final BucketActionMessage m,
			final String source,
			final IHarvestContext context,
			final Validation<BasicMessageBean, 
			IHarvestTechnologyModule> err_or_tech_module, // "pipeline element"
			final DataImportActorContext dim_context
			)
	{
		final ClassLoader saved_current_classloader = Thread.currentThread().getContextClassLoader();		
		try {			
			return err_or_tech_module.<CompletableFuture<BucketActionReplyMessage>>validation(
				//Error:
				error -> CompletableFuture.completedFuture(new BucketActionHandlerMessage(source, error))
				,
				// Normal
				tech_module -> {
					_logger.info("Set active classloader=" + tech_module.getClass().getClassLoader() + " class=" + tech_module.getClass() + " message=" + m.getClass().getSimpleName() + " bucket=" + bucket.full_name());					
					Thread.currentThread().setContextClassLoader(tech_module.getClass().getClassLoader());
					tech_module.onInit(context);
					
					// One final check before we do anything: are we allowed to run multi-node if we're trying
					//TODO (ALEPH-12): add test coverage for this
					if (Optional.ofNullable(bucket.multi_node_enabled()).orElse(false)) {
						if (!tech_module.supportsMultiNode(bucket, context)) {
							return CompletableFuture.completedFuture(
									new BucketActionHandlerMessage(source, SharedErrorUtils.buildErrorMessage(source, m,
										ErrorUtils.get(HarvestErrorUtils.TRIED_TO_RUN_MULTI_NODE_ON_UNSUPPORTED_TECH, bucket.full_name(), tech_module.getClass().getSimpleName()))));
						}
					}
					
					return Patterns.match(m).<CompletableFuture<BucketActionReplyMessage>>andReturn()
						.when(BucketActionMessage.BucketActionOfferMessage.class, msg -> {
							final boolean accept_or_ignore =
									canRunOnThisNode(bucket, dim_context) &&
									tech_module.canRunOnThisNode(bucket, context);
							return CompletableFuture.completedFuture(accept_or_ignore
									? new BucketActionReplyMessage.BucketActionWillAcceptMessage(source)
									: new BucketActionReplyMessage.BucketActionIgnoredMessage(source));
						})
						.when(BucketActionMessage.DeleteBucketActionMessage.class, msg -> {
							return tech_module.onDelete(bucket, context)
									.thenApply(reply -> new BucketActionHandlerMessage(source, reply));
						})
						.when(BucketActionMessage.NewBucketActionMessage.class, msg -> {
							return tech_module.onNewSource(bucket, context, !msg.is_suspended())  
									.thenApply(reply -> new BucketActionHandlerMessage(source, reply));
						})
						.when(BucketActionMessage.UpdateBucketActionMessage.class, msg -> {
							return tech_module.onUpdatedSource(msg.old_bucket(), bucket, msg.is_enabled(), BeanDiffUtils.createDiffBean(bucket, msg.old_bucket()), context)
									.thenApply(reply -> new BucketActionHandlerMessage(source, reply));
						})
						.when(BucketActionMessage.PurgeBucketActionMessage.class, msg -> {
							return tech_module.onPurge(msg.bucket(), context)
									.thenApply(reply -> new BucketActionHandlerMessage(source, reply));
						})
						.when(BucketActionMessage.TestBucketActionMessage.class, msg -> {
							return tech_module.onTestSource(bucket, msg.test_spec(), context)
									.thenApply(reply -> new BucketActionHandlerMessage(source, reply));
						})
						.when(BucketActionMessage.PollFreqBucketActionMessage.class, msg -> {
							return tech_module.onPeriodicPoll(bucket, context)
									.thenApply(reply -> new BucketActionHandlerMessage(source, reply));
						})
						.otherwise(msg -> { // return "command not recognized" error
							return CompletableFuture.completedFuture(
									new BucketActionHandlerMessage(source, SharedErrorUtils.buildErrorMessage(source, m,
										HarvestErrorUtils.MESSAGE_NOT_RECOGNIZED, 
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

	/**
	 * Checks to see if the current node matches the requested node rules
	 * @param bucket 
	 * 
	 * @return
	 */
	private static boolean canRunOnThisNode(final DataBucketBean bucket, 
			final DataImportActorContext context) {
		final DataImportConfigurationBean config = context.getDataImportConfigurationBean();		
		final Set<String> bucket_rules = bucket.node_list_rules() != null ? bucket.node_list_rules().stream().collect(Collectors.toSet()) : new HashSet<String>();

		//if we don't have any rules to follow, just allow it to run
		if ( bucket_rules.isEmpty() )
			return true;
		
		//check if the bucket rules match the config rules
		final String hostname = context.getInformationService().getHostname(); //this is my hostname for comparing globs/regex to
		//loop iteratively so we can kick out early if we find a match
		for ( final String rule : bucket_rules ) {
			if ( testNodeRule(rule, hostname, config.node_rules()) ) {
				return true;
			}
		}
		
		//we fell the whole way through, not a single rule was passed therefore
		return false;
	}

	/**
	 * Tests a node rule to see if it passes
	 * 1. EXCLUSIVE (starts with -) or INCLUSIVE (starts with + or something else)
	 * 2. glob or regex (/pattern/flags) for hostnames OR $(glob or regex) for rule
	 * 3. We pass if ANY rule is accepted
	 * 
	 * @param rule
	 * @param hostname
	 * @return
	 */
	private static boolean testNodeRule(final String rule, final String hostname, final Set<String> node_rules) {
		final boolean exclusive = rule.startsWith("-");		
		final String rule_wo_exclusive = rule.substring((rule.startsWith("-") || rule.startsWith("+")) ? 1 : 0); 
		final boolean is_node_rule = rule_wo_exclusive.startsWith("$");
		final String rule_wo_exclusive_hostname = rule_wo_exclusive.substring(rule_wo_exclusive.startsWith("$") ? 1 : 0);
		final Pattern pattern = PatternUtils.createPatternFromRegexOrGlob(rule_wo_exclusive_hostname);
		
		if ( is_node_rule ) {	
			//is node rule, check against all known node rules for a match		
			for ( String n_r : node_rules ) {
				//check if matches rule and we want to match on this (or opposite)
				if (pattern.matcher(n_r).find() == !exclusive)
					return true;
			}				
		} else {
			//is hostname rule, check if matches hostname and we want to match on this (or opposite)
			if (pattern.matcher(hostname).find() == !exclusive)
				return true;			
		}
		return false;
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
			final Validation<BasicMessageBean, IHarvestTechnologyModule> err_or_tech_module, 
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
							ErrorUtils.getLongForm(HarvestErrorUtils.HARVEST_TECH_ERROR, t.getCause(), m.bucket().full_name(), err_or_tech_module.success().getClass()))));
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
		final SingleQueryComponent<SharedLibraryBean> tech_query = 
				CrudUtils.anyOf(SharedLibraryBean.class)
					.when(SharedLibraryBean::_id, bucket.harvest_technology_name_or_id())
					.when(SharedLibraryBean::path_name, bucket.harvest_technology_name_or_id());
		
		final Stream<SingleQueryComponent<SharedLibraryBean>> other_libs = cache_tech_jar_only 
			? Stream.empty()
			: Optionals.ofNullable(bucket.harvest_configs()).stream()
				.flatMap(hcfg -> Stream.concat(
									Optional.ofNullable(hcfg.module_name_or_id()).map(Stream::of).orElse(Stream.empty())
									,
									Optionals.ofNullable(hcfg.library_names_or_ids()).stream()))
				.map(name -> {
					return CrudUtils.anyOf(SharedLibraryBean.class)
							.when(SharedLibraryBean::_id, name)
							.when(SharedLibraryBean::path_name, name);
				});

		return CrudUtils.<SharedLibraryBean>anyOf(Stream.concat(Stream.of(tech_query), other_libs));
	}
}
