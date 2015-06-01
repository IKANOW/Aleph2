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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_import_manager.harvest.utils.HarvestErrorUtils;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.utils.ClassloaderUtils;
import com.ikanow.aleph2.data_import_manager.utils.JarCacheUtils;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestTechnologyModule;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils.MethodNamingHelper;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Patterns;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionOfferMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionHandlerMessage;

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
	    		.match(BucketActionMessage.class, 
		    		m -> {
		    			_logger.info(ErrorUtils.get("Actor {0} received message {1} from {2}", this.self(), m.getClass().getSimpleName(), this.sender()));
		    			
		    			final ActorRef closing_sender = this.sender();
		    			final ActorRef closing_self = this.self();
		    					    			
	    				final String hostname = _context.getInformationService().getHostname();
		    			try {
		    				final boolean harvest_tech_only = m instanceof BucketActionOfferMessage;
		    				
		    				cacheJars(m.bucket(), harvest_tech_only, _management_db, _globals, _fs, hostname, m)
		    					.thenCompose(err_or_map -> {
		    						
									final IHarvestContext h_context = _context.getNewHarvestContext();
									
									final Either<BasicMessageBean, IHarvestTechnologyModule> err_or_tech_module = 
											getHarvestTechnology(m.bucket(), harvest_tech_only, m, hostname, err_or_map);
									
									return talkToHarvester(m.bucket(), m, hostname, h_context, err_or_tech_module);
		    					})
		    					.thenAccept(reply -> { // (reply can contain an error or successful reply, they're the same bean type)
									closing_sender.tell(reply,  closing_self);		    						
		    					})
		    					;
		    			}
		    			catch (Throwable e) { // (trying to use Either to avoid this, but just in case...)
		    				final BasicMessageBean error_bean = 
		    						HarvestErrorUtils.buildErrorMessage(hostname, m,
		    								ErrorUtils.getLongForm(HarvestErrorUtils.ERROR_CACHING_SHARED_LIBS, e, m.bucket().full_name())
		    								);
		    				closing_sender.tell(new BucketActionHandlerMessage(hostname, error_bean), closing_self);			    				
		    			}
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
	protected static Either<BasicMessageBean, IHarvestTechnologyModule> getHarvestTechnology(
			final @NonNull DataBucketBean bucket, 
			boolean harvest_tech_only,
			final @NonNull BucketActionMessage m, 
			final @NonNull String source,
			final @NonNull Either<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>> err_or_libs // "pipeline element"
			)
	{
		return err_or_libs.<Either<BasicMessageBean, IHarvestTechnologyModule>>either(
				//Error:
				error -> Either.left(error)
				,
				// Normal
				libs -> {
					final Tuple2<SharedLibraryBean, String> libbean_path = libs.get(bucket.harvest_technology_name_or_id());
					if ((null == libbean_path) || (null == libbean_path._2())) { // Nice easy error case, probably can't ever happen
						return Either.left(
								HarvestErrorUtils.buildErrorMessage(source, m,
										HarvestErrorUtils.SHARED_LIBRARY_NAME_NOT_FOUND, bucket.full_name(), bucket.harvest_technology_name_or_id()));
					}
					
					final List<String> other_libs = harvest_tech_only 
							? Collections.emptyList() 
							: libs.values().stream().map(lp -> lp._2()).collect(Collectors.toList());
					
					final Either<BasicMessageBean, IHarvestTechnologyModule> ret_val = 
							ClassloaderUtils.getFromCustomClasspath(IHarvestTechnologyModule.class, 
									libbean_path._1().misc_entry_point(), 
									Optional.of(libbean_path._2()),
									other_libs,
									source, m);
					
					return ret_val;
				});
	}
	
	//
	
	/** Make various requests of the harvester based on the message type
	 * @param bucket
	 * @param tech_module
	 * @param m
	 * @return - a future containing the reply or an error (they're the same type at this point hence can discard the Either finally)
	 */
	protected static CompletableFuture<BucketActionReplyMessage> talkToHarvester(
			final @NonNull DataBucketBean bucket, 
			final @NonNull BucketActionMessage m,
			final @NonNull String source,
			final @NonNull IHarvestContext context,
			final @NonNull Either<BasicMessageBean, IHarvestTechnologyModule> err_or_tech_module // "pipeline element"
			)
	{
		try {
			return err_or_tech_module.<CompletableFuture<BucketActionReplyMessage>>either(
				//Error:
				error -> CompletableFuture.completedFuture(new BucketActionHandlerMessage(source, error))
				,
				// Normal
				tech_module -> 
					Patterns.match(m).<CompletableFuture<BucketActionReplyMessage>>andReturn()
						.when(BucketActionMessage.BucketActionOfferMessage.class, msg -> {
							tech_module.onInit(context);
							final boolean accept_or_ignore = tech_module.canRunOnThisNode(bucket, context);
							return CompletableFuture.completedFuture(accept_or_ignore
									? new BucketActionReplyMessage.BucketActionWillAcceptMessage(source)
									: new BucketActionReplyMessage.BucketActionIgnoredMessage(source));
						})
						.when(BucketActionMessage.DeleteBucketActionMessage.class, msg -> {
							tech_module.onInit(context);
							return tech_module.onDelete(bucket, context)
									.thenApply(reply -> new BucketActionHandlerMessage(source, reply));
						})
						.when(BucketActionMessage.NewBucketActionMessage.class, msg -> {
							tech_module.onInit(context);
							return tech_module.onNewSource(bucket, context, msg.is_suspended())  
									.thenApply(reply -> new BucketActionHandlerMessage(source, reply));
						})
						.when(BucketActionMessage.UpdateBucketActionMessage.class, msg -> {
							tech_module.onInit(context);
							return tech_module.onUpdatedSource(msg.old_bucket(), bucket, msg.is_enabled(), Optional.empty(), context)
									.thenApply(reply -> new BucketActionHandlerMessage(source, reply));
						})
						.when(BucketActionMessage.UpdateBucketStateActionMessage.class, msg -> {
							tech_module.onInit(context);
							return (msg.is_suspended()
									? tech_module.onSuspend(bucket, context)
									: tech_module.onResume(bucket, context))
										.thenApply(reply -> new BucketActionHandlerMessage(source, reply));
						})
						.otherwise(msg -> { // return "command not recognized" error
							tech_module.onInit(context);
							return CompletableFuture.completedFuture(
									new BucketActionHandlerMessage(source, HarvestErrorUtils.buildErrorMessage(source, m,
										HarvestErrorUtils.MESSAGE_NOT_RECOGNIZED, 
											bucket.full_name(), m.getClass().getSimpleName())));
						})
				);
		}
		catch (Throwable e) { // (trying to use Either to avoid this, but just in case...)
			return CompletableFuture.completedFuture(
					new BucketActionHandlerMessage(source, HarvestErrorUtils.buildErrorMessage(source, m,
						ErrorUtils.getLongForm(HarvestErrorUtils.ERROR_LOADING_CLASS, e, err_or_tech_module.right().value().getClass()))));  
		}		
	}
	
	/** Given a bucket ...returns either - a future containing the first error encountered, _or_ a map (both name and id as keys) of path names 
	 * (and guarantee that the file has been cached when the future completes)
	 * @param bucket
	 * @param cache_tech_jar_only
	 * @param management_db
	 * @param globals
	 * @param fs
	 * @param handler_for_errors
	 * @param msg_for_errors
	 * @return  a future containing the first error encountered, _or_ a map (both name and id as keys) of path names 
	 */
	@SuppressWarnings("unchecked")
	protected static <M> CompletableFuture<Either<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>> 
		cacheJars(
				final DataBucketBean bucket, 
				final boolean cache_tech_jar_only,
				final @NonNull IManagementDbService management_db, 
				final @NonNull GlobalPropertiesBean globals,
				final @NonNull IStorageService fs, 
				final @NonNull String handler_for_errors, 
				final @NonNull M msg_for_errors
			)
	{
		try {
			MethodNamingHelper<SharedLibraryBean> helper = BeanTemplateUtils.from(SharedLibraryBean.class);
			final QueryComponent<SharedLibraryBean> spec = getQuery(bucket, cache_tech_jar_only);

			return management_db.getSharedLibraryStore().getObjectsBySpec(
					spec, 
					Arrays.asList(
						helper.field(SharedLibraryBean::_id), 
						helper.field(SharedLibraryBean::path_name), 
						helper.field(SharedLibraryBean::misc_entry_point)
					), 
					true)
					.thenComposeAsync(cursor -> {
						// This is a map of futures from the cache call - either an error or the path name
						// note we use a tuple of (id, name) as the key and then flatten out later 
						final Map<Tuple2<String, String>, Tuple2<SharedLibraryBean, CompletableFuture<Either<BasicMessageBean, String>>>> map_of_futures = 
							StreamSupport.stream(cursor.spliterator(), true)
								.filter(lib -> {
									return true;
								})
								.collect(Collectors.<SharedLibraryBean, Tuple2<String, String>, Tuple2<SharedLibraryBean, CompletableFuture<Either<BasicMessageBean, String>>>>
									toMap(
										// want to keep both the name and id versions - will flatten out below
										lib -> Tuples._2T(lib.path_name(), lib._id()), //(key)
										// spin off a future in which the file is being copied - save the shared library bean also
										lib -> Tuples._2T(lib, // (value) 
												JarCacheUtils.getCachedJar(globals.local_cached_jar_dir(), lib, fs, handler_for_errors, msg_for_errors))));
						
						// denest from map of futures to future of maps, also handle any errors here:
						// (some sort of "lift" function would be useful here - this are a somewhat inelegant few steps)
						
						final CompletableFuture<Either<BasicMessageBean, String>>[] futures = 
								(CompletableFuture<Either<BasicMessageBean, String>>[]) map_of_futures
								.values()
								.stream().map(t2 -> t2._2()).collect(Collectors.toList())
								.toArray(new CompletableFuture[0]);
						
						// (have to embed this thenApply instead of bringing it outside as part of the toCompose chain, because otherwise we'd lose map_of_futures scope)
						return CompletableFuture.allOf(futures).<Either<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>>thenApply(f -> {								
							try {
								final Map<String, Tuple2<SharedLibraryBean, String>> almost_there = map_of_futures.entrySet().stream()
									.flatMap(kv -> {
										final Either<BasicMessageBean, String> ret = kv.getValue()._2().join(); // (must have already returned if here
										return ret.<Stream<Tuple2<String, Tuple2<SharedLibraryBean, String>>>>
											either(
												//Error:
												err -> { throw new RuntimeException(err.message()); } // (not ideal, but will do)
												,
												// Normal:
												s -> { 
													return Arrays.asList(
														Tuples._2T(kv.getKey()._1(), Tuples._2T(kv.getValue()._1(), s)), // result object with path_name
														Tuples._2T(kv.getKey()._2(), Tuples._2T(kv.getValue()._1(), s))) // result object with id
															.stream();
												});
									})
									.collect(Collectors.<Tuple2<String, Tuple2<SharedLibraryBean, String>>, String, Tuple2<SharedLibraryBean, String>>
										toMap(
											idname_path -> idname_path._1(), //(key)
											idname_path -> idname_path._2() // (value)
											))
									;								
								return Either.<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>right(almost_there);
							}
							catch (Exception e) { // handle the exception thrown above containing the message bean from whatever the original error was!
								return Either.<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>left(
										HarvestErrorUtils.buildErrorMessage(handler_for_errors.toString(), msg_for_errors,
												e.getMessage()));
							}
						});
					});
		}
		catch (Exception e) {
			return CompletableFuture.completedFuture(
				Either.left(HarvestErrorUtils.buildErrorMessage(handler_for_errors.toString(), msg_for_errors,
					ErrorUtils.getLongForm(HarvestErrorUtils.ERROR_CACHING_SHARED_LIBS, e, bucket.full_name())
					)));
		}
	}


	/** Creates a query component to get all the shared library beans i need
	 * @param bucket
	 * @param cache_tech_jar_only
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected static QueryComponent<SharedLibraryBean> getQuery(
			final DataBucketBean bucket, 
			final boolean cache_tech_jar_only)
	{
		final SingleQueryComponent<SharedLibraryBean> tech_query = 
				CrudUtils.anyOf(SharedLibraryBean.class)
					.when(SharedLibraryBean::_id, bucket.harvest_technology_name_or_id())
					.when(SharedLibraryBean::path_name, bucket.harvest_technology_name_or_id());
		
		final List<SingleQueryComponent<SharedLibraryBean>> other_libs = cache_tech_jar_only 
			? Collections.emptyList()
			: Optionals.ofNullable(bucket.harvest_configs()).stream()
				.flatMap(hcfg -> Optionals.ofNullable(hcfg.library_ids_or_names()).stream())
				.map(name -> {
					return CrudUtils.anyOf(SharedLibraryBean.class)
							.when(SharedLibraryBean::_id, name)
							.when(SharedLibraryBean::path_name, name);
				})
				.collect(Collector.of(
						LinkedList::new,
						LinkedList::add,
						(left, right) -> { left.addAll(right); return left; }
						));

		return CrudUtils.<SharedLibraryBean>anyOf(tech_query,
				other_libs.toArray(new SingleQueryComponent[other_libs.size()]));
	}
}
