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

import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_import_manager.harvest.utils.HarvestErrorUtils;
import com.ikanow.aleph2.data_import_manager.services.DataImportActorContext;
import com.ikanow.aleph2.data_import_manager.utils.ClassloaderUtils;
import com.ikanow.aleph2.data_import_manager.utils.JarCacheUtils;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestTechnologyModule;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.Optionals;
import com.ikanow.aleph2.data_model.utils.Tuples;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionMessage.BucketActionOfferMessage;
import com.ikanow.aleph2.management_db.data_model.BucketActionReplyMessage.BucketActionHandlerMessage;

import fj.Unit;
import fj.data.Either;
import scala.PartialFunction;
import scala.Tuple2;
import scala.runtime.BoxedUnit;
import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.japi.pf.ReceiveBuilder;

/** This actor is responsible for supervising the job of handling changes to data
 *  buckets on the "data import manager" end
 * @author acp
 */
public class DataBucketChangeActor extends AbstractActor {

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
	    				final String hostname = _context.getInformationService().getHostname();
		    			try {
		    				final boolean harvest_tech_only = m instanceof BucketActionOfferMessage;
		    				
		    				cacheJars(m.bucket(), harvest_tech_only, _management_db, _globals, _fs, hostname, m)
		    					.thenCompose(err_map -> {		    						
		    						err_map.either(
		    								//error
		    								error -> {
		    									this.sender().tell(new BucketActionHandlerMessage(hostname, error),  this.sender());
		    									return Unit.unit();
		    								}
		    								,
		    								//normal
		    								map -> talkToHarvester_topLevel(m.bucket(), map, harvest_tech_only, m, hostname)
		    										.thenCompose(result -> {
		    											this.sender().tell(new BucketActionHandlerMessage(hostname, result),  this.sender());
		    											return null; //TODO how do i get rid of this
		    										}));
		    						
		    						return null;//TODO how do i get rid of this
		    					});
		    			}
		    			catch (Exception e) { // (trying to use Either to avoid this, but just in case...)
		    				final BasicMessageBean error_bean = 
		    						HarvestErrorUtils.buildErrorMessage(hostname, m,
		    								ErrorUtils.getLongForm(HarvestErrorUtils.ERROR_CACHING_SHARED_LIBS, e, m.bucket().full_name())
		    								);
		    				this.sender().tell(new BucketActionHandlerMessage(hostname, error_bean),  this.sender());			    				
		    			}
		    		})
	    		.build();
	 }
		
	////////////////////////////////////////////////////////////////////////////
	
	// Utility code
	
	/** Talks to the harvest tech module - this top level function just sets the classloader up and creates the module,
	 *  then calls talkToHarvester_actuallyTalk to do the talking
	 * @param bucket
	 * @param libs
	 * @param harvest_tech_only
	 * @param m
	 * @param source
	 * @return
	 */
	protected static CompletableFuture<BasicMessageBean> talkToHarvester_topLevel(
			final @NonNull DataBucketBean bucket, final @NonNull Map<String, Tuple2<SharedLibraryBean, String>> libs, boolean harvest_tech_only,
			final @NonNull BucketActionMessage m, final @NonNull String source
			)
	{
		final Tuple2<SharedLibraryBean, String> libbean_path = libs.get(bucket.harvest_technology_name_or_id());
		if ((null == libbean_path) || (null == libbean_path._2())) { // Nice easy error case, probably can't ever happen
			return CompletableFuture.completedFuture(
					HarvestErrorUtils.buildErrorMessage(source, m,
							ErrorUtils.get(HarvestErrorUtils.SHARED_LIBRARY_NAME_NOT_FOUND), bucket.full_name(), bucket.full_name()));
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
						
		return ret_val.either(
				//Error:
				error -> CompletableFuture.completedFuture(error),
				// Normal
				tech_module -> talkToHarvester_actuallyTalk(bucket, tech_module, m));
	}
	
	/** Make various requests of the harvester based on the message type
	 * @param bucket
	 * @param tech_module
	 * @param m
	 * @return
	 */
	protected static CompletableFuture<BasicMessageBean> talkToHarvester_actuallyTalk(
			final @NonNull DataBucketBean bucket, final @NonNull IHarvestTechnologyModule tech_module,
			final @NonNull BucketActionMessage m
			)
	{
		//TODO: need to go grab the harvest context
		return null;
	}
	
	/** Returns either - a future containing the first error encountered, _or_ a map (both name and id as keys) of path names 
	 * (and guarantee that the file has been cached when the future comples)
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
	protected static <M> CompletableFuture<Either<BasicMessageBean, Map<String, Tuple2<SharedLibraryBean, String>>>> cacheJars(final DataBucketBean bucket, final boolean cache_tech_jar_only,
			final @NonNull IManagementDbService management_db, final @NonNull GlobalPropertiesBean globals,
			 final @NonNull IStorageService fs, final @NonNull String handler_for_errors, final @NonNull M msg_for_errors
			)
	{
		try {
			final QueryComponent<SharedLibraryBean> spec = getQuery(bucket, cache_tech_jar_only);
			
			return management_db.getSharedLibraryStore().getObjectsBySpec(spec, Arrays.asList("_id", "path_name", "misc_entry_point"), true)
					.thenComposeAsync(cursor -> {
						// This is a map of futures from the cache call - either an error or the path name
						// note we use a tuple of (id, name) as the key and then flatten out later 
						final Map<Tuple2<String, String>, Tuple2<SharedLibraryBean, CompletableFuture<Either<BasicMessageBean, String>>>> map_of_futures = 
							StreamSupport.stream(cursor.spliterator(), true)
								.collect(Collectors.<SharedLibraryBean, Tuple2<String, String>, Tuple2<SharedLibraryBean, CompletableFuture<Either<BasicMessageBean, String>>>>
									toMap(
										// want to keep both the name and id versions - will flatten out below
										lib -> Tuples._2T(lib.path_name(), lib._id()), //(key)
										// spin off a future in which the file is being copied - save the shared library bean also
										lib -> Tuples._2T(lib, // (value) 
												JarCacheUtils.getCachedJar(globals.local_cached_jar_dir(), lib, fs, handler_for_errors, msg_for_errors))));
						
						// denest from map of futures to future of maps, also handle any errors here:
						
						CompletableFuture<Either<BasicMessageBean, String>>[] futures = 
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
														s -> 
														Arrays.asList(
															Tuples._2T(kv.getKey()._1(), Tuples._2T(kv.getValue()._1(), s)), 
															Tuples._2T(kv.getKey()._2(), Tuples._2T(kv.getValue()._1(), s)))
																.stream()
														);
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
	protected static QueryComponent<SharedLibraryBean> getQuery(final DataBucketBean bucket, final boolean cache_tech_jar_only) {
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
