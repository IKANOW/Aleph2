/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
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
package com.ikanow.aleph2.aleph2_rest_utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService.Cursor;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IDataServiceProvider.IGenericDataService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.QueryComponent;
import com.ikanow.aleph2.data_model.utils.CrudUtils.UpdateComponent;
import com.ikanow.aleph2.data_model.utils.FutureUtils.ManagementFuture;

import fj.data.Either;

/**
 * Used to help build rest services wrapped around ICrudServices quickly.
 * Takes an ICrudService, Object to send, Object to receive, the rest servlet
 * and returns the servlet with an added api call.
 * 
 * @author Burch
 *
 */
public class RestUtils {
	//service types
	private static final String SERVICE_TYPE_DATA_SERVICE = "DATA_SERVICE";
	private static final String SERVICE_TYPE_MANAGEMENT_DB = "MANAGEMENT_DB";
	//access levels
	private static final String ACCESS_LEVEL_READ = "READ";
	private static final String ACCESS_LEVEL_WRITE = "WRITE";
	//data service types
	private static final String DATA_SERVICE_IDENTIFIER_SEARCH_INDEX = "SEARCH_INDEX";
	private static final String DATA_SERVICE_IDENTIFIER_STORAGE = "STORAGE";
	
	//util
	private static final String BUCKET_SPLIT = ",";
	
	//management db service types
	private static final String MANAGEMENT_SERVICE_BUCKET_STORE = "BUCKET_STORE";
	private static final String MANAGEMENT_SERVICE_BUCKET_STATUS = "BUCKET_STATUS";
	private static final String MANAGEMENT_SERVICE_SHARED_LIBRARY = "SHARED_LIBRARY";
	private static final String MANAGEMENT_SERVICE_BUCKET_DATA = "BUCKET_DATA";
	

	private static final ObjectMapper mapper = new ObjectMapper();
	private static final String BUCKET_BINARY_DIR = "/data/";
	
	
	private static Logger _logger = LogManager.getLogger();	
	
	/**
	 * Returns a crud service pointed at the passed in arguments
	 * @param <T>
	 * @param <T>
	 * 
	 * @param service_type
	 * @param access_level
	 * @param service_identifier
	 * @param multivaluedMap 
	 * @return
	 */
	public static <T> Either<String,Tuple2<ICrudService<T>, Class<T>>> getCrudService(final IServiceContext service_context, final String service_type, 
			final String access_level, final String service_identifier, final Optional<String> bucket_full_names ) {
		_logger.error("trying to get crud service for: " + service_type + " " + access_level + " " + service_identifier);
		if ( service_type.toUpperCase().equals(SERVICE_TYPE_DATA_SERVICE) ) {
			return bucket_full_names.<Either<String,Tuple2<ICrudService<T>, Class<T>>>>
					map(b->getDataService(service_context, access_level, service_identifier, b))
					.orElse(Either.left("Query param 'buckets' is required when trying to get a data_service"));
		} else if ( service_type.toUpperCase().equals(SERVICE_TYPE_MANAGEMENT_DB) ) {
			return getManagementDBService(service_context, access_level, service_identifier, bucket_full_names);
		} else {
			return Either.left("Service type did not match expected types: " + service_type);
		}
	}
	
	/**
	 * Management DB always returns a typed crud service
	 * 
	 * @param <T>
	 * @param service_context
	 * @param access_level
	 * @param service_identifier
	 * @param bucket_full_names 
	 * @return
	 */
	private static <T> Either<String,Tuple2<ICrudService<T>, Class<T>>> getManagementDBService(final IServiceContext service_context, final String access_level, final String service_identifier, final Optional<String> bucket_full_names) {
		_logger.error("trying to get management db service for: " + access_level + " " + service_identifier);
		final IManagementDbService db_service = service_context.getCoreManagementDbService();
		//figure out which sub_service to get
		switch ( service_identifier.toUpperCase() ) {
			case MANAGEMENT_SERVICE_BUCKET_STORE:
				if (access_level.toUpperCase().equals(ACCESS_LEVEL_READ)) {
					return Either.right(new Tuple2(db_service.getDataBucketStore().readOnlyVersion().getCrudService().get(), DataBucketBean.class));
				} else if (access_level.toUpperCase().equals(ACCESS_LEVEL_WRITE)) {
					_logger.error("returning writable db crud service");
					return Either.right(new Tuple2(db_service.getDataBucketStore(), DataBucketBean.class));
				} else {
					return Either.left("Access Level did not match expected types: " + access_level);
				}
				
			case MANAGEMENT_SERVICE_BUCKET_STATUS:
				if (access_level.toUpperCase().equals(ACCESS_LEVEL_READ)) {
					return Either.right(new Tuple2(db_service.getDataBucketStatusStore().readOnlyVersion().getCrudService().get(), DataBucketStatusBean.class));
				} else if (access_level.toUpperCase().equals(ACCESS_LEVEL_WRITE)) {
					return Either.right(new Tuple2(db_service.getDataBucketStatusStore(), DataBucketStatusBean.class));
				} else {
					return Either.left("Access Level did not match expected types: " + access_level);
				}				
				
			case MANAGEMENT_SERVICE_SHARED_LIBRARY:
				if (access_level.toUpperCase().equals(ACCESS_LEVEL_READ)) {
					return Either.left("Error: can only get a writable version of shared_library store TODO should I create a read version for retrieval of files?");
//					return Either.right(new Tuple2(db_service.getSharedLibraryStore().readOnlyVersion().getCrudService().get(), SharedLibraryBean.class));
				} else if (access_level.toUpperCase().equals(ACCESS_LEVEL_WRITE)) {
					return Either.right(new Tuple2(new SharedLibraryCrudServiceWrapper(db_service.getSharedLibraryStore(), service_context), SharedLibraryBean.class));
//					return Either.right(new Tuple2(db_service.getSharedLibraryStore(), SharedLibraryBean.class));
				} else {
					return Either.left("Access Level did not match expected types: " + access_level);
				}
				
			case MANAGEMENT_SERVICE_BUCKET_DATA:
				if (access_level.toUpperCase().equals(ACCESS_LEVEL_READ)) {
					return Either.left("Error: can only get a writable version of bucket_data store TODO should I create a read version for retrieval of files?");
				} else if (access_level.toUpperCase().equals(ACCESS_LEVEL_WRITE)) {
					return Either.right(new Tuple2(RestUtils.getBucketDataStore(service_context, convertToBucketFullName(service_context, bucket_full_names.get())), FileDescriptor.class));
				} else {
					return Either.left("Access Level did not match expected types: " + access_level);
				}
			//TODO handle shared lib data (e.g. write a wrapper that creates a shared_lib entry and stores an object using storage_service)
				
			//TODO handle analytic crap "management_db.{read|rw}.bucket_state:{harvest|enrich|analytics}:{bucket}:{name} = (rw, type, bucket, name) -> getCoreMgmtDb().getBucketState(type, bucket, name)[.rw]
				
			default:
				return Either.left("Service Identifier did not match expected types: " + service_identifier);
		}
	}	

	/**
	 * Dataservice are generic, we typically just get a jsonnode typed one
	 * 
	 * @param service_context
	 * @param access_level
	 * @param service_identifier
	 * @param clazz
	 * @return
	 */
	private static <T> Either<String,Tuple2<ICrudService<T>, Class<T>>> getDataService(final IServiceContext service_context, final String access_level, final String service_identifier, final String bucket_full_names) {
		_logger.error("trying to get data_service for: " + access_level + " " + service_identifier );
		//wants a buckets crud service, try to get the approriate crud
		final Either<String, IGenericDataService> either_g = getGenericDataService(service_context, service_identifier);
		if ( either_g.isLeft() ) 
			return Either.left(either_g.left().value());
		final IGenericDataService generic_data_service = either_g.right().value();
		
		if (access_level.toUpperCase().equals(ACCESS_LEVEL_READ)) {
			 //TODO add options if any are provided
			return Either.right(new Tuple2(generic_data_service.getReadableCrudService(JsonNode.class, convertToBucketFullNames(service_context, bucket_full_names), Optional.empty()).get().getCrudService().get(), JsonNode.class));
		} else if (access_level.toUpperCase().equals(ACCESS_LEVEL_WRITE)) {
			//TODO add options if any are provided
			if ( bucket_full_names.contains(BUCKET_SPLIT) )
				return Either.left("Can only get write access to a single bucket at a time (buckets had a '"+BUCKET_SPLIT+"' in it signifying multiple bucket names)");
			return Either.right(new Tuple2(generic_data_service.getWritableDataService(JsonNode.class, convertToBucketFullName(service_context, bucket_full_names), Optional.empty(), Optional.empty()).get().getCrudService().get(), JsonNode.class));
		} else {
			return Either.left("Access Level did not match expected types: " + access_level);
		}
	}
	
	/**
	 * Convert a string full_name into a DataBucket e.g. take /my/bucket/name and return the DataBucketBean
	 * @param service_identifier
	 * @return
	 */
	public static DataBucketBean convertToBucketFullName(final IServiceContext service_context, final String bucket_full_name) {
		_logger.error("Looking up data bucket for : " + bucket_full_name);
		//TODO should I store this globally so I don't have to grab it everytime?
		final IManagementCrudService<DataBucketBean> bucket_store = service_context.getCoreManagementDbService().getDataBucketStore();
		ManagementFuture<Optional<DataBucketBean>> future = bucket_store.getObjectBySpec(CrudUtils.allOf(DataBucketBean.class).when(DataBucketBean::full_name, bucket_full_name));
		try {
			return future.get().get();
		} catch (Exception e) {
			_logger.error(ErrorUtils.getLongForm("Error trying to find bucket: {1} Error: {0}", e, bucket_full_name));
		}
		//TODO probably shouldn't return null, handle this differently
		return null;
	}

	/**
	 * @param service_identifier
	 * @return
	 */
	public static Collection<DataBucketBean> convertToBucketFullNames(final IServiceContext service_context, final String bucket_full_names) {		
		return Arrays.stream(bucket_full_names.split(BUCKET_SPLIT)).map(b->convertToBucketFullName(service_context, b)).collect(Collectors.toSet());
	}

	private static Either<String, IGenericDataService> getGenericDataService(final IServiceContext service_context, final String service_identifier ) {
		_logger.error("Getting generic service for data_service: " + service_identifier);
		//parse service type
		switch ( service_identifier.toUpperCase() ) {
			case DATA_SERVICE_IDENTIFIER_SEARCH_INDEX:
				return Either.right(service_context.getSearchIndexService().get().getDataService().get());
			case DATA_SERVICE_IDENTIFIER_STORAGE:
				return Either.right(service_context.getStorageService().getDataService().get());
			default:
				return Either.left("Service Identifier did not match expected types: " + service_identifier);
		}
	}
	
	public static ICrudService<FileDescriptor> getBucketDataStore(final IServiceContext service_context, final DataBucketBean bucket) {
		return new DataStoreCrudService(service_context, getBucketOutputDir(service_context, bucket));
	}
	
	public static DataStoreCrudService getSharedLibraryDataStore(final IServiceContext service_context) {
		return new DataStoreCrudService(service_context, getSharedLibraryOutputDir(service_context));
	}
	private static String getSharedLibraryOutputDir(final IServiceContext service_context) {
		final String output_dir = (service_context.getGlobalProperties().distributed_root_dir() + GlobalPropertiesBean.SHARED_LIBRARY_ROOT_OFFSET).replaceAll("//", "/");
		return output_dir;
	}
	
	private static String getBucketOutputDir(final IServiceContext service_context, final DataBucketBean bucket) {
		//create the path to bucket output
		//TODO something like globals.hdfs_location + bucket.full_name + /data/		
		final String output_dir = (service_context.getGlobalProperties().distributed_root_dir() + GlobalPropertiesBean.BUCKET_DATA_ROOT_OFFSET + bucket.full_name() + BUCKET_BINARY_DIR).replaceAll("//", "/");
//		_logger.error("WANT TO OUTPUT DIR TO: " + output_dir);
//		return "/tmp/burch/";
		return output_dir;
	}



	public static <T> QueryComponent<T> convertStringToQueryComponent(final String json, final Class<T> clazz, Optional<Long> limit) throws JsonParseException, JsonMappingException, IOException {		
		final QueryComponentBean toConvert = mapper.readValue(json, QueryComponentBean.class); //first convert to simple bean format
		return QueryComponentBeanUtils.convertQueryComponentBeanToComponent(toConvert, clazz, limit); //then convert to component format
	}
	
	public static <T> Tuple2<QueryComponent<T>, UpdateComponent<T>> convertStringToUpdateComponent(final String json, final Class<T> clazz) throws JsonParseException, JsonMappingException, IOException {
		final UpdateComponentBean toConvert = mapper.readValue(json, UpdateComponentBean.class); //first convert to simple bean format
		return QueryComponentBeanUtils.convertUpdateComponentBeanToUpdateComponent(toConvert, clazz); //then convert to component format
	}
	
	public static JsonNode convertSingleObjectToJson(final Boolean value, final String field_name) {
		final ObjectNode json = mapper.createObjectNode();
		json.put(field_name, value);
		return json;
	}
	
	public static JsonNode convertSingleObjectToJson(final String value, final String field_name) {
		final ObjectNode json = mapper.createObjectNode();
		json.put(field_name, value);
		return json;
	}
	
	public static JsonNode convertSingleObjectToJson(final Long value, final String field_name) {
		final ObjectNode json = mapper.createObjectNode();
		json.put(field_name, value);
		return json;
	}
	
	public static <T> JsonNode convertCursorToJson(final Cursor<T> cursor) throws JsonProcessingException {	
		final Collection<T> list = new ArrayList<T>(); 
		cursor.forEach(a->list.add(a));
		return BeanTemplateUtils.toJson(list);
	}
	
	public static <T> JsonNode convertObjectToJson(final T obj) throws JsonProcessingException {	
		return BeanTemplateUtils.toJson(obj);
	}
	
	public static JsonNode convertStringToJson(final String json) throws JsonProcessingException, IOException {
		return mapper.readTree(json);
	}

	/**
	 * Returns back an empty optional if value is null or empty, otherwise
	 * returns back an optional with value.
	 * 
	 * This is used because Jersey returns empty Strings for params that aren't supplied
	 * 
	 * @param value The value to put in an optional
	 * @return
	 */
	public static Optional<String> getOptional(final String value) {
		if ( value == null || value.isEmpty() )
			return Optional.empty();
		return Optional.of(value);
	}
	
	public static Optional<Long> getOptional(final Long value) {
		if ( value == null )
			return Optional.empty();
		return Optional.of(value);
	}
	
}
