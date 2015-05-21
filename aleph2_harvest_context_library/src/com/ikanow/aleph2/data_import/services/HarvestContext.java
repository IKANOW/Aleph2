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
package com.ikanow.aleph2.data_import.services;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_access.IServiceContext;
import com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketStatusBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.distributed_services.services.ICoreDistributedServices;

public class HarvestContext implements IHarvestContext {

	//TODO: have a method that grabs the right bucket and throws an exception if both specified?
	
	public enum State { IN_TECHNOLOGY, IN_MODULE };
	protected final State _state;
	
	protected IServiceContext _service_context;
	
	protected IManagementDbService _core_management_db;
	protected ICoreDistributedServices _distributed_services;
	
	/**Guice injector
	 * @param service_context
	 */
	@Inject 
	HarvestContext(IServiceContext service_context) {
		_state = State.IN_TECHNOLOGY;
		_service_context = service_context;
		_core_management_db = service_context.getManagementDbService(); // (actually returns the _core_ management db service)
		_distributed_services = service_context.getService(ICoreDistributedServices.class, Optional.empty());
	}
	
	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getService(java.lang.Class, java.util.Optional)
	 */
	@Override
	public <I> @NonNull Optional<I> getService(@NonNull Class<I> service_clazz,
			@NonNull Optional<String> service_name) {
		return Optional.ofNullable(_service_context.getService(service_clazz, service_name));
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#sendObjectToStreamingPipeline(java.util.Optional, com.fasterxml.jackson.databind.JsonNode)
	 */
	@Override
	public void sendObjectToStreamingPipeline(
			@NonNull Optional<DataBucketBean> bucket, @NonNull JsonNode object) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#sendObjectToStreamingPipeline(java.util.Optional, java.lang.Object)
	 */
	@Override
	public <T> void sendObjectToStreamingPipeline(
			@NonNull Optional<DataBucketBean> bucket, @NonNull T object) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#sendObjectToStreamingPipeline(java.util.Optional, java.util.Map)
	 */
	@Override
	public void sendObjectToStreamingPipeline(
			@NonNull Optional<DataBucketBean> bucket,
			@NonNull Map<String, Object> object) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getHarvestContextLibraries(java.util.Optional)
	 */
	@Override
	public @NonNull List<String> getHarvestContextLibraries(
			@NonNull Optional<Set<Class<?>>> services) {
		// TODO Auto-generated method stub
		
		// Consists of:
		// 1) This library
		// 2) Libraries that are always needed:
		//    - core distributed services
		//    - ?? management db (core + underlying?)
		//    - data model
		// 3) Any libraries associated with the services (TODO is that even possible?_
		
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getHarvestContextSignature(java.util.Optional)
	 */
	@Override
	public @NonNull String getHarvestContextSignature(
			@NonNull Optional<DataBucketBean> bucket, @NonNull Optional<Set<Class<?>>> services) {
		// TODO Auto-generated method stub
		
		// Returns a config object containing:
		// - set up for any of the services described
		// - all the rest of the configuration
		// - the bean ID
		
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getGlobalHarvestTechnologyConfiguration()
	 */
	@Override
	public @NonNull CompletableFuture<JsonNode> getGlobalHarvestTechnologyConfiguration() {
		//TODO (ALEPH-19): Fill this in later ... not actually sure I understand where this lives?
		// There is a harvest technology crud store? Should I just be using that?
		throw new RuntimeException("This operation is not currently supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getHarvestLibraries(java.util.Optional)
	 */
	@Override
	public @NonNull CompletableFuture<Map<String, String>> getHarvestLibraries(
			@NonNull Optional<DataBucketBean> bucket) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getBucketObjectStore(java.lang.Class, java.util.Optional, java.util.Optional, boolean)
	 */
	@Override
	public <S> @NonNull ICrudService<S> getBucketObjectStore(
			@NonNull Class<S> clazz, @NonNull Optional<DataBucketBean> bucket,
			@NonNull Optional<String> sub_collection, boolean auto_apply_prefix)
	{
		//TODO (ALEPH-19): Fill this in later
		//TODO (or can i just knock this out quickly now?)
		throw new RuntimeException("This operation is not currently supported");
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getBucketStatus(java.util.Optional)
	 */
	@Override
	public @NonNull CompletableFuture<DataBucketStatusBean> getBucketStatus(
			@NonNull Optional<DataBucketBean> bucket) {
		//TODO
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#logStatusForBucketOwner(java.util.Optional, com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean, boolean)
	 */
	@Override
	public void logStatusForBucketOwner(
			@NonNull Optional<DataBucketBean> bucket,
			@NonNull BasicMessageBean message, boolean roll_up_duplicates) 
	{
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#logStatusForBucketOwner(java.util.Optional, com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean)
	 */
	@Override
	public void logStatusForBucketOwner(
			@NonNull Optional<DataBucketBean> bucket,
			@NonNull BasicMessageBean message) {
		logStatusForBucketOwner(bucket, message, true);		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getTempOutputLocation(java.util.Optional)
	 */
	@Override
	public @NonNull String getTempOutputLocation(
			@NonNull Optional<DataBucketBean> bucket) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#getFinalOutputLocation(java.util.Optional)
	 */
	@Override
	public @NonNull String getFinalOutputLocation(
			@NonNull Optional<DataBucketBean> bucket) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#emergencyDisableBucket(java.util.Optional)
	 */
	@Override
	public void emergencyDisableBucket(@NonNull Optional<DataBucketBean> bucket) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#emergencyQuarantineBucket(java.util.Optional, java.lang.String)
	 */
	@Override
	public void emergencyQuarantineBucket(
			@NonNull Optional<DataBucketBean> bucket,
			@NonNull String quarantine_duration) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_import.IHarvestContext#initializeNewContext(java.lang.String)
	 */
	@Override
	public void initializeNewContext(@NonNull String signature) {
		// TODO Auto-generated method stub
		
	}

}
