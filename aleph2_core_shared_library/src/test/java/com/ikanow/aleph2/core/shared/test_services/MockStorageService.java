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
package com.ikanow.aleph2.core.shared.test_services;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import scala.Tuple2;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.StorageSchemaBean;
import com.ikanow.aleph2.data_model.objects.shared.BasicMessageBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.Tuples;

/** Cut down version of the storage service - for testing its admin functions only, not data storage
 * @author Alex
 */
public class MockStorageService implements IStorageService {
	protected static final Logger _logger = LogManager.getLogger();	

	final protected GlobalPropertiesBean _globals;
	
	
	@Inject 
	public MockStorageService(GlobalPropertiesBean globals) {
		_globals = globals;	
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.storage_service_hdfs.services.HdfsStorageService#getRootPath()
	 */
	@Override
	public String getRootPath() {		
		return _globals.distributed_root_dir();
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.storage_service_hdfs.services.HdfsStorageService#getBucketRootPath()
	 */
	@Override
	public String getBucketRootPath() {		
		return getRootPath() + "/data/";
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Optional<T> getUnderlyingPlatformDriver(
			Class<T> driver_class, Optional<String> driver_options) {
		T driver = null;
		try {
			if(driver_class!=null){
				if(driver_class.isAssignableFrom(FileContext.class)){
					FileContext fs = FileContext.getLocalFSFileContext(new Configuration());
					return (Optional<T>) Optional.of(fs);
				}
				else if(driver_class.isAssignableFrom(RawLocalFileSystem.class)){
					return Optional.of(driver_class.newInstance());
				}
				else if (driver_class.isAssignableFrom(AbstractFileSystem.class)) {
					FileContext fs = FileContext.getLocalFSFileContext(new Configuration());
					return (Optional<T>) Optional.of(fs.getDefaultFileSystem());
				}

			} // !=null
		} catch (Exception e) {
			_logger.error("Caught Exception:",e);
		}
		return Optional.ofNullable(driver);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.shared_services.IUnderlyingService#getUnderlyingArtefacts()
	 */
	@Override
	public Collection<Object> getUnderlyingArtefacts() {
		return Arrays.asList(this);
	}

	/* (non-Javadoc)
	 * @see com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService#validateSchema(com.ikanow.aleph2.data_model.objects.data_import.DataSchemaBean.StorageSchemaBean, com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean)
	 */
	@Override
	public Tuple2<String, List<BasicMessageBean>> validateSchema(
			StorageSchemaBean schema, DataBucketBean bucket) {
		return Tuples._2T("", Collections.emptyList());
	}
}
