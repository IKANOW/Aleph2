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
 ******************************************************************************/
package com.ikanow.aleph2.data_model.objects.shared;

import org.checkerframework.checker.nullness.qual.Nullable;

public class GlobalPropertiesBean {

	public static final String PROPERTIES_ROOT = "Globals"; 	
	public static final String __DEFAULT_LOCAL_ROOT_DIR = "/opt/aleph2-home/"; 	
	public static final String __DEFAULT_LOCAL_YARN_CONFIG_DIR = "/opt/aleph2-home/yarn-config/"; 	
	public static final String __DEFAULT_LOCAL_CACHED_JARS_DIR = "/opt/aleph2-home/cached-jars/"; 	
	public static final String __DEFAULT_DISTRIBUTED_ROOT_DIR = "/app/aleph2/"; 	
	
	/** User constructor
	 * @param local_root_dir
	 * @param local_yarn_config_dir
	 */
	public GlobalPropertiesBean(final @Nullable String local_root_dir, 
			final @Nullable String local_yarn_config_dir, final @Nullable String local_cached_jar_dir, 
			final @Nullable String distributed_root_dir
			)
	{
		this.local_root_dir = local_root_dir;
		this.local_yarn_config_dir = local_yarn_config_dir;
		this.local_cached_jar_dir = local_cached_jar_dir;
		this.distributed_root_dir = distributed_root_dir;
	}
	
	/** Constructor for Jackson
	 */
	protected GlobalPropertiesBean() {}
	
	/** This is the root for all local directories
	 * @return
	 */
	public String local_root_dir() { return null == local_root_dir ? __DEFAULT_LOCAL_ROOT_DIR : local_root_dir; }
	
	/** This is the root for all distributed directories
	 * @return
	 */
	public String distributed_root_dir() { return null == distributed_root_dir ? __DEFAULT_DISTRIBUTED_ROOT_DIR : distributed_root_dir; }
	
	/** Use this for all Hadoop/YARN related configuration files
	 * @return the local root directory
	 */
	public String local_yarn_config_dir() { return null == local_yarn_config_dir ? __DEFAULT_LOCAL_YARN_CONFIG_DIR : local_yarn_config_dir; }	
	
	/** This contains the location of locally cached JAR files
	 * @return the local root directory
	 */
	public String local_cached_jar_dir() { return null == local_cached_jar_dir ? __DEFAULT_LOCAL_CACHED_JARS_DIR : local_cached_jar_dir; }	
	
	private String local_root_dir;
	private String local_yarn_config_dir;
	private String local_cached_jar_dir;
	private String distributed_root_dir;
}
