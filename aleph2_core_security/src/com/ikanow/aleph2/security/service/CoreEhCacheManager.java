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
package com.ikanow.aleph2.security.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.cache.ehcache.EhCacheManager;

/**
 * This class is a wrapper singleton class to the ehCache manage instance. It us used to prevent duplicate creation by guice modules.
 * @author jfreydank
 *
 */
public class CoreEhCacheManager {
	private static CoreEhCacheManager coreEhCacheManagerInstance;
	private static final Logger logger = LogManager.getLogger(CoreEhCacheManager.class);

	private EhCacheManager ehCacheManagerInstance = null;
	
	private CoreEhCacheManager(){
    	logger.debug("CoreEhCacheManager() constructor");		
	}

	public static synchronized CoreEhCacheManager getInstance(){
		if(coreEhCacheManagerInstance == null){
			coreEhCacheManagerInstance = new CoreEhCacheManager();
		}
    	logger.debug("CoreEhCacheManager.getInstance():"+coreEhCacheManagerInstance);
		return coreEhCacheManagerInstance;
	}
	
	public EhCacheManager getCacheManager(){
		if(ehCacheManagerInstance==null){
	    	System.setProperty("ehcache.disk.store.dir", System.getProperty("java.io.tmpdir") +"/shiro-cache-" + System.getProperty("user.name"));
			ehCacheManagerInstance = new EhCacheManager();
			ehCacheManagerInstance.setCacheManagerConfigFile("classpath:ehcache.xml");
		}
    	logger.debug("CoreEhCacheManager.getCacheManager:"+ ehCacheManagerInstance);
		return ehCacheManagerInstance; 
    }
    

/*	public void bindCacheManager(PrivateBinder binder) {
		if(ehCacheManagerInstance==null){
	    	System.setProperty("ehcache.disk.store.dir", System.getProperty("java.io.tmpdir") +"/shiro-cache-" + System.getProperty("user.name"));
			ehCacheManagerInstance = new EhCacheManager();
			ehCacheManagerInstance.setCacheManagerConfigFile("classpath:ehcache.xml");

			binder.bind(CacheManager.class).toInstance(ehCacheManagerInstance);		
	    	binder.expose(CacheManager.class);
		}		
	}
*/
}
