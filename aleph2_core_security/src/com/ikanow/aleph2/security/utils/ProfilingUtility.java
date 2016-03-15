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
package com.ikanow.aleph2.security.utils;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This is a helper class for measuring time (and memory) consumption of a program.
 * @author JFreydan
 */
public class ProfilingUtility
{
	private static final Logger logger = LogManager.getLogger(ProfilingUtility.class);

	private HashMap<String,Long> times = null;	
  private static ProfilingUtility instance;
  private ProfilingUtility(){
  	times = new HashMap<String,Long>();
  }

  public static synchronized ProfilingUtility getInstance(){
  	if(instance == null){
  		instance = new ProfilingUtility();
  	}
  	return instance;
  }
    /** 
     * This method start the time measurement for one key (method Name) , e.g. if you want to track a time, e.g. before a function entry.
     * @param key
     */
	public static synchronized void timeStart(String key){		
		getInstance().times.put(key, new Long(System.currentTimeMillis()));
	}

	
	/** 
	 * This method start the time measurement for one key (method Name) , e.g. if you want to track a time, e.g. before a function entry.
	 * @param key
	 */
	public static void timeStopAndLog(String key){
		long timeStop = System.currentTimeMillis();
		Long timeStart = (Long)getInstance().times.get(key);
		long timeUsed =0;
		if(timeStart!=null){
			timeUsed = timeStop - timeStart.longValue();
		}
		logger.debug("Profiled: "+key+"="+timeUsed);
	}
  
}
