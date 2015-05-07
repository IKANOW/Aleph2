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
package com.ikanow.aleph2.test_utils;

public class LoadConfigUtility {
	
	/**
	 * This test suite is for letting other applications test adding their services to our
	 * platform and making sure all regression tests pass before they throw it on a live cluster.
	 * 
	 * Because we are using typesafe config files, you can override the config for these test cases
	 * in a couple ways to add in your own widget: https://github.com/typesafehub/config#standard-behavior
	 * 
	 * 1. You can submit system properties overriding your specific service
	 * 2. You can manually change the application.conf in this class
	 * 
	 */
	
	//TODO do we need to pass our config file from here to the other components so they use the overridden fields?
}
