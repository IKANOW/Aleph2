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
package com.ikanow.aleph2.data_model.objects.data_analytics;

public class AnalyticThreadBean {

	//TODO so the thread looks like
	// analytic engine (MapReduce/Spark/Storm) -> module (eg sliding window aggregation) -> thread (set of modules within an engine + config)
	// (Compare to import .. analytic engine <-> ?? / analytic module <-> harvester / bucket <->  analytic thread
	//TODO: hmm suppose I want to run Flume as the overall technology, but I want to run different 
	// java within my different flume instances? i need some way to manage my "JAR" files
	
	// Each analytic has a bunch of generic parameters:
	// - The layer on which to run
	// - The analytic module
	// - Start conditions (permanent/periodic/dependent on another job)
	// - Exit conditions
	
	//TODO: list/graph of analytics with some sort og exit criteria
}
