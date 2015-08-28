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
package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.services.BatchEnrichmentContext;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;

public class LocalBeJobLauncher extends BeJobLauncher {
    @SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(LocalBeJobLauncher.class);


    
	@Inject 
	public LocalBeJobLauncher(GlobalPropertiesBean globals,BeJobLoader beJobLoader, BatchEnrichmentContext batchEnrichmentContext) {
		super(globals,beJobLoader,batchEnrichmentContext);
	}


	@Override
	public Configuration getConf() {
		if (configuration == null) {

			this.configuration = new Configuration(true);
			configuration.setBoolean("mapred.used.genericoptionsparser", true); // (just stops an annoying warning from appearing)
			configuration.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");									
			configuration.set("mapred.job.tracker", "local");
			configuration.set("fs.defaultFS", "file:///");
			configuration.unset("mapreduce.framework.name");
		}
		return configuration;
	}


	@Override
	public void launch(Job job) throws ClassNotFoundException, IOException, InterruptedException {
		job.waitForCompletion(true);
		//super.launch(job);
	}


	
}
