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
