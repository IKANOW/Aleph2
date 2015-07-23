package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.services.BatchEnrichmentContext;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.utils.DataBucketTest;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;

public class MiniClusterBeJobLauncher extends BeJobLauncher {
    private static final Logger logger = LogManager.getLogger(DataBucketTest.class);

    private MiniMRYarnCluster mrCluster;
    
	@Inject 
	public MiniClusterBeJobLauncher(GlobalPropertiesBean globals,BeJobLoader beJobLoader,BatchEnrichmentContext batchEnrichmentContext) {
		super(globals,beJobLoader,batchEnrichmentContext);
	}


	@Override
	public Configuration getConf() {
		if (configuration == null) {

			this.configuration = new Configuration(true);
			String stagingdir = configuration.get("yarn.app.mapreduce.am.staging-dir");
			logger.debug("staging dir:" + stagingdir);
			configuration.setBoolean(YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS, true);
			configuration.setBoolean(JHAdminConfig.MR_HISTORY_MINICLUSTER_FIXED_PORTS, true);

			try {
				mrCluster = new MiniMRYarnCluster("MiniClusterTest", 1);
				mrCluster.init(configuration);
				start();
			} catch (Exception e) {
				logger.error("getConfiguration caused exception", e);
			}
		}
		return configuration;
	}

    public void start() {
        if (mrCluster != null) {
            mrCluster.start();
        }
    }
    
    public void stop() {
        if (mrCluster != null) {
            mrCluster.stop();
            mrCluster = null;
        }
    }

}
