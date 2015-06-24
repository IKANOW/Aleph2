package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ikanow.aleph2.data_import_manager.batch_enrichment.utils.DataBucketTest;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;

public class MiniClusterBeJobLauncher extends BeJobLauncher {
    private static final Logger logger = LogManager.getLogger(DataBucketTest.class);

    private MiniMRYarnCluster mrCluster;
    
//    private JobConf mrClusterConf;
//    private FileSystem localFileSystem;

    
	MiniClusterBeJobLauncher(GlobalPropertiesBean globals) {
		super(globals);
		// TODO Auto-generated constructor stub
	}


	@Override
	protected Configuration getConfiguration() {// TODO Auto-generated method stub
		//super.getConfiguration();
		this.configuration = new Configuration(false);
		try {			
        //localFileSystem = FileSystem.get(configuration);		
        //mrCluster = new MiniMRCluster(1, localFileSystem.getUri().toString(), 1, null, null, new JobConf(configuration));
        mrCluster = new MiniMRYarnCluster("StandaloneTest",1);
        mrCluster.init(configuration);                       
		} catch (Exception e) {
			logger.error("getConfiguration caused exception",e);
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
