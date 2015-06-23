package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.File;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;


public class BeJobLauncher implements IBeJobService{

	private static final Logger logger = LogManager.getLogger(BeJobLauncher.class);

	private Configuration config;
	protected GlobalPropertiesBean _globals = null;

	@Inject 
	BeJobLauncher(GlobalPropertiesBean globals) {
		_globals = globals;	
	}
	
	/** 
	 * Override this function with system specific configuration
	 * @return
	 */
	protected Configuration getConfiguration(){
		if(config == null){
			this.config = new Configuration(false);
		
			if (new File(_globals.local_yarn_config_dir()).exists()) {
				config.addResource(new Path(_globals.local_yarn_config_dir() +"/core-site.xml"));
				config.addResource(new Path(_globals.local_yarn_config_dir() +"/yarn-site.xml"));
				config.addResource(new Path(_globals.local_yarn_config_dir() +"/hdfs-site.xml"));
				config.addResource(new Path(_globals.local_yarn_config_dir() +"/hadoop-site.xml"));
				config.addResource(new Path(_globals.local_yarn_config_dir() +"/mapred-site.xml"));
			}
			// These are not added by Hortonworks, so add them manually
			config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");									
			config.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
			// Some other config defaults:
			// (not sure if these are actually applied, or derived from the defaults - for some reason they don't appear in CDH's client config)
			config.set("mapred.reduce.tasks.speculative.execution", "false");
		}
		return config;
	}


	@Override
	public boolean runEnhancementJob(DataBucketBean bucket,EnrichmentControlMetadataBean ec ,List<SharedLibraryBean> sharedLibraries,Path bucketInput,Path bucketOutput){
		
		Configuration config = getConfiguration();
		String jobName = bucket.full_name()+"_BatchEnrichment";
		boolean success = false;
		try {
			if(bucket!=null){
				Job job = Job.getInstance( config ,jobName);
			    job.setJarByClass(BatchEnrichmentJob.class);
				
			    job.setOutputKeyClass(Text.class);
			    job.setOutputValueClass(IntWritable.class);
	
			    job.setMapperClass(BatchEnrichmentJob.BatchErichmentMapper.class);
			    job.setReducerClass(BatchEnrichmentJob.BatchEnrichmentReducer.class);
	
			    job.setInputFormatClass(TextInputFormat.class);
			    job.setOutputFormatClass(TextOutputFormat.class);
	
			    job.setInputFormatClass(BeFileInputFormat.class);

			    FileInputFormat.setInputPaths(job, bucketInput);
			    FileOutputFormat.setOutputPath(job, bucketOutput);		    
			    
			    // add configuration into config
			    String bucketJson = BeanTemplateUtils.toJson(bucket).toString();
			    config.set(BatchEnrichmentJob.DATA_BUCKET_BEAN_PARAM, bucketJson);
			    
			    // TODO do we want asynchronous?
			    //job.submit();	
			     // Submit the job, then poll for progress until the job is complete
			     success =  job.waitForCompletion(true);
			}
		} catch (Exception e) {
			logger.error("Caught Exception",e);
		} 
		
		return success;
	     		
	}
	

}

