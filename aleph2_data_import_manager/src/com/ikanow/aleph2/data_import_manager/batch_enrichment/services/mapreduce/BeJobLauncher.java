package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce.VerySimpleLocalExample.TestInputFormat;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce.VerySimpleLocalExample.TestMapper;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;


public class BeJobLauncher implements IBeJobService{

	private static final Logger logger = LogManager.getLogger(BeJobLauncher.class);

	protected Configuration configuration;
	protected GlobalPropertiesBean _globals = null;

	protected BeJobLoader beJobLoader;

	protected String yarnConfigie = null;
	
	@Inject 
	BeJobLauncher(GlobalPropertiesBean globals, BeJobLoader beJobLoader) {
		_globals = globals;	
		this.beJobLoader = beJobLoader;
	}
	
	/** 
	 * Override this function with system specific configuration
	 * @return
	 */
	public Configuration getConf(){
		if(configuration == null){
			this.configuration = new Configuration(false);
		
			if (new File(_globals.local_yarn_config_dir()).exists()) {
				configuration.addResource(new Path(_globals.local_yarn_config_dir() +"/core-site.xml"));
				configuration.addResource(new Path(_globals.local_yarn_config_dir() +"/yarn-site.xml"));
				configuration.addResource(new Path(_globals.local_yarn_config_dir() +"/hdfs-site.xml"));
				configuration.addResource(new Path(_globals.local_yarn_config_dir() +"/hadoop-site.xml"));
				configuration.addResource(new Path(_globals.local_yarn_config_dir() +"/mapred-site.xml"));
			}
			// These are not added by Hortonworks, so add them manually
			configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");									
			configuration.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
			// Some other config defaults:
			// (not sure if these are actually applied, or derived from the defaults - for some reason they don't appear in CDH's client config)
			configuration.set("mapred.reduce.tasks.speculative.execution", "false");
		}
		return configuration;
	}



	@Override
	public boolean runEnhancementJob(String bucketFullName, String bucketPathStr, String ecMetadataBeanName){
		
		Configuration config = getConf();
		boolean success = false;
		try {
			
		BeJobBean beJob = beJobLoader.loadBeJob(bucketFullName, bucketPathStr, ecMetadataBeanName);
		

		if(beJob!=null){
			DataBucketBean bucket = beJob.getDataBucketBean(); 
			if(bucket!=null){

				String jobName = beJob.getDataBucketBean().full_name()+"_BatchEnrichment";
				Job job = Job.getInstance( config ,jobName);
			    job.setJarByClass(BatchEnrichmentJob.class);
				
	
			    job.setMapperClass(BatchEnrichmentJob.BatchErichmentMapper.class);
			    job.setNumReduceTasks(0);
			  //  job.setReducerClass(BatchEnrichmentJob.BatchEnrichmentReducer.class);
			    
			    job.setInputFormatClass(BeFileInputFormat.class);

				// Output format:
			    job.setOutputFormatClass(TextOutputFormat.class);
	

			    Path inPath = beJob.getBucketInputPath();
			    logger.debug("Bucket Input Path:"+inPath.toString());
				FileInputFormat.addInputPath(job, inPath);
				// delete output path if it exists
				Path outPath = beJob.getBucketOutPath();

				try {
					FileContext.getLocalFSFileContext().delete(outPath, true);
				}
				catch (Exception e1) {} // (just doesn't exist yet)
				FileOutputFormat.setOutputPath(job, outPath);    
			    
			    // add configuration into config
			    String bucketJson = BeanTemplateUtils.toJson(bucket).toString();
			    config.set(BatchEnrichmentJob.DATA_BUCKET_BEAN_PARAM, bucketJson);
			    
			    job.submit();
			    // TODO don't wait
/*					while (!job.isComplete()) {
						Thread.sleep(100);
					}
	*/
			}
		}
		} catch (Exception e) {
			logger.error("Caught Exception",e);
		} 
		return success;
	     		
	}

}

