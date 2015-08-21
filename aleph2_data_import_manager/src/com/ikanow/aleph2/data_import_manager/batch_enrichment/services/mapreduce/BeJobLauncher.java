package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_import_manager.batch_enrichment.services.BatchEnrichmentContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;


public class BeJobLauncher implements IBeJobService{

	private static final Logger logger = LogManager.getLogger(BeJobLauncher.class);

	protected Configuration configuration;
	protected GlobalPropertiesBean _globals = null;

	protected BeJobLoader beJobLoader;

	protected String yarnConfigie = null;

	protected BatchEnrichmentContext batchEnrichmentContext;

	@Inject 
	BeJobLauncher(GlobalPropertiesBean globals, BeJobLoader beJobLoader, BatchEnrichmentContext batchEnrichmentContext) {
		_globals = globals;	
		this.beJobLoader = beJobLoader;
		this.batchEnrichmentContext = batchEnrichmentContext;
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
	public String runEnhancementJob(String bucketFullName, String bucketPathStr, String ecMetadataBeanName){
		
		Configuration config = getConf();
		String jobName = null;
		try {
			
		BeJobBean beJob = beJobLoader.loadBeJob(bucketFullName, bucketPathStr, ecMetadataBeanName);
		

		if(beJob!=null){
			DataBucketBean bucket = beJob.getDataBucketBean(); 
			if(bucket!=null){

				batchEnrichmentContext.setBucket(bucket);
				batchEnrichmentContext.setLibraryConfig(BeJobBean.extractLibrary(beJob.getSharedLibraries(),SharedLibraryBean.LibraryType.enrichment_module).get());

				String contextSignature = batchEnrichmentContext.getEnrichmentContextSignature(Optional.of(bucket), Optional.empty()); 
			    config.set(BatchEnrichmentJob.BE_CONTEXT_SIGNATURE, contextSignature);
				
				jobName = beJob.getDataBucketBean().full_name()+"_BatchEnrichment";
				// set metadata bean to job jik we need to have more config, bean is included in bucket data but needs to be identified
				config.set(BatchEnrichmentJob.BE_META_BEAN_PARAM, ecMetadataBeanName);

			    // do not set anything into config past this line
			    Job job = Job.getInstance( config ,jobName);
			    job.setJarByClass(BatchEnrichmentJob.class);
				
	
			    job.setMapperClass(BatchEnrichmentJob.BatchErichmentMapper.class);
			    job.setNumReduceTasks(0);
			  //  job.setReducerClass(BatchEnrichmentJob.BatchEnrichmentReducer.class);
			    
			    job.setInputFormatClass(BeFileInputFormat.class);

				// Output format:
			    job.setOutputFormatClass(BeFileOutputFormat.class);
	

			    Path inPath = new Path(beJob.getBucketInputPath());
			    logger.debug("Bucket Input Path:"+inPath.toString());
				FileInputFormat.addInputPath(job, inPath);
				// delete output path if it exists
				Path outPath = new Path(beJob.getBucketOutPath());

				try {
					FileContext.getLocalFSFileContext().delete(outPath, true);
				}
				catch (Exception e1) {} // (just doesn't exist yet)
				FileOutputFormat.setOutputPath(job, outPath);    
			    
				launch(job);
			}
		}
		} catch (Exception e) {
			logger.error("Caught Exception",e);
		} 
		return jobName;
	     		
	}
	
	// default behavior is to
	public void launch(Job job) throws ClassNotFoundException, IOException, InterruptedException{
		job.submit();
		
	}

}

