package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;


public class JobLauncher {

	private static final Logger logger = LogManager.getLogger(JobLauncher.class);

	private Configuration config;
	protected GlobalPropertiesBean _globals = null;

	@Inject 
	JobLauncher(GlobalPropertiesBean globals) {
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public String runHadoopJob(MapReduceJob job, String tempJarLocation) throws IOException
	{
	/*	StringWriter xml = new StringWriter();
		String outputCollection = job.outputCollectionTemp;// (non-append mode) 
		if ( ( null != job.appendResults ) && job.appendResults) 
			outputCollection = job.outputCollection; // (append mode, write directly in....)
		else if ( null != job.incrementalMode )
			job.incrementalMode = false; // (not allowed to be in incremental mode and not update mode)
		
		createConfigXML(xml, job.jobtitle,job.inputCollection, InfiniteHadoopUtils.getQueryOrProcessing(job.query,InfiniteHadoopUtils.QuerySpec.INPUTFIELDS), job.isCustomTable, job.getOutputDatabase(), job._id.toString(), outputCollection, job.mapper, job.reducer, job.combiner, InfiniteHadoopUtils.getQueryOrProcessing(job.query,InfiniteHadoopUtils.QuerySpec.QUERY), job.communityIds, job.getOutputKey(), job.getOutputValue(),job.arguments, job.incrementalMode, job.submitterID, job.selfMerge, job.outputCollection, job.appendResults,prop_general.getDatabaseServer());
		*/
		ClassLoader savedClassLoader = Thread.currentThread().getContextClassLoader();
				
		URLClassLoader child = new URLClassLoader (new URL[] { new File(tempJarLocation).toURI().toURL() }, savedClassLoader);			
		Thread.currentThread().setContextClassLoader(child);

		// Check version: for now, any infinit.e.data_model with an VersionTest class is acceptable
/*		boolean dataModelLoaded = true;
		try {
			URLClassLoader versionTest = new URLClassLoader (new URL[] { new File(tempJarLocation).toURI().toURL() }, null);
			try {
				Class.forName("com.ikanow.infinit.e.data_model.custom.InfiniteMongoInputFormat", true, versionTest);				
			}
			catch (ClassNotFoundException e2) {
				//(this is fine, will use the cached version)
				dataModelLoaded = false;
			}
			if (dataModelLoaded)
				Class.forName("com.ikanow.infinit.e.data_model.custom.InfiniteMongoVersionTest", true, versionTest);
		} 
		catch (ClassNotFoundException e1) {
			throw new RuntimeException("This JAR is compiled with too old a version of the data-model, please recompile with Jan 2014 (rc2) onwards");
		}		
	*/	
		// Now load the XML into a configuration object: 
		Configuration config = getConfiguration();
		// Add the client configuration overrides:
		/*		if (!bLocalMode) {
			String hadoopConfigPath = props_custom.getHadoopConfigPath() + "/hadoop/";
			config.addResource(new Path(hadoopConfigPath + "core-site.xml"));
			config.addResource(new Path(hadoopConfigPath + "mapred-site.xml"));
			config.addResource(new Path(hadoopConfigPath + "hadoop-site.xml"));
		}//TESTED
		
		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(new ByteArrayInputStream(xml.toString().getBytes()));
			NodeList nList = doc.getElementsByTagName("property");
			
			for (int temp = 0; temp < nList.getLength(); temp++) {			 
				Node nNode = nList.item(temp);
				if (nNode.getNodeType() == Node.ELEMENT_NODE) {				   
					Element eElement = (Element) nNode;	
					String name = getTagValue("name", eElement);
					String value = getTagValue("value", eElement);
					if ((null != name) && (null != value)) {
						config.set(name, value);
					}
				}
			}
		}
		catch (Exception e) {
			throw new IOException(e.getMessage());
		} */
		
		// Some other config defaults:
		// (not sure if these are actually applied, or derived from the defaults - for some reason they don't appear in CDH's client config)
		config.set("mapred.map.tasks.speculative.execution", "false");
		config.set("mapred.reduce.tasks.speculative.execution", "false");
		// (default security is ignored here, have it set via HADOOP_TASKTRACKER_CONF in cloudera)
		
		// Now run the JAR file
		try {
			/*BasicDBObject advancedConfigurationDbo = null;
			try {
				advancedConfigurationDbo = (null != job.query) ? ((BasicDBObject) com.mongodb.util.JSON.parse(job.query)) : (new BasicDBObject());
			}
			catch (Exception e) {
				advancedConfigurationDbo = new BasicDBObject();
			}
			
			boolean esMode = advancedConfigurationDbo.containsField("qt") && !job.isCustomTable;
			if (esMode && !job.inputCollection.equals("doc_metadata.metadata")) {
				throw new RuntimeException("Infinit.e Queries are only supported on doc_metadata - use MongoDB queries instead.");
			}
*/
/*			config.setBoolean("mapred.used.genericoptionsparser", true); // (just stops an annoying warning from appearing)
			if (bLocalMode) { // local job tracker and FS mode
				config.set("mapred.job.tracker", "local");
				config.set("fs.defaultFS", "local");		
			}
			else {
				if (bTestMode) { // run job tracker locally but FS mode remotely
					config.set("mapred.job.tracker", "local");					
				}
				else { // normal job tracker
					String trackerUrl = HadoopUtils.getXMLProperty(props_custom.getHadoopConfigPath() + "/hadoop/mapred-site.xml", "mapred.job.tracker");
					config.set("mapred.job.tracker", trackerUrl);
				}
				String fsUrl = HadoopUtils.getXMLProperty(props_custom.getHadoopConfigPath() + "/hadoop/core-site.xml", "fs.defaultFS");
				config.set("fs.defaultFS", fsUrl);				
			} */
/*			if (!dataModelLoaded && !(bTestMode || bLocalMode)) { // If running distributed and no data model loaded then add ourselves
				Path jarToCache = InfiniteHadoopUtils.cacheLocalFile("/opt/infinite-home/lib/", "infinit.e.data_model.jar", config);
				DistributedCache.addFileToClassPath(jarToCache, config);
				jarToCache = InfiniteHadoopUtils.cacheLocalFile("/opt/infinite-home/lib/", "infinit.e.processing.custom.library.jar", config);
				DistributedCache.addFileToClassPath(jarToCache, config);
			}//TESTED
			
			// Debug scripts (only if they exist), and only in non local/test mode
			if (!bLocalMode && !bTestMode) {
				
				try {
					Path scriptToCache = InfiniteHadoopUtils.cacheLocalFile("/opt/infinite-home/scripts/", "custom_map_error_handler.sh", config);
					config.set("mapred.map.task.debug.script", "custom_map_error_handler.sh " + job.jobtitle);
					config.set("mapreduce.map.debug.script", "custom_map_error_handler.sh " + job.jobtitle);						
					DistributedCache.createSymlink(config);
					DistributedCache.addCacheFile(scriptToCache.toUri(), config);
				}
				catch (Exception e) {} // just carry on
				
				try {
					Path scriptToCache = InfiniteHadoopUtils.cacheLocalFile("/opt/infinite-home/scripts/", "custom_reduce_error_handler.sh", config);
					config.set("mapred.reduce.task.debug.script", "custom_reduce_error_handler.sh " + job.jobtitle);
					config.set("mapreduce.reduce.debug.script", "custom_reduce_error_handler.sh " + job.jobtitle);						
					DistributedCache.createSymlink(config);
					DistributedCache.addCacheFile(scriptToCache.toUri(), config);
				}
				catch (Exception e) {} // just carry on
				
			}//TODO (???): TOTEST
*/
			// (need to do these 2 things here before the job is created, at which point the config class has been copied across)
			//1)
			Class<?> mapperClazz = Class.forName (job.getMapper(), true, child);
/*			if (ICustomInfiniteInternalEngine.class.isAssignableFrom(mapperClazz)) { // Special case: internal custom engine, so gets an additional integration hook
				ICustomInfiniteInternalEngine preActivities = (ICustomInfiniteInternalEngine) mapperClazz.newInstance();
				preActivities.preTaskActivities(job._id, job.communityIds, config, !(bTestMode || bLocalMode));
			}//TESTED
			//2)
			if (job.inputCollection.equalsIgnoreCase("file.binary_shares")) {
				// Need to download the GridFSZip file
				try {
					Path jarToCache = InfiniteHadoopUtils.cacheLocalFile("/opt/infinite-home/lib/unbundled/", "GridFSZipFile.jar", config);
					DistributedCache.addFileToClassPath(jarToCache, config);
				}
				catch (Throwable t) {} // (this is fine, will already be on the classpath .. otherwise lots of other stuff will be failing all over the place!)				
			}

			if (job.inputCollection.equals("records")) {
				
				InfiniteElasticsearchHadoopUtils.handleElasticsearchInput(job, config, advancedConfigurationDbo);
				
				//(won't run under 0.19 so running with "records" should cause all sorts of exceptions)
				
			}//TESTED (by hand)			

			if (bTestMode || bLocalMode) { // If running locally, turn "snappy" off - tomcat isn't pointing its native library path in the right place
				config.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.DefaultCodec");
			}	
			
			
			// Manually specified caches
			List<URL> localJarCaches = 
					InfiniteHadoopUtils.handleCacheList(advancedConfigurationDbo.get("$caches"), job, config, props_custom);			
*/
			Job hj = Job.getInstance( config ); // (NOTE: from here, changes to config are ignored)
/*			try {
				
				if (null != localJarCaches) {
					if (bLocalMode || bTestMode) {
						Method method = URLClassLoader.class.getDeclaredMethod("addURL", new Class[]{URL.class});
						method.setAccessible(true);
						method.invoke(child, localJarCaches.toArray());
						
					}//TOTEST (tested logically)
					
				}	*/			
				Class<?> classToLoad = Class.forName (job.getMapper(), true, child);			
				hj.setJarByClass(classToLoad);
				
				if ("filesystem".equalsIgnoreCase(job.getInputCollection())) {
					String inputPath = job.getFileUrl();
					//String inputPath = MongoDbUtil.getProperty(advancedConfigurationDbo, "file.url");
						if (null == inputPath) {
							throw new RuntimeException("Must specify 'file.url' if reading from filesystem.");
						}
						if (!inputPath.endsWith("/")) {
							inputPath = inputPath + "/";
						}
					//inputPath = InfiniteHadoopUtils.authenticateInputDirectory(job, inputPath);
					
//					InfiniteFileInputFormat.addInputPath(hj, new Path(inputPath + "*/*")); // (that extra bit makes it recursive)
					/*InfiniteFileInputFormat.setMaxInputSplitSize(hj, 33554432); // (32MB)
					InfiniteFileInputFormat.setInfiniteInputPathFilter(hj, config);
					hj.setInputFormatClass((Class<? extends InputFormat>) Class.forName ("com.ikanow.infinit.e.data_model.custom.InfiniteFileInputFormat", true, child));
					*/
				}
				else if ("file.binary_shares".equalsIgnoreCase(job.getInputCollection())) {
					
					String[] oidStrs = null;
						String inputPath = job.getFileUrl();
						//String inputPath = MongoDbUtil.getProperty(advancedConfigurationDbo, "file.url");
						Pattern oidExtractor = Pattern.compile("inf://share/([^/]+)");
						Matcher m = oidExtractor.matcher(inputPath);
						if (m.find()) {
							oidStrs = m.group(1).split("\\s*,\\s*");
							
						}
						else {
							throw new RuntimeException("file.url must be in format inf://share/<oid-list>/<string>: " + inputPath);
						}
						//InfiniteHadoopUtils.authenticateShareList(job, oidStrs);
					
					hj.getConfiguration().setStrings("mapred.input.dir", oidStrs);
					hj.setInputFormatClass( (Class<? extends InputFormat>) Class.forName ("com.ikanow.infinit.e.data_model.custom.InfiniteShareInputFormat", true, child));
					
				}
				else if ("records".equalsIgnoreCase(job.getInputCollection())) {
					hj.setInputFormatClass((Class<? extends InputFormat>) Class.forName ("com.ikanow.infinit.e.data_model.custom.InfiniteEsInputFormat", true, child));
				}
				else {
/*					if (esMode) {
						hj.setInputFormatClass((Class<? extends InputFormat>) Class.forName ("com.ikanow.infinit.e.processing.custom.utils.InfiniteElasticsearchMongoInputFormat", true, child));						
					}
					else {
						hj.setInputFormatClass((Class<? extends InputFormat>) Class.forName ("com.ikanow.infinit.e.data_model.custom.InfiniteMongoInputFormat", true, child));
					} */
				}
				if (job.isExportToHdfs()) {
					
					//TODO (INF-2469): Also, if the output key is BSON then also run as text (but output as JSON?)
					
					Path outPath = ensureOutputDirectory(job);
					
					if ("org.apache.hadoop.io.text".equalsIgnoreCase(job.getOutputKey()) && "org.apache.hadoop.io.text".equalsIgnoreCase(job.getOutputValue()))
					{
						// (slight hack before I sort out the horrendous job class - if key/val both text and exporting to HDFS then output as Text)
						hj.setOutputFormatClass((Class<? extends OutputFormat>) Class.forName ("org.apache.hadoop.mapreduce.lib.output.TextOutputFormat", true, child));
						TextOutputFormat.setOutputPath(hj, outPath);
					}//TESTED
					else {
						hj.setOutputFormatClass((Class<? extends OutputFormat>) Class.forName ("org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat", true, child));					
						SequenceFileOutputFormat.setOutputPath(hj, outPath);
					}//TESTED
				}
				else { // normal case, stays in MongoDB
					hj.setOutputFormatClass((Class<? extends OutputFormat>) Class.forName ("com.ikanow.infinit.e.data_model.custom.InfiniteMongoOutputFormat", true, child));
				}
				hj.setMapperClass((Class<? extends Mapper>) mapperClazz);
/*				String mapperOutputKey = advancedConfigurationDbo.getString("$mapper_key_class", null);
				if (null != mapperOutputKeyOverride) {
					hj.setMapOutputKeyClass(Class.forName(mapperOutputKeyOverride));
				}//TESTED 
				
				String mapperOutputValueOverride = advancedConfigurationDbo.getString("$mapper_value_class", null);
				if (null != mapperOutputValueOverride) {
					hj.setMapOutputValueClass(Class.forName(mapperOutputValueOverride));
				}//TESTED 
	*/			
				// TODO why startWith # ?
				if ((null != job.getReducer()) && !job.getReducer().startsWith("#") && !"null".equalsIgnoreCase(job.getReducer()) && !"none".equalsIgnoreCase(job.getReducer())) {
					hj.setReducerClass((Class<? extends Reducer>) Class.forName (job.getReducer(), true, child));
					// Variable reducers:
/*					if (null != job.query) {
						try { 
							hj.setNumReduceTasks(advancedConfigurationDbo.getInt("$reducers", 1));
						}catch (Exception e) {
							try {
								// (just check it's not a string that is a valid int)
								hj.setNumReduceTasks(Integer.parseInt(advancedConfigurationDbo.getString("$reducers", "1")));
							}
							catch (Exception e2) {}
							
						}
					}//TESTED
					*/
				}
				else {
					hj.setNumReduceTasks(0);
				}
				if ((null != job.getCombiner()) && !job.getCombiner().startsWith("#") && !job.getCombiner().equalsIgnoreCase("null") && !job.getCombiner().equalsIgnoreCase("none")) {
					hj.setCombinerClass((Class<? extends Reducer>) Class.forName (job.getCombiner(), true, child));
				}
				hj.setOutputKeyClass(Class.forName (job.getOutputKey(), true, child));
				hj.setOutputValueClass(Class.forName (job.getOutputValue(), true, child));
				
				hj.setJobName(job.getJobtitle());
				//currJobName = job.jobtitle;

		// TODO before submit
			//FileInputFormat.setInputPath(job, inDir);
			 //FileOutputFormat.setOutputPath(job, outDir);
		
/*			if (bTestMode || bLocalMode) {
				
				hj.submit();
				//currThreadId = null;
				//Logger.getRootLogger().addAppender(this);
				//currLocalJobId = hj.getJobID().toString();
				currLocalJobErrs.setLength(0);
				while (!hj.isComplete()) {
					Thread.sleep(1000);
				}
				//Logger.getRootLogger().removeAppender(this);
				if (hj.isSuccessful()) {
					if (this.currLocalJobErrs.length() > 0) {
						return "local_done: " + this.currLocalJobErrs.toString();						
					}
					else {
						return "local_done";
					}
				}
				else {
					return "Error: " + this.currLocalJobErrs.toString();
				}
			}
			else { 
			*/
				hj.submit();
				String jobId = hj.getJobID().toString();
				return jobId;
//			} 			
		}
		catch (Exception e) { 
			logger.error("Caught exception",e);
		}
		return null;
	}

	public boolean runEnhancementJob(DataBucketBean bucket,Path bucketInput,Path bucketOutput){
		
		Configuration config = getConfiguration();
		String jobName = bucket.full_name()+"_BatchEnrichment";
		boolean success = false;
		try {
			Job job = Job.getInstance( config ,jobName);
		    job.setJarByClass(BatchEnrichmentJob.class);
			
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);

		    job.setMapperClass(BatchEnrichmentJob.BatchErichmentMapper.class);
		    job.setReducerClass(BatchEnrichmentJob.BatchEnrichmentReducer.class);

		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);

		    FileInputFormat.setInputPaths(job, bucketInput);
		    FileOutputFormat.setOutputPath(job, bucketOutput);		    
		    
		    
		    // TODO do we want asynchronous?
		    //job.submit();	
		     // Submit the job, then poll for progress until the job is complete
		     success =  job.waitForCompletion(true);
		} catch (Exception e) {
			logger.error("Caught Exception",e);
		} 
		return success;
	     		
	}
	
	private Path ensureOutputDirectory(MapReduceJob job) {
	// TODO Auto-generated method stub
	return null;
	}

}

