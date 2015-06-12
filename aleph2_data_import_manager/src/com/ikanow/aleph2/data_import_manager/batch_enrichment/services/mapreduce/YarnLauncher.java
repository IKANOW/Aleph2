/*******************************************************************************
 * Copyright 2015, The IKANOW Open Source Project.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.objects.shared.GlobalPropertiesBean;

public class YarnLauncher {

	private static final Logger logger = LogManager.getLogger();

	final protected GlobalPropertiesBean _globals;

	@Inject
	YarnLauncher(GlobalPropertiesBean globals) {
		_globals = globals;
	}

	public void launchYarnJob(YarnJobBean jobBean) {
		try {
			// initialize and start a YarnClient.
			YarnClient yarnClient = YarnClient.createYarnClient();
			Configuration conf = createConfiguration();
			yarnClient.init(conf);
			yarnClient.start();

			// the client needs to create an application, and get its
			// application id.
			YarnClientApplication app = yarnClient.createApplication();
			GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
			logger.info("Yarn appResponse:"+appResponse.toString());
			// set the application submission context
			ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
			ApplicationId appId = appContext.getApplicationId();

			appContext.setKeepContainersAcrossApplicationAttempts(jobBean.isKeepContainers());
			appContext.setApplicationName(jobBean.getAppName());

			// set local resources for the application master
			// local files or archives as needed
			// In this scenario, the jar file for the application master is part
			// of the local resources
			Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

			logger.info("Copy App Master jar from local filesystem and add to local environment");
			// Copy the application master jar to the filesystem
			// Create a local resource to point to the destination jar path
			FileSystem fs = FileSystem.get(conf);
			addToLocalResources(jobBean.getAppName(), fs, jobBean.getAppMasterJar(), jobBean.getAppMasterJarPath(), appId.toString(),
					localResources, null);

			// Set the log4j properties if needed
			if (jobBean.getLog4jPropFile() != null && !jobBean.getLog4jPropFile().isEmpty()) {
				addToLocalResources(jobBean.getAppName(), fs, jobBean.getLog4jPropFile(), jobBean.getLog4jPath(), appId.toString(),
						localResources, null);
			}

			// Set the env variables to be setup in the env where the
			// application master will be run
			logger.info("Set the environment for the application master");
			Map<String, String> env = new HashMap<String, String>();

			// Add AppMaster.jar location to classpath
			// At some point we should not be required to add
			// the hadoop specific classpaths to the env.
			// It should be provided out of the box.
			// For now setting all required classpaths including
			// the classpath to "." for the application jar
			StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$()).append(ApplicationConstants.CLASS_PATH_SEPARATOR)
					.append("./*");
			for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
					YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
				classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
				classPathEnv.append(c.trim());
			}
			classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");

			// Set the necessary command to execute the application master
			Vector<CharSequence> vargs = new Vector<CharSequence>(30);

			// Set java executable command
			logger.info("Setting up app master command");
			vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
			// Set Xmx based on am memory size
			vargs.add("-Xmx" + jobBean.getMxMemory() + "m");
			// Set class name
			vargs.add(jobBean.getAppMasterMainClass());
			// Set params for Application Master
			vargs.add("--container_memory " + String.valueOf(jobBean.getContainerMemory()));
			vargs.add("--container_vcores " + String.valueOf(jobBean.getContainerVirtualCores()));
			vargs.add("--num_containers " + String.valueOf(jobBean.getNumContainers()));
			vargs.add("--priority " + String.valueOf(jobBean.getShellCmdPriority()));

			for (Map.Entry<String, String> entry : jobBean.getShellEnv().entrySet()) {
				vargs.add("--shell_env " + entry.getKey() + "=" + entry.getValue());
			}
			if (jobBean.isDebugFlag()) {
				vargs.add("--debug");
			}

			vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
			vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

			// Get final commmand
			StringBuilder command = new StringBuilder();
			for (CharSequence str : vargs) {
				command.append(str).append(" ");
			}

			logger.info("Completed setting up app master command " + command.toString());
			List<String> commands = new ArrayList<String>();
			commands.add(command.toString());

			// Set up the container launch context for the application master
			ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null);

			// Set up resource type requirements
			// For now, both memory and vcores are supported, so we set memory
			// and
			// vcores requirements
			Resource capability = Resource.newInstance(jobBean.getMxMemory(), jobBean.getAmVCores());
			appContext.setResource(capability);

			// Service data is a binary blob that can be passed to the
			// application
			// Not needed in this scenario
			// amContainer.setServiceData(serviceData);

			setupSecurity(conf,fs,amContainer);
			appContext.setAMContainerSpec(amContainer);
			
			// Set the priority for the application master
			Priority pri = Priority.newInstance(jobBean.getAmPriority());
			appContext.setPriority(pri);

			// Set the queue to which this application is to be submitted in the RM
			appContext.setQueue(jobBean.getAmQueue());

			// Submit the application to the applications manager
			// SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);

			yarnClient.submitApplication(appContext);
		} catch (Exception e) {
			logger.error("Caught Exception", e);
		}

	} // launchYarnJob


	protected  void setupSecurity(Configuration conf, FileSystem fs, ContainerLaunchContext amContainer) throws IOException {
		// Setup security tokens
		
	    // Setup security tokens
	    if (UserGroupInformation.isSecurityEnabled()) {
	      // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
	      Credentials credentials = new Credentials();
	      String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
	      if (tokenRenewer == null || tokenRenewer.length() == 0) {
	        throw new IOException(
	          "Can't get Master Kerberos principal for the RM to use as renewer");
	      }

	      // For now, only getting tokens for the default file-system.
	      final Token<?> tokens[] =
	          fs.addDelegationTokens(tokenRenewer, credentials);
	      if (tokens != null) {
	        for (Token<?> token : tokens) {
	          logger.info("Got dt for " + fs.getUri() + "; " + token);
	        }
	      }
	      DataOutputBuffer dob = new DataOutputBuffer();
	      credentials.writeTokenStorageToStream(dob);
	      ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
	      amContainer.setTokens(fsTokens);
	    }
		 
		
	}

	public Configuration createConfiguration() {
		// Now load the XML into a configuration object:
		Configuration config = new Configuration();

		if (new File(_globals.local_yarn_config_dir()).exists()) {
			config.addResource(new Path(_globals.local_yarn_config_dir() + "/yarn-site.xml"));
			config.addResource(new Path(_globals.local_yarn_config_dir() + "/core-site.xml"));
			config.addResource(new Path(_globals.local_yarn_config_dir() + "/hdfs-site.xml"));
			config.addResource(new Path(_globals.local_yarn_config_dir() + "/mapred-site.xml"));
		} else {
			config.addResource("default_fs.xml");
		}
		// These are not added by Hortonworks, so add them manually
		config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		config.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");

		return config;
	} // create Configuration

	private void addToLocalResources(String appName, FileSystem fs, String fileSrcPath, String fileDstPath, String appId,
			Map<String, LocalResource> localResources, String resources) throws IOException {
		String suffix = appName + "/" + appId + "/" + fileDstPath;
		Path dst = new Path(fs.getHomeDirectory(), suffix);
		if (fileSrcPath == null) {
			FSDataOutputStream ostream = null;
			try {
				ostream = FileSystem.create(fs, dst, new FsPermission((short) 0710));
				ostream.writeUTF(resources);
			} finally {
				IOUtils.closeQuietly(ostream);
			}
		} else {
			fs.copyFromLocalFile(new Path(fileSrcPath), dst);
		}
		FileStatus scFileStatus = fs.getFileStatus(dst);
		LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()), LocalResourceType.FILE,
				LocalResourceVisibility.APPLICATION, scFileStatus.getLen(), scFileStatus.getModificationTime());
		localResources.put(fileDstPath, scRsrc);
	}

}
