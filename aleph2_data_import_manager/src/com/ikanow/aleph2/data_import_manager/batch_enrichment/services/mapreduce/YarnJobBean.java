package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.util.HashMap;
import java.util.Map;

public class YarnJobBean {

	private String appName = "BATCH_ENHANCEMENT";
	private String appMasterJarPath;
	private String log4jPath;

	// App master priority
	private int amPriority = 0;
	// Queue for App master
	private String amQueue = "";
	// Amt. of memory resource to request for to run the App Master
	private int mxMemory = 10;
	// Amt. of virtual core resource to request for to run the App Master
	private int amVCores = 1;

	// Application master jar file
	private String appMasterJar = "";
	// Main class to invoke application master
	private String appMasterMainClass = null;;

	// Shell command to be executed
	private String shellCommand = "";
	// Location of shell script
	private String shellScriptPath = "";
	// Args to be passed to the shell command
	private String[] shellArgs = new String[] {};
	// Env variables to be setup for the shell command
	private Map<String, String> shellEnv = new HashMap<String, String>();
	// Shell Command Container priority
	private int shellCmdPriority = 0;

	// Amt of memory to request for container in which shell script will be
	// executed
	private int containerMemory = 10;
	// Amt. of virtual cores to request for container in which shell script will
	// be executed
	private int containerVirtualCores = 1;
	// No. of containers in which the shell script needs to be executed
	private int numContainers = 1;

	// log4j.properties file
	// if available, add to local resources and set into classpath
	private String log4jPropFile = "";

	// flag to indicate whether to keep containers across application attempts.
	private boolean keepContainers = false;
	  // Debug flag
	  boolean debugFlag = false;	

	public int getAmPriority() {
		return amPriority;
	}

	public void setAmPriority(int amPriority) {
		this.amPriority = amPriority;
	}

	public String getAmQueue() {
		return amQueue;
	}

	public void setAmQueue(String amQueue) {
		this.amQueue = amQueue;
	}

	public int getAmVCores() {
		return amVCores;
	}

	public void setAmVCores(int amVCores) {
		this.amVCores = amVCores;
	}

	public int getContainerMemory() {
		return containerMemory;
	}

	public void setContainerMemory(int containerMemory) {
		this.containerMemory = containerMemory;
	}

	public int getContainerVirtualCores() {
		return containerVirtualCores;
	}

	public void setContainerVirtualCores(int containerVirtualCores) {
		this.containerVirtualCores = containerVirtualCores;
	}

	public int getNumContainers() {
		return numContainers;
	}

	public void setNumContainers(int numContainers) {
		this.numContainers = numContainers;
	}

	public String getAppMasterMainClass() {
		return appMasterMainClass;
	}

	public void setAppMasterMainClass(String appMasterMainClass) {
		this.appMasterMainClass = appMasterMainClass;
	}

	public boolean isKeepContainers() {
		return keepContainers;
	}

	public void setKeepContainers(boolean keepContainers) {
		this.keepContainers = keepContainers;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getAppMasterJar() {
		return appMasterJar;
	}

	public void setAppMasterJar(String appMasterJar) {
		this.appMasterJar = appMasterJar;
	}

	public String getAppMasterJarPath() {
		return appMasterJarPath;
	}

	public void setAppMasterJarPath(String appMasterJarPath) {
		this.appMasterJarPath = appMasterJarPath;
	}

	public String getLog4jPath() {
		return log4jPath;
	}

	public void setLog4jPath(String log4jPath) {
		this.log4jPath = log4jPath;
	}

	public String getLog4jPropFile() {
		return log4jPropFile;
	}

	public void setLog4jPropFile(String log4jPropFile) {
		this.log4jPropFile = log4jPropFile;
	}

	public int getMxMemory() {
		return mxMemory;
	}

	public void setMxMemory(int mxMemory) {
		this.mxMemory = mxMemory;
	}

	public String getShellCommand() {
		return shellCommand;
	}

	public void setShellCommand(String shellCommand) {
		this.shellCommand = shellCommand;
	}

	public String getShellScriptPath() {
		return shellScriptPath;
	}

	public void setShellScriptPath(String shellScriptPath) {
		this.shellScriptPath = shellScriptPath;
	}

	public String[] getShellArgs() {
		return shellArgs;
	}

	public void setShellArgs(String[] shellArgs) {
		this.shellArgs = shellArgs;
	}

	public Map<String, String> getShellEnv() {
		return shellEnv;
	}

	public void setShellEnv(Map<String, String> shellEnv) {
		this.shellEnv = shellEnv;
	}

	public int getShellCmdPriority() {
		return shellCmdPriority;
	}

	public void setShellCmdPriority(int shellCmdPriority) {
		this.shellCmdPriority = shellCmdPriority;
	}

	public boolean isDebugFlag() {
		return debugFlag;
	}

	public void setDebugFlag(boolean debugFlag) {
		this.debugFlag = debugFlag;
	}
}
