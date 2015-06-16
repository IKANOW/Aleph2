package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

public class MapReduceJob {


	private String mapper;
	public String getMapper() {
		return mapper;
	}
	public void setMapper(String mapper) {
		this.mapper = mapper;
	}
	public String getReducer() {
		return reducer;
	}
	public void setReducer(String reducer) {
		this.reducer = reducer;
	}
	private String reducer;
	
	private String inputCollection;
	public String getInputCollection() {
		return inputCollection;
	}
	public void setInputCollection(String inputCollection) {
		this.inputCollection = inputCollection;
	}
	
	private String fileUrl;
	
	private boolean exportToHdfs = false;
	
	public boolean isExportToHdfs() {
		return exportToHdfs;
	}
	public void setExportToHdfs(boolean exportToHdfs) {
		this.exportToHdfs = exportToHdfs;
	}
	public String getFileUrl() {
		return fileUrl;
	}
	public void setFileUrl(String fileUrl) {
		this.fileUrl = fileUrl;
	}
	
	private String bucketName;
	public String getBucketName() {
		return bucketName;
	}
	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}
	private String bucketPath;
	public String getBucketPath() {
		return bucketPath;
	}
	public void setBucketPath(String bucketPath) {
		this.bucketPath = bucketPath;
	}
	
	private String outputKey;
	public String getOutputKey() {
		return outputKey;
	}
	public void setOutputKey(String outputKey) {
		this.outputKey = outputKey;
	}
	public String getOutputValue() {
		return outputValue;
	}
	public void setOutputValue(String outputValue) {
		this.outputValue = outputValue;
	}
	private String outputValue;
	private String combiner;
	public String getCombiner() {
		return combiner;
	}
	public void setCombiner(String combiner) {
		this.combiner = combiner;		
	}
	
	private String jobtitle;
	public String getJobtitle() {
		return jobtitle;
	}
	public void setJobtitle(String jobtitle) {
		this.jobtitle = jobtitle;
	}
	
}
