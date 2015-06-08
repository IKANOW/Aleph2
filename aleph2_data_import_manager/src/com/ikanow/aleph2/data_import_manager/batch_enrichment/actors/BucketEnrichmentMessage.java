package com.ikanow.aleph2.data_import_manager.batch_enrichment.actors;

import java.io.Serializable;

public class BucketEnrichmentMessage implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String bucketPathStr;
	public String getBucketPathStr() {
		return bucketPathStr;
	}

	public String getBucketZkPath() {
		return bucketZkPath;
	}


	private String bucketZkPath;
	private String buckeFullName;

	public String getBuckeFullName() {
		return buckeFullName;
	}

	public BucketEnrichmentMessage(String bucketPathStr, String bucketFullName, String bucketZkPath) {
		this.bucketPathStr = bucketPathStr;
		this.buckeFullName = bucketFullName;
		this.bucketZkPath = bucketZkPath;
	}

	@Override
	public String toString() {
		return "bucketPathStr:"+bucketPathStr+"bucketFullName:"+buckeFullName+",bucketZkPath:"+bucketZkPath;
	}
}
