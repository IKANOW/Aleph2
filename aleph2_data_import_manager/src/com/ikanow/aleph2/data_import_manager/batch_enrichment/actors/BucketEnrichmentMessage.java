package com.ikanow.aleph2.data_import_manager.batch_enrichment.actors;

public class BucketEnrichmentMessage {
	private String bucketPathStr;
	public String getBucketPathStr() {
		return bucketPathStr;
	}

	public String getBucketZkPath() {
		return bucketZkPath;
	}

	public String getBucketId() {
		return bucketId;
	}

	private String bucketZkPath;
	private String bucketId;

	public BucketEnrichmentMessage(String bucketPathStr, String bucketId, String bucketZkPath) {
		this.bucketPathStr = bucketPathStr;
		this.bucketId = bucketId;
		this.bucketZkPath = bucketZkPath;
	}

	@Override
	public String toString() {
		return "bucketPathStr:"+bucketPathStr+"bucketId:"+bucketId+",bucketZkPath:"+bucketZkPath;
	}
}
