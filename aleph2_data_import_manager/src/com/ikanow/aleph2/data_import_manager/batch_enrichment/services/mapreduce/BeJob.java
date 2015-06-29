package com.ikanow.aleph2.data_import_manager.batch_enrichment.services.mapreduce;

import java.util.List;

import org.apache.hadoop.fs.Path;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

/** This class contains data objects for one batrch entrichment bucket enhancem,ent job. 
*/

public class BeJob {
	private String bucketPathStr;
	private Path bucketInpuPath = null;
	private Path bucketOutPath = null;
	private EnrichmentControlMetadataBean enrichmentControlMetadataBean = null;
	private List<SharedLibraryBean> sharedLibraries = null;

	
	public BeJob(DataBucketBean dataBucketBean, EnrichmentControlMetadataBean enrichmentControlMetadataBean, List<SharedLibraryBean> sharedLibraries, String bucketPathStr){
		this.dataBucketBean = dataBucketBean;
		this.enrichmentControlMetadataBean =  enrichmentControlMetadataBean;
		this.sharedLibraries =  sharedLibraries;
		this.bucketPathStr = bucketPathStr;
		this.bucketInpuPath = new Path(bucketPathStr + "/managed_bucket/import/ready");
		this.bucketOutPath = new Path(bucketPathStr + "/managed_bucket/import/temp");
	}
	
	private DataBucketBean dataBucketBean = null;
	
	public DataBucketBean getDataBucketBean() {
		return dataBucketBean;
	}
	public void setDataBucketBean(DataBucketBean dataBucketBean) {
		this.dataBucketBean = dataBucketBean;
	}
	public EnrichmentControlMetadataBean getEnrichmentControlMetadataBean() {
		return enrichmentControlMetadataBean;
	}
	public void setEnrichmentControlMetadataBean(EnrichmentControlMetadataBean enrichmentControlMetadataBean) {
		this.enrichmentControlMetadataBean = enrichmentControlMetadataBean;
	}
	public List<SharedLibraryBean> getSharedLibraries() {
		return sharedLibraries;
	}
	public void setSharedLibraries(List<SharedLibraryBean> sharedLibraries) {
		this.sharedLibraries = sharedLibraries;
	}

	public String getBucketPathStr() {
		return bucketPathStr;
	}
	public void setBucketPathStr(String bucketPathStr) {
		this.bucketPathStr = bucketPathStr;
	}
	public Path getBucketInpuPath() {
		return bucketInpuPath;
	}
	public void setBucketInpuPath(Path bucketInpuPath) {
		this.bucketInpuPath = bucketInpuPath;
	}
	public Path getBucketOutPath() {
		return bucketOutPath;
	}
	public void setBucketOutPath(Path bucketOutPath) {
		this.bucketOutPath = bucketOutPath;
	}
	
}
