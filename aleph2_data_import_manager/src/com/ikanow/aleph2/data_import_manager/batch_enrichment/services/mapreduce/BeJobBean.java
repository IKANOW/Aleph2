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

import java.util.List;
import java.util.Optional;

import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.objects.data_import.EnrichmentControlMetadataBean;
import com.ikanow.aleph2.data_model.objects.shared.SharedLibraryBean;

/** This class contains data objects for one batch entrichment bucket enhancement job. 
*/

public class BeJobBean {
	private String bucketPathStr;
	private String bucketInputPath = null;
	private String bucketOutPath = null;
	private List<SharedLibraryBean> sharedLibraries = null;
	private DataBucketBean dataBucketBean = null;
	private String enrichmentControlMetadataName;
	
	public BeJobBean(){
		
	}

	public BeJobBean(DataBucketBean dataBucketBean, String enrichmentControlMetadataName, List<SharedLibraryBean> sharedLibraries, String bucketPathStr, String bucketInputPath, String bucketOutPath){
		this.dataBucketBean = dataBucketBean;
		this.enrichmentControlMetadataName = enrichmentControlMetadataName;
		this.sharedLibraries =  sharedLibraries;
		this.bucketPathStr = bucketPathStr;
		this.bucketInputPath = bucketInputPath;
		this.bucketOutPath = bucketOutPath;
	}
	
	
	public DataBucketBean getDataBucketBean() {
		return dataBucketBean;
	}
	public void setDataBucketBean(DataBucketBean dataBucketBean) {
		this.dataBucketBean = dataBucketBean;
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

	
	public String getEnrichmentControlMetadataName() {
		return enrichmentControlMetadataName;
	}


	public void setEnrichmentControlMetadataName(String enrichmentControlMetadataName) {
		this.enrichmentControlMetadataName = enrichmentControlMetadataName;
	}


	public String getBucketInputPath() {
		return bucketInputPath;
	}

	public void setBucketInputPath(String bucketInputPath) {
		this.bucketInputPath = bucketInputPath;
	}

	public String getBucketOutPath() {
		return bucketOutPath;
	}

	public void setBucketOutPath(String bucketOutPath) {
		this.bucketOutPath = bucketOutPath;
	}

	public static Optional<EnrichmentControlMetadataBean> extractEnrichmentControlMetadata(DataBucketBean dataBucketBean,String enrichmentControlMetadataName){
		Optional<EnrichmentControlMetadataBean> oecm = dataBucketBean.batch_enrichment_configs().stream().filter(ec -> ec.name().equals(enrichmentControlMetadataName)).findFirst();
		return oecm;
		
	}
	
	public static Optional<SharedLibraryBean> extractLibrary(List<SharedLibraryBean> sharedLibraries, SharedLibraryBean.LibraryType libType){
		Optional<SharedLibraryBean> olib = sharedLibraries.stream().filter(l -> l.type() == libType).findFirst();
		return olib;		
	}
}
